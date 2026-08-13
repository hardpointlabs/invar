//! Redis sorted-set commands: `ZADD`, `ZREM`, `ZSCORE`, all `ZRANGE`/
//! `ZRANK`/`ZCOUNT`/`ZPOP`/`Z*STORE`/`ZINTER`/`ZUNION`/`ZDIFF` variants, and
//! the blocking `BZPOPMIN`/`BZPOPMAX`.
//!
//! Port of the Go `redis/zset` package. A sorted set is an ordered family of
//! index entries under private keys, fronted by a small sentinel entry under
//! the public key, matching Go's on-disk layout exactly (see
//! `redis/zset/zset.go` for the canonical description):
//!
//! * The **sentinel** (public key, metadata `ValueType::SortedSet`) holds the
//!   set cardinality as a 4-byte big-endian uint32. See
//!   [`read_sentinel`]/[`write_sentinel`].
//! * The **score index** lives under the private keys
//!   `-<db>:<key>:score:<8B encScore>:<member>` with an empty value, where
//!   `encScore` is the 8-byte order-preserving encoding of [`encode_score`],
//!   so an ascending key scan visits members in ascending (score, member)
//!   order — see [`NodeRef::score_key`] and [`NodeRef::score_prefix`].
//! * The **member index** lives under `-<db>:<key>:member:<member>` with an
//!   8-byte big-endian float bits value, used for O(1) score lookups by
//!   member — see [`NodeRef::member_key`].
//!
//! As with the Go list/set/strings ports, the sentinel is verified to carry
//! `ValueType::SortedSet` metadata before any member is touched, so a command
//! aimed at a key holding another value type replies `WRONGTYPE`.
//!
//! Blocking pops (`BZPOPMIN`/`BZPOPMAX`) are special: they are not
//! [`QueuedOp`]s. They write their reply directly (mirroring the Go listener,
//! which returns without dispatching pending ops), so they live as the async
//! [`bzpop_reply`] function that the dispatcher calls directly. Writers that
//! add to a sorted set (`ZADD`) claim and serve the longest-waiting blocked
//! client inside their DbOp, exactly like Go, via the [`WatchRegistry`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::{Claim, PopResult, ValueType, WatchRegistry};
use crate::resp::RespValue;

/// Metadata type byte stamped on every zset sentinel, matching
/// `ValueType::SortedSet`.
const TYPE_SORTED_SET: u8 = ValueType::SortedSet as u8;

/// Anything the wire side of a crashed `DbOp` is allowed to claim if the
/// result shape is unexpected (a "can't happen" guard).
fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

// --- Float formatting ---

/// Formats a float the way Go's `redis/common.FormatFloat` does (which the
/// zset wire replies use): shortest round-trip decimal digits, no exponent,
/// and `inf`/`-inf` (not `+Inf`/`-Inf`, matching Redis's literal scores) for
/// the infinities. NaN is kept as `NaN`.
fn format_f64(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v > 0.0 && v.is_infinite() {
        return "inf".to_string();
    }
    if v < 0.0 && v.is_infinite() {
        return "-inf".to_string();
    }
    format!("{v}")
}

// --- Score encoding ---

/// `encodeScore`: the 8-byte big-endian encoding that preserves float order
/// under a bytewise ascending scan. Negative floats (sign bit set) are fully
/// inverted; positive floats only have their sign bit flipped.
fn encode_score(score: f64) -> Vec<u8> {
    let mut bits = score.to_bits();
    if bits >> 63 == 1 {
        bits ^= u64::MAX;
    } else {
        bits ^= 0x8000_0000_0000_0000;
    }
    bits.to_be_bytes().to_vec()
}

/// The inverse of [`encode_score`].
fn decode_score(bytes: &[u8]) -> f64 {
    let mut bits = u64::from_be_bytes(bytes.try_into().expect("8-byte encoded score"));
    if bits >> 63 == 1 {
        bits ^= 0x8000_0000_0000_0000;
    } else {
        bits ^= u64::MAX;
    }
    f64::from_bits(bits)
}

/// `scoreBytes`: the raw 8-byte big-endian float bits, stored as the value of
/// the member index entries.
fn score_bytes(score: f64) -> Vec<u8> {
    score.to_bits().to_be_bytes().to_vec()
}

// --- Storage layout ---

/// Identifies a sorted set on disk: the public sentinel key plus the private
/// prefix under which the score/member index entries live.
#[derive(Clone)]
struct NodeRef {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl NodeRef {
    fn new(session: &Session, key: &[u8]) -> Self {
        Self {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }
    }

    /// `-<db>:<key>:score:<8B encScore>:<member>` (mirroring Go's
    /// `scoreCompound` private-keyed).
    fn score_key(&self, score: f64, member: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.node_prefix.len() + 8 + 1 + 8 + member.len());
        key.extend_from_slice(&self.node_prefix);
        key.extend_from_slice(b":score:");
        key.extend_from_slice(&encode_score(score));
        key.push(b':');
        key.extend_from_slice(member);
        key
    }

    /// `-<db>:<key>:member:<member>` (mirroring Go's `memberCompound`
    /// private-keyed).
    fn member_key(&self, member: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.node_prefix.len() + 9 + member.len());
        key.extend_from_slice(&self.node_prefix);
        key.extend_from_slice(b":member:");
        key.extend_from_slice(member);
        key
    }

    /// Prefix enumerating the score index in ascending score order.
    fn score_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(self.node_prefix.len() + 7);
        prefix.extend_from_slice(&self.node_prefix);
        prefix.extend_from_slice(b":score:");
        prefix
    }

    /// Prefix enumerating the member index.
    fn member_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(self.node_prefix.len() + 9);
        prefix.extend_from_slice(&self.node_prefix);
        prefix.extend_from_slice(b":member:");
        prefix
    }
}

/// A member and its score.
struct MemberScore {
    member: Vec<u8>,
    score: f64,
}

/// Reads the sentinel, verifying the entry is a sorted set. A missing key
/// maps to [`KvError::KeyNotFound`]; a key holding any other type is
/// [`DbError::WrongType`]; a value too short to hold a count is treated as a
/// missing key.
async fn read_sentinel(tx: &dyn Tx, public_key: &[u8]) -> Result<u32, DbError> {
    let item = tx.get(public_key).await?;
    if item.metadata() != TYPE_SORTED_SET {
        return Err(DbError::WrongType);
    }
    let val = item.value();
    if val.len() < 4 {
        return Err(DbError::Kv(KvError::KeyNotFound));
    }
    Ok(u32::from_be_bytes(
        val[0..4].try_into().expect("slice in range"),
    ))
}

/// Like [`read_sentinel`], but a missing key yields `None` instead of an
/// error.
async fn zset_count(tx: &dyn Tx, public_key: &[u8]) -> Result<Option<u32>, DbError> {
    match read_sentinel(tx, public_key).await {
        Ok(count) => Ok(Some(count)),
        Err(DbError::Kv(KvError::KeyNotFound)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Writes the zset sentinel entry with sorted-set metadata.
fn write_sentinel(tx: &dyn Tx, public_key: &[u8], count: u32) -> Result<(), DbError> {
    tx.set(
        Entry::new(public_key.to_vec(), count.to_be_bytes().to_vec()).metadata(TYPE_SORTED_SET),
    )?;
    Ok(())
}

/// Loads every member in score order (ascending, with member bytes as the
/// tie-break). An absent key yields an empty list; a key holding another
/// value type is an error.
async fn load_all_members(tx: &dyn Tx, node: &NodeRef) -> Result<Vec<MemberScore>, DbError> {
    if zset_count(tx, &node.public_key).await?.is_none() {
        return Ok(Vec::new());
    }
    let prefix = node.score_prefix();
    let mut it = tx.new_iterator(&prefix).await?;
    let mut result = Vec::new();
    while it.next().await {
        if let Some(item) = it.item() {
            let key = item.key();
            let enc = &key[prefix.len()..prefix.len() + 8];
            let member = key[prefix.len() + 8 + 1..].to_vec();
            result.push(MemberScore {
                member,
                score: decode_score(enc),
            });
        }
    }
    let err = it.err().cloned();
    if let Some(e) = err {
        it.close().await?;
        return Err(DbError::Kv(e));
    }
    it.close().await?;
    Ok(result)
}

/// Deletes every score and member index key plus the sentinel (mirroring Go's
/// `clearInternalKeys`).
async fn clear_internal_keys(tx: &dyn Tx, node: &NodeRef) -> Result<(), DbError> {
    let mut score_keys = Vec::new();
    {
        let mut it = tx.new_iterator(&node.score_prefix()).await?;
        while it.next().await {
            if let Some(item) = it.item() {
                score_keys.push(item.key().to_vec());
            }
        }
        let err = it.err().cloned();
        if let Some(e) = err {
            it.close().await?;
            return Err(DbError::Kv(e));
        }
        it.close().await?;
    }
    let mut member_keys = Vec::new();
    {
        let mut it = tx.new_iterator(&node.member_prefix()).await?;
        while it.next().await {
            if let Some(item) = it.item() {
                member_keys.push(item.key().to_vec());
            }
        }
        let err = it.err().cloned();
        if let Some(e) = err {
            it.close().await?;
            return Err(DbError::Kv(e));
        }
        it.close().await?;
    }
    for key in score_keys {
        tx.delete(&key)?;
    }
    for key in member_keys {
        tx.delete(&key)?;
    }
    tx.delete(&node.public_key)?;
    Ok(())
}

/// Removes and returns the single element with the lowest score, or `None` if
/// empty. The shared primitive used by `ZPOPMIN`, the `BZPOPMIN` claim side
/// and the `ZADD` wake-up path. Mirrors Go's `popOneMin`.
async fn pop_one_min(tx: &dyn Tx, node: &NodeRef) -> Result<Option<MemberScore>, DbError> {
    let entries = load_all_members(tx, node).await?;
    if entries.is_empty() {
        return Ok(None);
    }
    let e = &entries[0];
    tx.delete(&node.member_key(&e.member))?;
    tx.delete(&node.score_key(e.score, &e.member))?;
    let new_count = (entries.len() as u32).saturating_sub(1);
    if new_count == 0 {
        tx.delete(&node.public_key)?;
    } else {
        write_sentinel(tx, &node.public_key, new_count)?;
    }
    Ok(Some(MemberScore {
        member: e.member.clone(),
        score: e.score,
    }))
}

/// Removes and returns the single element with the highest score, or `None`
/// if empty. Mirrors Go's `popOneMax`.
async fn pop_one_max(tx: &dyn Tx, node: &NodeRef) -> Result<Option<MemberScore>, DbError> {
    let entries = load_all_members(tx, node).await?;
    if entries.is_empty() {
        return Ok(None);
    }
    let e = &entries[entries.len() - 1];
    tx.delete(&node.member_key(&e.member))?;
    tx.delete(&node.score_key(e.score, &e.member))?;
    let new_count = (entries.len() as u32).saturating_sub(1);
    if new_count == 0 {
        tx.delete(&node.public_key)?;
    } else {
        write_sentinel(tx, &node.public_key, new_count)?;
    }
    Ok(Some(MemberScore {
        member: e.member.clone(),
        score: e.score,
    }))
}

// --- Parsing helpers ---

/// Normalizes start/stop (supporting negative indices) to `[lo, hi]`. Returns
/// `None` when the range is empty. Mirrors Go's `rangeIndexes`.
fn range_indexes(n: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let mut start = start;
    let mut stop = stop;
    if start < 0 {
        start += n as i64;
    }
    if stop < 0 {
        stop += n as i64;
    }
    if start < 0 {
        start = 0;
    }
    if stop >= n as i64 {
        stop = n as i64 - 1;
    }
    if start > stop || start >= n as i64 {
        return None;
    }
    Some((start as usize, stop as usize))
}

/// Parses a score bound: `+inf`/`-inf` or `(val` for exclusive. Mirrors Go's
/// `parseFloatBound`.
fn parse_float_bound(s: &str) -> Result<(f64, bool), DbError> {
    if s == "+inf" || s == "inf" {
        return Ok((f64::INFINITY, false));
    }
    if s == "-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }
    if let Some(rest) = s.strip_prefix('(') {
        return match rest.parse::<f64>() {
            Ok(val) => Ok((val, true)),
            Err(_) => Err(DbError::Redis("min or max value is not a float".into())),
        };
    }
    match s.parse::<f64>() {
        Ok(val) => Ok((val, false)),
        Err(_) => Err(DbError::Redis("min or max value is not a float".into())),
    }
}

/// Parses a lexicographic bound: `+`/`-` for unbounded, `(val` for exclusive,
/// `[val` for inclusive. Mirrors Go's `parseLexBound`.
fn parse_lex_bound(s: &str) -> Result<(Option<Vec<u8>>, bool), DbError> {
    if s == "+" || s == "-" {
        return Ok((None, false));
    }
    if let Some(rest) = s.strip_prefix('(') {
        return Ok((Some(rest.as_bytes().to_vec()), true));
    }
    if let Some(rest) = s.strip_prefix('[') {
        return Ok((Some(rest.as_bytes().to_vec()), false));
    }
    Err(DbError::Redis("min or max value is not a string".into()))
}

/// Filters members by the lexicographic range `[min, max]`/`(min, max)`,
/// using Redis lex notation. Mirrors Go's `filterLexRange`.
fn filter_lex_range(
    members: &[Vec<u8>],
    min_str: &str,
    max_str: &str,
) -> Result<Vec<Vec<u8>>, DbError> {
    let (min_val, min_excl) = parse_lex_bound(min_str)?;
    let (max_val, max_excl) = parse_lex_bound(max_str)?;

    let mut result = Vec::new();
    for m in members {
        if let Some(min_val) = &min_val {
            let cmp = m.as_slice().cmp(min_val.as_slice());
            if min_excl && cmp != std::cmp::Ordering::Greater {
                continue;
            }
            if !min_excl && cmp == std::cmp::Ordering::Less {
                continue;
            }
        }
        if let Some(max_val) = &max_val {
            let cmp = m.as_slice().cmp(max_val.as_slice());
            if max_excl && cmp != std::cmp::Ordering::Less {
                continue;
            }
            if !max_excl && cmp == std::cmp::Ordering::Greater {
                continue;
            }
        }
        result.push(m.clone());
    }
    Ok(result)
}

// --- Set algebra (multi-key commands) ---

/// Loads a zset as a member→score map (mirroring Go's `loadZSetMap`).
async fn load_zset_map(tx: &dyn Tx, node: &NodeRef) -> Result<HashMap<Vec<u8>, f64>, DbError> {
    let mut map = HashMap::new();
    for e in load_all_members(tx, node).await? {
        map.insert(e.member, e.score);
    }
    Ok(map)
}

/// Sorts a member→score map into ascending (score, member) order, mirroring
/// Go's `zsetToSlice`.
fn zset_to_slice(map: &HashMap<Vec<u8>, f64>) -> Vec<MemberScore> {
    let mut result: Vec<MemberScore> = map
        .iter()
        .map(|(member, score)| MemberScore {
            member: member.clone(),
            score: *score,
        })
        .collect();
    result.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.member.cmp(&b.member))
    });
    result
}

/// Aggregates a member's scores across sources by the aggregate mode
/// (`SUM`/`MIN`/`MAX`, case-insensitive; anything else keeps the first).
fn aggregate_scores(aggregate: &str, scores: &[f64]) -> f64 {
    match aggregate.to_ascii_lowercase().as_str() {
        "sum" => scores.iter().sum(),
        "min" => scores.iter().cloned().fold(f64::INFINITY, f64::min),
        "max" => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        _ => scores.first().copied().unwrap_or(0.0),
    }
}

/// Multiplies every score by the first weight when any were supplied
/// (mirroring Go's `applyWeights`).
fn apply_weights(map: &mut HashMap<Vec<u8>, f64>, weights: &[f64]) {
    if let Some(weight) = weights.first() {
        for score in map.values_mut() {
            *score *= *weight;
        }
    }
}

/// Members present in the first zset and none of the others (mirroring Go's
/// `zdiff`).
async fn zdiff_map(tx: &dyn Tx, nodes: &[NodeRef]) -> Result<HashMap<Vec<u8>, f64>, DbError> {
    let result = HashMap::new();
    let Some(first) = nodes.first() else {
        return Ok(result);
    };
    let mut diff = load_zset_map(tx, first).await?;
    for node in &nodes[1..] {
        let other = load_zset_map(tx, node).await?;
        for member in other.keys() {
            diff.remove(member);
        }
    }
    Ok(diff)
}

/// Members present in every zset, scores aggregated (mirroring Go's `zinter`).
async fn zinter_map(
    tx: &dyn Tx,
    aggregate: &str,
    weights: &[f64],
    nodes: &[NodeRef],
) -> Result<HashMap<Vec<u8>, f64>, DbError> {
    let mut maps = Vec::new();
    for node in nodes {
        maps.push(load_zset_map(tx, node).await?);
    }
    let mut result = intersect_scores(aggregate, &maps);
    apply_weights(&mut result, weights);
    Ok(result)
}

fn intersect_scores(aggregate: &str, maps: &[HashMap<Vec<u8>, f64>]) -> HashMap<Vec<u8>, f64> {
    let mut result = HashMap::new();
    let Some(first) = maps.first() else {
        return result;
    };
    for member in first.keys() {
        let mut present = true;
        let mut scores = Vec::new();
        for map in maps {
            match map.get(member) {
                Some(score) => scores.push(*score),
                None => {
                    present = false;
                    break;
                }
            }
        }
        if !present {
            continue;
        }
        result.insert(member.clone(), aggregate_scores(aggregate, &scores));
    }
    result
}

/// The union of every zset, with per-member scores aggregated across sources
/// (mirroring Go's `zunion`).
async fn zunion_map(
    tx: &dyn Tx,
    aggregate: &str,
    weights: &[f64],
    nodes: &[NodeRef],
) -> Result<HashMap<Vec<u8>, f64>, DbError> {
    let mut maps = Vec::new();
    for node in nodes {
        maps.push(load_zset_map(tx, node).await?);
    }
    let mut sources: HashMap<Vec<u8>, Vec<f64>> = HashMap::new();
    for map in &maps {
        for (member, score) in map {
            sources.entry(member.clone()).or_default().push(*score);
        }
    }
    let mut union = HashMap::new();
    for (member, scores) in sources {
        if scores.len() == 1 {
            union.insert(member, scores[0]);
        } else {
            union.insert(member, aggregate_scores(aggregate, &scores));
        }
    }
    apply_weights(&mut union, weights);
    Ok(union)
}

/// Stores `members` as a fresh zset under `dest`, replacing anything already
/// there, and returns the number of members stored (mirroring Go's
/// `storeZSetResult`). An empty slice leaves an empty sentinel in place.
async fn store_zset_result(
    tx: &dyn Tx,
    dest: &NodeRef,
    members: &[MemberScore],
) -> Result<i64, DbError> {
    clear_internal_keys(tx, dest).await?;
    for e in members {
        tx.set(Entry::new(dest.score_key(e.score, &e.member), Vec::new()))?;
        tx.set(Entry::new(dest.member_key(&e.member), score_bytes(e.score)))?;
    }
    write_sentinel(tx, &dest.public_key, members.len() as u32)?;
    Ok(members.len() as i64)
}

/// Flattens members into the `[member, score, ...]` array shape used by the
/// bulk-array replies (mirroring Go's `flattenMembers`).
fn flatten_members(members: &[MemberScore], with_scores: bool) -> Vec<Vec<u8>> {
    if with_scores {
        let mut result = Vec::with_capacity(members.len() * 2);
        for e in members {
            result.push(e.member.clone());
            result.push(format_f64(e.score).into_bytes());
        }
        return result;
    }
    members.iter().map(|e| e.member.clone()).collect()
}

// --- Command functions ---

/// `ZADD key [NX|XX] [CH] [GT|LT] [INCR] score member [score member ...]` —
/// adds or updates members, returning how many were added (or changed, with
/// `CH`). Claims and serves the longest-waiting `BZPOP*` client when this add
/// makes the set non-empty, exactly like Go.
pub fn zadd(session: &Session, key: &[u8], args: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZAddOp {
            node: NodeRef::new(session, key),
            raw_key: key.to_vec(),
            args: args.to_vec(),
            registry: session.registry(),
        }),
        wire_op: Box::new(ZAddWire),
        is_mutating: true,
    }
}

/// `ZCARD key` — returns the set cardinality, 0 if missing.
pub fn zcard(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZCardOp {
            node: NodeRef::new(session, key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
    }
}

/// `ZSCORE key member` — returns the member's score, or null if missing.
pub fn zscore(session: &Session, key: &[u8], member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZScoreOp {
            node: NodeRef::new(session, key),
            member: member.to_vec(),
        }),
        wire_op: Box::new(ScoreWire::Nullable),
        is_mutating: false,
    }
}

/// `ZREM key member [member ...]` — removes members, returning how many were
/// removed.
pub fn zrem(session: &Session, key: &[u8], members: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZRemOp {
            node: NodeRef::new(session, key),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZRANGE key start stop [WITHSCORES]` — members in `[start, stop]` by rank.
pub fn zrange(session: &Session, key: &[u8], start: i64, stop: i64, with_scores: bool) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RangeByRankOp {
            node: NodeRef::new(session, key),
            start,
            stop,
            with_scores,
            reverse: false,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZREVRANGE key start stop [WITHSCORES]` — `ZRANGE` in descending rank.
pub fn zrevrange(
    session: &Session,
    key: &[u8],
    start: i64,
    stop: i64,
    with_scores: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RangeByRankOp {
            node: NodeRef::new(session, key),
            start,
            stop,
            with_scores,
            reverse: true,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZRANK key member` — the member's ascending rank, or null if missing.
pub fn zrank(session: &Session, key: &[u8], member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RankOp {
            node: NodeRef::new(session, key),
            member: member.to_vec(),
            reverse: false,
        }),
        wire_op: Box::new(NullableIntWire),
        is_mutating: false,
    }
}

/// `ZREVRANK key member` — the member's descending rank, or null.
pub fn zrevrank(session: &Session, key: &[u8], member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RankOp {
            node: NodeRef::new(session, key),
            member: member.to_vec(),
            reverse: true,
        }),
        wire_op: Box::new(NullableIntWire),
        is_mutating: false,
    }
}

/// `ZCOUNT key min max` — members whose score falls in `[min, max]`.
pub fn zcount(session: &Session, key: &[u8], min_str: &str, max_str: &str) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ScoreRangeCountOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
    }
}

/// `ZINCRBY key increment member` — increments a member's score, returning
/// the new score.
pub fn zincrby(session: &Session, key: &[u8], increment: f64, member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZIncrByOp {
            node: NodeRef::new(session, key),
            increment,
            member: member.to_vec(),
        }),
        wire_op: Box::new(ScoreWire::Plain),
        is_mutating: true,
    }
}

/// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` — members
/// whose score falls in `[min, max]`, ascending.
pub fn zrangebyscore(
    session: &Session,
    key: &[u8],
    min_str: &str,
    max_str: &str,
    with_scores: bool,
    limit: Option<(i64, i64)>,
) -> QueuedOp {
    let (limit_offset, limit_count, has_limit) = match limit {
        Some((offset, count)) => (offset, count, true),
        None => (0, 0, false),
    };
    QueuedOp {
        db_op: Box::new(RangeByScoreOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
            with_scores,
            limit_offset,
            limit_count,
            has_limit,
            reverse: false,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` — note the
/// reversed argument order; result is descending.
pub fn zrevrangebyscore(
    session: &Session,
    key: &[u8],
    max_str: &str,
    min_str: &str,
    with_scores: bool,
    limit: Option<(i64, i64)>,
) -> QueuedOp {
    let (limit_offset, limit_count, has_limit) = match limit {
        Some((offset, count)) => (offset, count, true),
        None => (0, 0, false),
    };
    QueuedOp {
        db_op: Box::new(RangeByScoreOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
            with_scores,
            limit_offset,
            limit_count,
            has_limit,
            reverse: true,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZRANGEBYLEX key min max [LIMIT offset count]` — members in a
/// lexicographic range, ascending.
pub fn zrangebylex(
    session: &Session,
    key: &[u8],
    min_str: &str,
    max_str: &str,
    limit_offset: i64,
    limit_count: i64,
    has_limit: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RangeByLexOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
            limit_offset,
            limit_count,
            has_limit,
            reverse: false,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZREVRANGEBYLEX key max min [LIMIT offset count]` — lexicographic range,
/// descending, with the bounds reversed.
pub fn zrevrangebylex(
    session: &Session,
    key: &[u8],
    max_str: &str,
    min_str: &str,
    limit_offset: i64,
    limit_count: i64,
    has_limit: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RangeByLexOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
            limit_offset,
            limit_count,
            has_limit,
            reverse: true,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZLEXCOUNT key min max` — members in a lexicographic range.
pub fn zlexcount(session: &Session, key: &[u8], min_str: &str, max_str: &str) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LexCountOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
    }
}

/// `ZREMRANGEBYRANK key start stop` — removes members by rank.
pub fn zremrangebyrank(session: &Session, key: &[u8], start: i64, stop: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RemRangeByRankOp {
            node: NodeRef::new(session, key),
            start,
            stop,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZREMRANGEBYSCORE key min max` — removes members whose score is in range.
pub fn zremrangebyscore(session: &Session, key: &[u8], min_str: &str, max_str: &str) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RemRangeByScoreOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZREMRANGEBYLEX key min max` — removes members in a lexicographic range.
pub fn zremrangebylex(session: &Session, key: &[u8], min_str: &str, max_str: &str) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RemRangeByLexOp {
            node: NodeRef::new(session, key),
            min_str: min_str.to_string(),
            max_str: max_str.to_string(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZPOPMIN key [count]` — removes and returns the lowest-score members.
pub fn zpopmin(session: &Session, key: &[u8], count: usize) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZPopOp {
            node: NodeRef::new(session, key),
            count,
            want_min: true,
        }),
        wire_op: Box::new(MemberScoreArrayWire { with_scores: true }),
        is_mutating: true,
    }
}

/// `ZPOPMAX key [count]` — removes and returns the highest-score members.
pub fn zpopmax(session: &Session, key: &[u8], count: usize) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZPopOp {
            node: NodeRef::new(session, key),
            count,
            want_min: false,
        }),
        wire_op: Box::new(MemberScoreArrayWire { with_scores: true }),
        is_mutating: true,
    }
}

/// `ZMSCORE key member [member ...]` — scores for the given members, null for
/// absent ones.
pub fn zmscore(session: &Session, key: &[u8], members: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZMScoreOp {
            node: NodeRef::new(session, key),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }),
        wire_op: Box::new(ZMScoreWire),
        is_mutating: false,
    }
}

/// `ZRANDMEMBER key [count]` — random members. A negative `count` includes
/// scores.
pub fn zrandmember(session: &Session, key: &[u8], count: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZRandMemberOp {
            node: NodeRef::new(session, key),
            count,
        }),
        wire_op: Box::new(MemberScoreArrayWire {
            with_scores: count < 0,
        }),
        is_mutating: false,
    }
}

/// `ZDIFF numkeys key [key ...] [WITHSCORES]` — members in the first zset but
/// none of the others.
pub fn zdiff(session: &Session, with_scores: bool, keys: &[Bytes]) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(SetOp::new(
            nodes,
            String::new(),
            Vec::new(),
            with_scores,
            SetOpKind::Diff,
        )),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZDIFFSTORE destination numkeys key [key ...]` — stores the set
/// difference, returning the stored cardinality.
pub fn zdiffstore(session: &Session, dest: &[u8], keys: &[Bytes]) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(StoreSetOp {
            dest: NodeRef::new(session, dest),
            nodes,
            aggregate: String::new(),
            weights: Vec::new(),
            kind: SetOpKind::Diff,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZINTER numkeys key [key ...] [WEIGHTS w...] [AGGREGATE sum|min|max]
/// [WITHSCORES]` — members present in every zset.
pub fn zinter(
    session: &Session,
    aggregate: &str,
    weights: &[f64],
    with_scores: bool,
    keys: &[Bytes],
) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(SetOp::new(
            nodes,
            aggregate.to_string(),
            weights.to_vec(),
            with_scores,
            SetOpKind::Inter,
        )),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS w...]
/// [AGGREGATE sum|min|max]` — stores the intersection.
pub fn zinterstore(
    session: &Session,
    dest: &[u8],
    aggregate: &str,
    weights: &[f64],
    keys: &[Bytes],
) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(StoreSetOp {
            dest: NodeRef::new(session, dest),
            nodes,
            aggregate: aggregate.to_string(),
            weights: weights.to_vec(),
            kind: SetOpKind::Inter,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZUNION numkeys key [key ...] [WEIGHTS w...] [AGGREGATE sum|min|max]
/// [WITHSCORES]` — the union of the given zsets.
pub fn zunion(
    session: &Session,
    aggregate: &str,
    weights: &[f64],
    with_scores: bool,
    keys: &[Bytes],
) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(SetOp::new(
            nodes,
            aggregate.to_string(),
            weights.to_vec(),
            with_scores,
            SetOpKind::Union,
        )),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
    }
}

/// `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w...]
/// [AGGREGATE sum|min|max]` — stores the union.
pub fn zunionstore(
    session: &Session,
    dest: &[u8],
    aggregate: &str,
    weights: &[f64],
    keys: &[Bytes],
) -> QueuedOp {
    let nodes: Vec<NodeRef> = keys.iter().map(|k| NodeRef::new(session, k)).collect();
    QueuedOp {
        db_op: Box::new(StoreSetOp {
            dest: NodeRef::new(session, dest),
            nodes,
            aggregate: aggregate.to_string(),
            weights: weights.to_vec(),
            kind: SetOpKind::Union,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `ZRANGESTORE destination source start stop` — stores the rank range of
/// `source` into `destination`, returning the stored cardinality.
pub fn zrangestore(session: &Session, dest: &[u8], src: &[u8], start: i64, stop: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ZRangeStoreOp {
            dest: NodeRef::new(session, dest),
            src: NodeRef::new(session, src),
            start,
            stop,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

// --- DbOp halves ---

/// The integer-returning read families shared by several commands.
struct ZCardOp {
    node: NodeRef,
}

impl DbOp for ZCardOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        Box::pin(async move {
            let count = zset_count(tx, &node.public_key).await?.unwrap_or(0);
            let result: DbResult = Box::new(count as i64);
            Ok(result)
        })
    }
}

struct ZScoreOp {
    node: NodeRef,
    member: Vec<u8>,
}

impl DbOp for ZScoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let member = self.member.clone();
        Box::pin(async move {
            if zset_count(tx, &node.public_key).await?.is_none() {
                let result: DbResult = Box::new(None::<f64>);
                return Ok(result);
            }
            let key = node.member_key(&member);
            let score = match tx.get(&key).await {
                Ok(item) => {
                    let val = item.value();
                    if val.len() < 8 {
                        return Err(DbError::Kv(KvError::KeyNotFound));
                    }
                    Some(f64::from_bits(u64::from_be_bytes(
                        val[0..8].try_into().expect("slice in range"),
                    )))
                }
                Err(KvError::KeyNotFound) => None,
                Err(e) => return Err(e.into()),
            };
            let result: DbResult = Box::new(score);
            Ok(result)
        })
    }
}

struct ZRemOp {
    node: NodeRef,
    members: Vec<Vec<u8>>,
}

impl DbOp for ZRemOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let members = self.members.clone();
        Box::pin(async move {
            let mut count = match zset_count(tx, &node.public_key).await? {
                Some(count) => count,
                None => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
            };
            let mut removed = 0i64;
            for member in &members {
                let member_key = node.member_key(member);
                let score = match tx.get(&member_key).await {
                    Ok(item) => {
                        let val = item.value();
                        if val.len() < 8 {
                            return Err(DbError::Kv(KvError::KeyNotFound));
                        }
                        f64::from_bits(u64::from_be_bytes(
                            val[0..8].try_into().expect("slice in range"),
                        ))
                    }
                    Err(KvError::KeyNotFound) => continue,
                    Err(e) => return Err(e.into()),
                };
                tx.delete(&member_key)?;
                tx.delete(&node.score_key(score, member))?;
                count -= 1;
                removed += 1;
            }
            if count == 0 {
                tx.delete(&node.public_key)?;
            } else {
                write_sentinel(tx, &node.public_key, count)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

/// `ZRANGE`/`ZREVRANGE`.
struct RangeByRankOp {
    node: NodeRef,
    start: i64,
    stop: i64,
    with_scores: bool,
    reverse: bool,
}

impl DbOp for RangeByRankOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let (start, stop, with_scores, reverse) =
            (self.start, self.stop, self.with_scores, self.reverse);
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let n = entries.len();
            let Some((lo, hi)) = range_indexes(n, start, stop) else {
                let empty: Vec<Vec<u8>> = Vec::new();
                let result: DbResult = Box::new(empty);
                return Ok(result);
            };
            let result = if reverse {
                // Reverse: iterate from (n-1-start) down to (n-1-stop).
                let r_hi = n - 1 - lo;
                let r_lo = n - 1 - hi;
                let mut flat: Vec<Vec<u8>> =
                    Vec::with_capacity((r_hi - r_lo + 1) * if with_scores { 2 } else { 1 });
                for i in (r_lo..=r_hi).rev() {
                    flat.push(entries[i].member.clone());
                    if with_scores {
                        flat.push(format_f64(entries[i].score).into_bytes());
                    }
                }
                flat
            } else {
                flatten_members(&entries[lo..=hi], with_scores)
            };
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// `ZRANK`/`ZREVRANK`.
struct RankOp {
    node: NodeRef,
    member: Vec<u8>,
    reverse: bool,
}

impl DbOp for RankOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let member = self.member.clone();
        let reverse = self.reverse;
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let mut rank = None;
            for (i, e) in entries.iter().enumerate() {
                if e.member == member {
                    rank = Some(if reverse {
                        (entries.len() - 1 - i) as i64
                    } else {
                        i as i64
                    });
                    break;
                }
            }
            let result: DbResult = Box::new(rank);
            Ok(result)
        })
    }
}

/// `ZCOUNT`.
struct ScoreRangeCountOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
}

impl DbOp for ScoreRangeCountOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let min_str = self.min_str.clone();
        let max_str = self.max_str.clone();
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let (min_val, min_excl) = parse_float_bound(&min_str)?;
            let (max_val, max_excl) = parse_float_bound(&max_str)?;
            let mut count = 0i64;
            for e in &entries {
                if min_excl && e.score <= min_val {
                    continue;
                }
                if !min_excl && e.score < min_val {
                    continue;
                }
                if max_excl && e.score >= max_val {
                    continue;
                }
                if !max_excl && e.score > max_val {
                    continue;
                }
                count += 1;
            }
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

/// The `ZADD` op. Runs the option/pair parsing inside the transaction (so
/// option errors surface at `EXEC`, like Go) and claims a blocked `BZPOP*`
/// waiter when the write adds members.
struct ZAddOp {
    node: NodeRef,
    raw_key: Vec<u8>,
    args: Vec<Bytes>,
    registry: Arc<WatchRegistry>,
}

impl ZAddOp {
    /// Returns any claim held by a committed [`ZAddResult`] back to the front
    /// of its queues so the waiter stays the longest-waiting client.
    fn release_claims_impl(&self, result: &DbResult) {
        if let Some(zadd) = result.downcast_ref::<ZAddResult>() {
            if let Some(claim) = &zadd.claim {
                self.registry.release_front(claim);
            }
        }
    }
}

struct ZAddResult {
    count: i64,
    claim: Option<Claim>,
}

impl DbOp for ZAddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let raw_key = self.raw_key.clone();
        let args = self.args.clone();
        let registry = self.registry.clone();
        Box::pin(async move {
            if args.is_empty() {
                return Err(DbError::Redis(
                    "wrong number of arguments for 'zadd' command".into(),
                ));
            }

            let mut nx = false;
            let mut xx = false;
            let mut ch = false;
            let mut gt = false;
            let mut lt = false;
            let mut idx = 0;
            while idx < args.len() {
                let arg = args[idx].to_ascii_lowercase();
                if arg.eq_ignore_ascii_case(b"nx") {
                    nx = true;
                    idx += 1;
                } else if arg.eq_ignore_ascii_case(b"xx") {
                    xx = true;
                    idx += 1;
                } else if arg.eq_ignore_ascii_case(b"ch") {
                    ch = true;
                    idx += 1;
                } else if arg.eq_ignore_ascii_case(b"gt") {
                    gt = true;
                    idx += 1;
                } else if arg.eq_ignore_ascii_case(b"lt") {
                    lt = true;
                    idx += 1;
                } else {
                    break;
                }
            }

            if nx && xx {
                return Err(DbError::Redis(
                    "XX and NX, XX and GT/LT, NX and GT/LT options are not compatible".into(),
                ));
            }
            if nx && (gt || lt) {
                return Err(DbError::Redis(
                    "XX and NX, XX and GT/LT, NX and GT/LT options are not compatible".into(),
                ));
            }
            if xx && (gt || lt) {
                return Err(DbError::Redis(
                    "XX and NX, XX and GT/LT, NX and GT/LT options are not compatible".into(),
                ));
            }
            if gt && lt {
                return Err(DbError::Redis(
                    "GT and LT options are not compatible".into(),
                ));
            }

            let remaining = &args[idx..];
            if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                return Err(DbError::Redis(
                    "wrong number of arguments for 'zadd' command".into(),
                ));
            }

            let mut count = match read_sentinel(tx, &node.public_key).await {
                Ok(count) => count,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    if xx {
                        let result: DbResult = Box::new(ZAddResult {
                            count: 0,
                            claim: None,
                        });
                        return Ok(result);
                    }
                    0
                }
                Err(e) => return Err(e),
            };

            let mut changed = 0i64;
            let mut added = 0i64;
            for i in (0..remaining.len()).step_by(2) {
                let score_str = std::str::from_utf8(&remaining[i])
                    .map_err(|_| DbError::Redis("value is not a valid float".into()))?;
                let score: f64 = score_str
                    .parse()
                    .map_err(|_| DbError::Redis("value is not a valid float".into()))?;
                if score.is_nan() {
                    return Err(DbError::Redis("value is not a valid float".into()));
                }
                let member = remaining[i + 1].to_vec();
                let member_key = node.member_key(&member);
                match tx.get(&member_key).await {
                    Err(KvError::KeyNotFound) => {
                        // New member.
                        if !xx {
                            tx.set(Entry::new(node.score_key(score, &member), Vec::new()))?;
                            tx.set(Entry::new(member_key, score_bytes(score)))?;
                            count += 1;
                            added += 1;
                        }
                        // XX skips non-existing members.
                    }
                    Err(e) => return Err(e.into()),
                    Ok(item) => {
                        // Member exists.
                        if nx {
                            continue;
                        }
                        let val = item.value();
                        if val.len() < 8 {
                            return Err(DbError::Kv(KvError::KeyNotFound));
                        }
                        let old_score = f64::from_bits(u64::from_be_bytes(
                            val[0..8].try_into().expect("slice in range"),
                        ));
                        if gt && score.partial_cmp(&old_score) != Some(std::cmp::Ordering::Greater)
                        {
                            continue;
                        }
                        if lt && score.partial_cmp(&old_score) != Some(std::cmp::Ordering::Less) {
                            continue;
                        }
                        if old_score != score {
                            tx.delete(&node.score_key(old_score, &member))?;
                            tx.set(Entry::new(node.score_key(score, &member), Vec::new()))?;
                            tx.set(Entry::new(member_key, score_bytes(score)))?;
                            changed += 1;
                        }
                    }
                }
            }

            if added > 0 || changed > 0 {
                write_sentinel(tx, &node.public_key, count)?;
            }

            // After persisting the write, claim the front waiter (if any) and
            // pop one element on its behalf, atomically within this
            // transaction. The claim is returned so the wire side can wake
            // the waiter once the transaction commits.
            let mut claim: Option<Claim> = None;
            if added > 0 {
                claim = registry.try_claim(&node.public_key);
                if let Some(claim_ref) = &mut claim {
                    let popped = if claim_ref.want_min() {
                        pop_one_min(tx, &node).await
                    } else {
                        pop_one_max(tx, &node).await
                    };
                    match popped {
                        Err(e) => {
                            registry.release_front(claim_ref);
                            return Err(e);
                        }
                        Ok(Some(p)) => {
                            claim_ref.set_result(PopResult {
                                key: raw_key,
                                member: p.member,
                                score: p.score,
                            });
                        }
                        Ok(None) => {}
                    }
                }
            }

            let n = changed + added;
            let result: DbResult = Box::new(ZAddResult {
                count: if ch { n } else { added },
                claim,
            });
            Ok(result)
        })
    }

    fn release_claims(&self, result: &DbResult) {
        self.release_claims_impl(result);
    }
}

/// `ZINCRBY`.
struct ZIncrByOp {
    node: NodeRef,
    increment: f64,
    member: Vec<u8>,
}

impl DbOp for ZIncrByOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let increment = self.increment;
        let member = self.member.clone();
        Box::pin(async move {
            let existing_count = zset_count(tx, &node.public_key).await?;
            let member_key = node.member_key(&member);
            let score = match tx.get(&member_key).await {
                Err(KvError::KeyNotFound) => {
                    // New member with the increment as its score.
                    if increment.is_nan() {
                        return Err(DbError::Redis(
                            "resulting score is not a valid float".into(),
                        ));
                    }
                    tx.set(Entry::new(node.score_key(increment, &member), Vec::new()))?;
                    tx.set(Entry::new(member_key, score_bytes(increment)))?;
                    let new_count = existing_count.map_or(1, |c| c + 1);
                    write_sentinel(tx, &node.public_key, new_count)?;
                    let result: DbResult = Box::new(increment);
                    return Ok(result);
                }
                Ok(item) => {
                    let val = item.value();
                    if val.len() < 8 {
                        return Err(DbError::Kv(KvError::KeyNotFound));
                    }
                    f64::from_bits(u64::from_be_bytes(
                        val[0..8].try_into().expect("slice in range"),
                    ))
                }
                Err(e) => return Err(e.into()),
            };

            let new_score = score + increment;
            if new_score.is_nan() {
                return Err(DbError::Redis(
                    "resulting score is not a valid float".into(),
                ));
            }
            tx.delete(&node.score_key(score, &member))?;
            tx.set(Entry::new(node.score_key(new_score, &member), Vec::new()))?;
            tx.set(Entry::new(member_key, score_bytes(new_score)))?;
            let result: DbResult = Box::new(new_score);
            Ok(result)
        })
    }
}

/// `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`.
struct RangeByScoreOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
    with_scores: bool,
    limit_offset: i64,
    limit_count: i64,
    has_limit: bool,
    reverse: bool,
}

impl DbOp for RangeByScoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let (min_str, max_str, with_scores) =
            (self.min_str.clone(), self.max_str.clone(), self.with_scores);
        let (limit_offset, limit_count, has_limit, reverse) = (
            self.limit_offset,
            self.limit_count,
            self.has_limit,
            self.reverse,
        );
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let (min_val, min_excl) = parse_float_bound(&min_str)?;
            let (max_val, max_excl) = parse_float_bound(&max_str)?;

            let mut filtered: Vec<MemberScore> = Vec::new();
            for e in &entries {
                if min_excl && e.score <= min_val {
                    continue;
                }
                if !min_excl && e.score < min_val {
                    continue;
                }
                if max_excl && e.score >= max_val {
                    continue;
                }
                if !max_excl && e.score > max_val {
                    continue;
                }
                filtered.push(MemberScore {
                    member: e.member.clone(),
                    score: e.score,
                });
            }

            if has_limit {
                let mut offset = limit_offset;
                let mut count = limit_count;
                if offset < 0 {
                    offset = 0;
                }
                if offset >= filtered.len() as i64 {
                    let empty: Vec<Vec<u8>> = Vec::new();
                    let result: DbResult = Box::new(empty);
                    return Ok(result);
                }
                if count < 0 {
                    count = filtered.len() as i64 - offset;
                }
                filtered = filtered.split_off(offset as usize);
                if (count as usize) < filtered.len() {
                    filtered.truncate(count as usize);
                }
            }

            let flat = flatten_members(&filtered, with_scores);
            let result = if reverse {
                reverse_bulk_array(flat, with_scores)
            } else {
                flat
            };
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// Reverses an already-flattened `[member, score, ...]` array (member/score
/// pairs kept adjacent), mirroring Go's inline reversal in
/// `ZRevRangeByScore`/`ZRevRangeByLex`.
fn reverse_bulk_array(flat: Vec<Vec<u8>>, with_scores: bool) -> Vec<Vec<u8>> {
    let mut flat = flat;
    let step = if with_scores { 2usize } else { 1usize };
    let mut i = 0;
    let mut j = flat.len().saturating_sub(step);
    while i < j {
        for k in 0..step {
            flat.swap(i + k, j + k);
        }
        i += step;
        j = j.saturating_sub(step);
    }
    flat
}

/// `ZRANGEBYLEX`/`ZREVRANGEBYLEX`.
struct RangeByLexOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
    limit_offset: i64,
    limit_count: i64,
    has_limit: bool,
    reverse: bool,
}

impl DbOp for RangeByLexOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let (min_str, max_str) = (self.min_str.clone(), self.max_str.clone());
        let (limit_offset, limit_count, has_limit, reverse) = (
            self.limit_offset,
            self.limit_count,
            self.has_limit,
            self.reverse,
        );
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let members: Vec<Vec<u8>> = entries.iter().map(|e| e.member.clone()).collect();
            let mut result = filter_lex_range(&members, &min_str, &max_str)?;

            if has_limit {
                let mut offset = limit_offset;
                let mut count = limit_count;
                if offset < 0 {
                    offset = 0;
                }
                if offset >= result.len() as i64 {
                    let empty: Vec<Vec<u8>> = Vec::new();
                    let result: DbResult = Box::new(empty);
                    return Ok(result);
                }
                if count < 0 {
                    count = result.len() as i64 - offset;
                }
                result = result.split_off(offset as usize);
                if (count as usize) < result.len() {
                    result.truncate(count as usize);
                }
            }

            if reverse {
                result.reverse();
            }
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// `ZLEXCOUNT`.
struct LexCountOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
}

impl DbOp for LexCountOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let min_str = self.min_str.clone();
        let max_str = self.max_str.clone();
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let members: Vec<Vec<u8>> = entries.iter().map(|e| e.member.clone()).collect();
            let result = filter_lex_range(&members, &min_str, &max_str)?;
            let result: DbResult = Box::new(result.len() as i64);
            Ok(result)
        })
    }
}

/// `ZREMRANGEBYRANK`.
struct RemRangeByRankOp {
    node: NodeRef,
    start: i64,
    stop: i64,
}

impl DbOp for RemRangeByRankOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let (start, stop) = (self.start, self.stop);
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let n = entries.len();
            let Some((lo, hi)) = range_indexes(n, start, stop) else {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            };
            let mut removed = 0i64;
            for e in &entries[lo..=hi] {
                tx.delete(&node.member_key(&e.member))?;
                tx.delete(&node.score_key(e.score, &e.member))?;
                removed += 1;
            }
            let new_count = n as i64 - removed;
            if new_count == 0 {
                tx.delete(&node.public_key)?;
            } else {
                write_sentinel(tx, &node.public_key, new_count as u32)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

/// `ZREMRANGEBYSCORE`.
struct RemRangeByScoreOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
}

impl DbOp for RemRangeByScoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let min_str = self.min_str.clone();
        let max_str = self.max_str.clone();
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let (min_val, min_excl) = parse_float_bound(&min_str)?;
            let (max_val, max_excl) = parse_float_bound(&max_str)?;

            let mut removed = 0i64;
            for e in &entries {
                let mut in_range = true;
                if min_excl && e.score <= min_val {
                    in_range = false;
                }
                if !min_excl && e.score < min_val {
                    in_range = false;
                }
                if max_excl && e.score >= max_val {
                    in_range = false;
                }
                if !max_excl && e.score > max_val {
                    in_range = false;
                }
                if !in_range {
                    continue;
                }
                tx.delete(&node.member_key(&e.member))?;
                tx.delete(&node.score_key(e.score, &e.member))?;
                removed += 1;
            }

            if removed == 0 {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }
            let new_count = match zset_count(tx, &node.public_key).await? {
                Some(c) => c as i64 - removed,
                None => return Err(DbError::Kv(KvError::KeyNotFound)),
            };
            if new_count == 0 {
                tx.delete(&node.public_key)?;
            } else {
                write_sentinel(tx, &node.public_key, new_count as u32)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

/// `ZREMRANGEBYLEX`.
struct RemRangeByLexOp {
    node: NodeRef,
    min_str: String,
    max_str: String,
}

impl DbOp for RemRangeByLexOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let min_str = self.min_str.clone();
        let max_str = self.max_str.clone();
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let members: Vec<Vec<u8>> = entries.iter().map(|e| e.member.clone()).collect();
            let to_remove = filter_lex_range(&members, &min_str, &max_str)?;
            let remove_map: HashMap<Vec<u8>, bool> =
                to_remove.into_iter().map(|m| (m, true)).collect();

            let mut removed = 0i64;
            for e in &entries {
                if !remove_map.contains_key(&e.member) {
                    continue;
                }
                tx.delete(&node.member_key(&e.member))?;
                tx.delete(&node.score_key(e.score, &e.member))?;
                removed += 1;
            }

            if removed == 0 {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }
            let new_count = match zset_count(tx, &node.public_key).await? {
                Some(c) => c as i64 - removed,
                None => return Err(DbError::Kv(KvError::KeyNotFound)),
            };
            if new_count == 0 {
                tx.delete(&node.public_key)?;
            } else {
                write_sentinel(tx, &node.public_key, new_count as u32)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

/// `ZPOPMIN`/`ZPOPMAX`.
struct ZPopOp {
    node: NodeRef,
    count: usize,
    want_min: bool,
}

impl DbOp for ZPopOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let count = self.count;
        let want_min = self.want_min;
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let mut count = count.min(entries.len());
            if want_min {
                count = count.min(entries.len());
                let popped = &entries[..count];
                for e in popped {
                    tx.delete(&node.member_key(&e.member))?;
                    tx.delete(&node.score_key(e.score, &e.member))?;
                }
                let new_count = entries.len() - count;
                if new_count == 0 {
                    tx.delete(&node.public_key)?;
                } else {
                    write_sentinel(tx, &node.public_key, new_count as u32)?;
                }
                let result: DbResult = Box::new(
                    popped
                        .iter()
                        .map(|e| MemberScore {
                            member: e.member.clone(),
                            score: e.score,
                        })
                        .collect::<Vec<MemberScore>>(),
                );
                Ok(result)
            } else {
                let popped = &entries[entries.len() - count..];
                for e in popped {
                    tx.delete(&node.member_key(&e.member))?;
                    tx.delete(&node.score_key(e.score, &e.member))?;
                }
                let new_count = entries.len() - count;
                if new_count == 0 {
                    tx.delete(&node.public_key)?;
                } else {
                    write_sentinel(tx, &node.public_key, new_count as u32)?;
                }
                let result: DbResult = Box::new(
                    popped
                        .iter()
                        .map(|e| MemberScore {
                            member: e.member.clone(),
                            score: e.score,
                        })
                        .collect::<Vec<MemberScore>>(),
                );
                Ok(result)
            }
        })
    }
}

/// `ZMSCORE`.
struct ZMScoreOp {
    node: NodeRef,
    members: Vec<Vec<u8>>,
}

struct ZMScoreResult {
    scores: Vec<Option<f64>>,
}

impl DbOp for ZMScoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let members = self.members.clone();
        Box::pin(async move {
            if zset_count(tx, &node.public_key).await?.is_none() {
                let result = ZMScoreResult {
                    scores: vec![None; members.len()],
                };
                let result: DbResult = Box::new(result);
                return Ok(result);
            }
            let mut scores = Vec::with_capacity(members.len());
            for member in &members {
                let score = match tx.get(&node.member_key(member)).await {
                    Ok(item) => {
                        let val = item.value();
                        if val.len() < 8 {
                            return Err(DbError::Kv(KvError::KeyNotFound));
                        }
                        Some(f64::from_bits(u64::from_be_bytes(
                            val[0..8].try_into().expect("slice in range"),
                        )))
                    }
                    Err(KvError::KeyNotFound) => None,
                    Err(e) => return Err(e.into()),
                };
                scores.push(score);
            }
            let result: DbResult = Box::new(ZMScoreResult { scores });
            Ok(result)
        })
    }
}

/// `ZRANDMEMBER`.
struct ZRandMemberOp {
    node: NodeRef,
    count: i64,
}

impl DbOp for ZRandMemberOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node = self.node.clone();
        let count = self.count;
        Box::pin(async move {
            let entries = load_all_members(tx, &node).await?;
            let n = entries.len();
            let mut count = count;
            if count == 0 || n == 0 {
                let empty: Vec<MemberScore> = Vec::new();
                let result: DbResult = Box::new(empty);
                return Ok(result);
            }
            if count < 0 {
                count = -count;
            }
            if count >= n as i64 {
                count = n as i64;
            }
            let perm = random_perm(n);
            let mut result = Vec::with_capacity(count as usize);
            for i in 0..count as usize {
                result.push(MemberScore {
                    member: entries[perm[i]].member.clone(),
                    score: entries[perm[i]].score,
                });
            }
            if self.count >= 0 {
                for e in &mut result {
                    e.score = 0.0;
                }
            }
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// Monotonic counter mixed into [`random_perm`] so successive calls in the
/// same nanosecond still differ.
static RAND_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a pseudo-random index in `[0, n)`, or 0 when `n == 0`.
fn rand_index(n: u32) -> usize {
    if n == 0 {
        return 0;
    }
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = RAND_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut state = epoch ^ count.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    (state.wrapping_mul(0x2545_f491_bf4d_2d2d) % (n as u64)) as usize
}

/// A random permutation of `[0, n)`, mirroring Go's `rand.Perm`.
fn random_perm(n: usize) -> Vec<usize> {
    let mut perm: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rand_index((i + 1) as u32);
        perm.swap(i, j);
    }
    perm
}

/// The read side of `ZDIFF`/`ZINTER`/`ZUNION`.
#[derive(Clone, Copy)]
enum SetOpKind {
    Diff,
    Inter,
    Union,
}

struct SetOp {
    nodes: Vec<NodeRef>,
    aggregate: String,
    weights: Vec<f64>,
    with_scores: bool,
    kind: SetOpKind,
}

impl SetOp {
    fn new(
        nodes: Vec<NodeRef>,
        aggregate: String,
        weights: Vec<f64>,
        with_scores: bool,
        kind: SetOpKind,
    ) -> Self {
        Self {
            nodes,
            aggregate,
            weights,
            with_scores,
            kind,
        }
    }
}

impl DbOp for SetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let nodes = self.nodes.clone();
        let aggregate = self.aggregate.clone();
        let weights = self.weights.clone();
        let with_scores = self.with_scores;
        let kind = self.kind;
        Box::pin(async move {
            let map = match kind {
                SetOpKind::Diff => zdiff_map(tx, &nodes).await?,
                SetOpKind::Inter => zinter_map(tx, &aggregate, &weights, &nodes).await?,
                SetOpKind::Union => zunion_map(tx, &aggregate, &weights, &nodes).await?,
            };
            let result = flatten_members(&zset_to_slice(&map), with_scores);
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// The store side of `ZDIFFSTORE`/`ZINTERSTORE`/`ZUNIONSTORE`.
struct StoreSetOp {
    dest: NodeRef,
    nodes: Vec<NodeRef>,
    aggregate: String,
    weights: Vec<f64>,
    kind: SetOpKind,
}

impl DbOp for StoreSetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let dest = self.dest.clone();
        let nodes = self.nodes.clone();
        let aggregate = self.aggregate.clone();
        let weights = self.weights.clone();
        let kind = self.kind;
        Box::pin(async move {
            let map = match kind {
                SetOpKind::Diff => zdiff_map(tx, &nodes).await?,
                SetOpKind::Inter => zinter_map(tx, &aggregate, &weights, &nodes).await?,
                SetOpKind::Union => zunion_map(tx, &aggregate, &weights, &nodes).await?,
            };
            let n = store_zset_result(tx, &dest, &zset_to_slice(&map)).await?;
            let result: DbResult = Box::new(n);
            Ok(result)
        })
    }
}

/// `ZRANGESTORE`.
struct ZRangeStoreOp {
    dest: NodeRef,
    src: NodeRef,
    start: i64,
    stop: i64,
}

impl DbOp for ZRangeStoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let dest = self.dest.clone();
        let src = self.src.clone();
        let (start, stop) = (self.start, self.stop);
        Box::pin(async move {
            let entries = load_all_members(tx, &src).await?;
            let n = match range_indexes(entries.len(), start, stop) {
                Some((lo, hi)) => {
                    let n = store_zset_result(tx, &dest, &entries[lo..=hi]).await?;
                    n
                }
                None => store_zset_result(tx, &dest, &[]).await?,
            };
            let result: DbResult = Box::new(n);
            Ok(result)
        })
    }
}

// --- WireOp halves ---

/// Replies an integer result.
struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(value) => RespValue::Integer(*value),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a null bulk for `None`, an integer otherwise.
struct NullableIntWire;

impl WireOp for NullableIntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<i64>>() {
                Ok(boxed) => match *boxed {
                    Some(value) => RespValue::Integer(value),
                    None => RespValue::BulkString(None),
                },
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `ZSCORE`/`ZINCRBY`: a bulk serialized float, or null for a missing
/// member.
enum ScoreWire {
    Nullable,
    Plain,
}

impl WireOp for ScoreWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<f64>>() {
                Ok(boxed) => match *boxed {
                    Some(score) => RespValue::BulkString(Some(Bytes::from(format_f64(score)))),
                    None => RespValue::BulkString(None),
                },
                Err(res) => match res.downcast::<f64>() {
                    Ok(score) => RespValue::BulkString(Some(Bytes::from(format_f64(*score)))),
                    Err(_) => internal_error(),
                },
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an array of bulk strings.
struct BulkArrayWire;

impl WireOp for BulkArrayWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Vec<u8>>>() {
                Ok(boxed) => RespValue::Array(Some(
                    boxed
                        .iter()
                        .map(|v| RespValue::BulkString(Some(Bytes::copy_from_slice(v))))
                        .collect(),
                )),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an array of member [/ score] pairs from a `Vec<MemberScore>`.
struct MemberScoreArrayWire {
    with_scores: bool,
}

impl WireOp for MemberScoreArrayWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<MemberScore>>() {
                Ok(boxed) => {
                    let mut items = Vec::with_capacity(boxed.len() * 2);
                    for e in boxed.iter() {
                        items.push(RespValue::BulkString(Some(Bytes::copy_from_slice(
                            &e.member,
                        ))));
                        if self.with_scores {
                            items.push(RespValue::BulkString(Some(Bytes::from(format_f64(
                                e.score,
                            )))));
                        }
                    }
                    RespValue::Array(Some(items))
                }
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `ZMSCORE`: an array of bulk scores or nulls, in member order.
struct ZMScoreWire;

impl WireOp for ZMScoreWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<ZMScoreResult>() {
                Ok(boxed) => RespValue::Array(Some(
                    boxed
                        .scores
                        .iter()
                        .map(|s| match s {
                            Some(score) => {
                                RespValue::BulkString(Some(Bytes::from(format_f64(*score))))
                            }
                            None => RespValue::BulkString(None),
                        })
                        .collect(),
                )),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `ZADD`: the added/changed count, then wakes any blocked waiter the
/// DbOp claimed (only after the transaction has committed).
struct ZAddWire;

impl WireOp for ZAddWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<ZAddResult>() {
                Ok(boxed) => {
                    if let Some(claim) = &boxed.claim {
                        claim.wake();
                    }
                    RespValue::Integer(boxed.count)
                }
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

// --- Blocking pops ---

/// The reply for a successful `BZPOPMIN`/`BZPOPMAX`: a 3-element array of
/// `[key, member, score]`.
fn bzpop_reply_value(p: PopResult) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from(p.key))),
        RespValue::BulkString(Some(Bytes::from(p.member))),
        RespValue::BulkString(Some(Bytes::from(format_f64(p.score)))),
    ]))
}

/// The shared implementation of `BZPOPMIN` and `BZPOPMAX`.
///
/// Step 1: attempt an immediate non-blocking pop across the keys in order
/// (first non-empty key wins), inside a writable transaction.
/// Step 2: if all keys are empty and `session.should_block()` is `true`,
/// register a waiter and block. If blocking is forbidden (`MULTI`/`EXEC` or a
/// Lua-script context), reply with a null immediately — the same reply a real
/// timeout produces.
///
/// `want_min` is `true` for `BZPOPMIN`, `false` for `BZPOPMAX`. A `timeout` of
/// `0` blocks indefinitely. Returns a single RESP reply. Like the Go
/// implementation, this is NOT a [`QueuedOp`]: the dispatcher must not run
/// pending ops after calling it.
pub fn bzpop_reply(
    session: &Session,
    keys: &[&[u8]],
    timeout: f64,
    want_min: bool,
) -> BoxFuture<'static, RespValue> {
    // Capture everything the blocking future needs as owned data up front:
    // a `&Session` (itself not `Sync`) must not be held across the await.
    let store = session.store();
    let registry = session.registry();
    let can_block = session.should_block();
    let nodes: Vec<(NodeRef, Vec<u8>)> = keys
        .iter()
        .map(|k| (NodeRef::new(session, k), k.to_vec()))
        .collect();

    Box::pin(async move {
        let tx = match store.begin(true).await {
            Ok(tx) => tx,
            Err(e) => return RespValue::Error(format!("ERR {e}").into()),
        };

        let mut immediate: Option<PopResult> = None;
        for (node, raw_key) in &nodes {
            let popped = if want_min {
                pop_one_min(&*tx, node).await
            } else {
                pop_one_max(&*tx, node).await
            };
            match popped {
                Err(e) => {
                    tx.discard();
                    return err_resp(&e);
                }
                Ok(Some(p)) => {
                    immediate = Some(PopResult {
                        key: raw_key.clone(),
                        member: p.member,
                        score: p.score,
                    });
                    break;
                }
                Ok(None) => {}
            }
        }

        if let Some(immediate) = immediate {
            if tx.commit().await.is_err() {
                return RespValue::Error(Bytes::from_static(b"ERR Couldn't commit transaction"));
            }
            return bzpop_reply_value(immediate);
        }

        tx.discard();
        if !can_block {
            return RespValue::BulkString(None);
        }
        let public_keys: Vec<Vec<u8>> = nodes.iter().map(|(n, _)| n.public_key.clone()).collect();
        let duration = if timeout.is_finite() && timeout > 0.0 {
            Some(Duration::from_secs_f64(timeout))
        } else {
            None
        };
        match registry.block(&public_keys, want_min, duration).await {
            Some(result) => bzpop_reply_value(result),
            None => RespValue::BulkString(None),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{test_session, test_store};
    use std::collections::HashSet;

    /// Runs one op through its own transaction and commits if it mutates, then
    /// renders the reply (the unit-test equivalent of Go's `kvs.Update`/`Read`).
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Adds `score member` pairs via `ZADD`.
    async fn zadd_pairs(session: &Session, key: &[u8], pairs: &[(&str, f64)]) {
        let mut args: Vec<Bytes> = Vec::new();
        for (member, score) in pairs {
            args.push(Bytes::from(format!("{score}")));
            args.push(Bytes::from(member.to_string()));
        }
        exec(session, zadd(session, key, &args)).await;
    }

    fn expect_bulk(reply: &RespValue) -> &[u8] {
        match reply {
            RespValue::BulkString(Some(b)) => b,
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    fn expect_array(reply: &RespValue) -> Vec<Vec<u8>> {
        match reply {
            RespValue::Array(Some(v)) => v
                .iter()
                .map(|e| match e {
                    RespValue::BulkString(Some(b)) => b.to_vec(),
                    other => panic!("expected bulk element, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn expect_error(reply: &RespValue) -> Vec<u8> {
        match reply {
            RespValue::Error(b) => b.to_vec(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn score_priority_encoding_roundtrips_include_negatives_and_inf() {
        let vals = [
            0.0,
            -1.5,
            1.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e300,
            -1e-300,
        ];
        for v in vals {
            assert_eq!(decode_score(&encode_score(v)), v, "value {v}");
        }
        // Ascending encoded order matches ascending numeric order.
        let mut encoded: Vec<Vec<u8>> = vals.iter().map(|v| encode_score(*v)).collect();
        let mut sorted = vals.to_vec();
        encoded.sort();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for (e, s) in encoded.iter().zip(&sorted) {
            assert_eq!(decode_score(e), *s);
        }
    }

    #[tokio::test]
    async fn zadd_new_key() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(3)
        );
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", 0, -1, false)).await),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[tokio::test]
    async fn zadd_updates_existing() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        // Update b to a higher score; card unchanged.
        zadd_pairs(&session, b"z", &[("b", 5.0)]).await;
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(2)
        );
        let all = expect_array(&exec(&session, zrange(&session, b"z", 0, -1, true)).await);
        assert_eq!(
            all,
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"5".to_vec()]
        );
    }

    #[tokio::test]
    async fn zadd_nx_does_not_update_existing() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        let args = vec![
            Bytes::from_static(b"NX"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"7"),
            Bytes::from_static(b"c"),
        ];
        assert_eq!(
            exec(&session, zadd(&session, b"z", &args)).await,
            RespValue::Integer(1)
        );
        // a unchanged at 1.
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"1"
        );
        // c added at 7.
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"c")).await),
            b"7"
        );
    }

    #[tokio::test]
    async fn zadd_xx_only_updates_existing() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        let args = vec![
            Bytes::from_static(b"XX"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"7"),
            Bytes::from_static(b"new"),
        ];
        assert_eq!(
            exec(&session, zadd(&session, b"z", &args)).await,
            RespValue::Integer(0)
        );
        // a updated to 10.
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"10"
        );
        // new not added.
        assert!(
            exec(&session, zscore(&session, b"z", b"new")).await == RespValue::BulkString(None)
        );
    }

    #[tokio::test]
    async fn zadd_ch_counts_changed() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        let args = vec![
            Bytes::from_static(b"CH"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"11"),
            Bytes::from_static(b"d"),
        ];
        // Both a (changed) and d (added) -> 2 with CH, vs 1 without.
        assert_eq!(
            exec(&session, zadd(&session, b"z", &args)).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn zadd_gt_lt_take_effect() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 5.0)]).await;
        // GT: only applies when new score strictly greater.
        let gt_lower = vec![
            Bytes::from_static(b"GT"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"a"),
        ];
        assert_eq!(
            exec(&session, zadd(&session, b"z", &gt_lower)).await,
            RespValue::Integer(0)
        );
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"5"
        );
        let gt_higher = vec![
            Bytes::from_static(b"GT"),
            Bytes::from_static(b"6"),
            Bytes::from_static(b"a"),
        ];
        assert_eq!(
            exec(&session, zadd(&session, b"z", &gt_higher)).await,
            RespValue::Integer(0)
        );
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"6"
        );
        // LT: only applies when new score strictly lower.
        let lt_higher = vec![
            Bytes::from_static(b"LT"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"a"),
        ];
        assert_eq!(
            exec(&session, zadd(&session, b"z", &lt_higher)).await,
            RespValue::Integer(0)
        );
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"6"
        );
        let lt_lower = vec![
            Bytes::from_static(b"LT"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"a"),
        ];
        exec(&session, zadd(&session, b"z", &lt_lower)).await;
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"a")).await),
            b"2"
        );
    }

    #[tokio::test]
    async fn zcard_missing_key_is_zero() {
        let session = test_session();
        assert_eq!(
            exec(&session, zcard(&session, b"nope")).await,
            RespValue::Integer(0)
        );
    }

    #[tokio::test]
    async fn zscore_missing_member_is_null() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0)]).await;
        assert!(exec(&session, zscore(&session, b"z", b"zz")).await == RespValue::BulkString(None));
        // Missing key entirely.
        assert!(
            exec(&session, zscore(&session, b"nope", b"a")).await == RespValue::BulkString(None)
        );
    }

    #[tokio::test]
    async fn zrem_removes_members() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        let removed = exec(
            &session,
            zrem(
                &session,
                b"z",
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"c"),
                    Bytes::from_static(b"missing"),
                ],
            ),
        )
        .await;
        assert_eq!(removed, RespValue::Integer(2));
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(1)
        );
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", 0, -1, false)).await),
            vec![b"b".to_vec()]
        );
    }

    #[tokio::test]
    async fn zrem_all_members_deletes_the_set() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        let removed = exec(
            &session,
            zrem(
                &session,
                b"z",
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ),
        )
        .await;
        assert_eq!(removed, RespValue::Integer(2));
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(0)
        );
        // Key should be fully gone.
        assert!(exec(&session, zscore(&session, b"z", b"a")).await == RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn zrange_by_rank_with_negative_indexes() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", 1, 2, false)).await),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", -2, -1, false)).await),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        // Empty range.
        assert!(
            exec(&session, zrange(&session, b"z", 5, 10, false)).await
                == RespValue::Array(Some(vec![]))
        );
    }

    #[tokio::test]
    async fn zrange_with_scores() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", 0, -1, true)).await),
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
        );
    }

    #[tokio::test]
    async fn zrevrange_descending() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            expect_array(&exec(&session, zrevrange(&session, b"z", 0, -1, false)).await),
            vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
        );
    }

    #[tokio::test]
    async fn zrank_and_zrevrank() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            exec(&session, zrank(&session, b"z", b"b")).await,
            RespValue::Integer(1)
        );
        assert_eq!(
            exec(&session, zrevrank(&session, b"z", b"b")).await,
            RespValue::Integer(1)
        );
        assert_eq!(
            exec(&session, zrank(&session, b"z", b"c")).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            exec(&session, zrevrank(&session, b"z", b"c")).await,
            RespValue::Integer(0)
        );
        // Missing member -> null.
        assert!(exec(&session, zrank(&session, b"z", b"zz")).await == RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zcount() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            exec(&session, zcount(&session, b"z", "-inf", "+inf")).await,
            RespValue::Integer(3)
        );
        assert_eq!(
            exec(&session, zcount(&session, b"z", "2", "3")).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            exec(&session, zcount(&session, b"z", "(2", "3")).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn zincrby_adds_and_increments() {
        let session = test_session();
        assert_eq!(
            expect_bulk(&exec(&session, zincrby(&session, b"z", 5.0, b"a")).await),
            b"5"
        );
        assert_eq!(
            expect_bulk(&exec(&session, zincrby(&session, b"z", 3.0, b"a")).await),
            b"8"
        );
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn test_zrangebyscore() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        let reply = exec(
            &session,
            zrangebyscore(&session, b"z", "2", "3", true, None),
        )
        .await;
        assert_eq!(
            expect_array(&reply),
            vec![b"b".to_vec(), b"2".to_vec(), b"c".to_vec(), b"3".to_vec()]
        );
        let reply = exec(
            &session,
            zrangebyscore(&session, b"z", "(2", "+inf", false, Some((0, 1))),
        )
        .await;
        assert_eq!(expect_array(&reply), vec![b"c".to_vec()]);
    }

    #[tokio::test]
    async fn zrangebylex_and_zlexcount() {
        let session = test_session();
        zadd_pairs(
            &session,
            b"z",
            &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)],
        )
        .await;
        assert_eq!(
            expect_array(&exec(&session, zrangebylex(&session, b"z", "-", "+", 0, 0, false)).await),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
        assert_eq!(
            expect_array(
                &exec(
                    &session,
                    zrangebylex(&session, b"z", "[b", "(d", 0, 0, false)
                )
                .await
            ),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            exec(&session, zlexcount(&session, b"z", "-", "+")).await,
            RespValue::Integer(4)
        );
        assert_eq!(
            exec(&session, zlexcount(&session, b"z", "[b", "(d")).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn zremrange_by_rank_score_lex() {
        let session = test_session();
        zadd_pairs(
            &session,
            b"z",
            &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)],
        )
        .await;
        assert_eq!(
            exec(&session, zremrangebyrank(&session, b"z", 1, 2)).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"z", 0, -1, false)).await),
            vec![b"a".to_vec(), b"d".to_vec()]
        );
    }

    #[tokio::test]
    async fn zpopmin_and_max() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        let reply = exec(&session, zpopmin(&session, b"z", 2)).await;
        assert_eq!(
            expect_array(&reply),
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
        );
        let reply = exec(&session, zpopmax(&session, b"z", 1)).await;
        assert_eq!(expect_array(&reply), vec![b"c".to_vec(), b"3".to_vec()]);
        assert_eq!(
            exec(&session, zcard(&session, b"z")).await,
            RespValue::Integer(0)
        );
    }

    #[tokio::test]
    async fn zmscore_returns_null_for_missing() {
        let session = test_session();
        zadd_pairs(&session, b"z", &[("a", 1.0), ("b", 2.0)]).await;
        let reply = exec(
            &session,
            zmscore(
                &session,
                b"z",
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"missing"),
                    Bytes::from_static(b"b"),
                ],
            ),
        )
        .await;
        match reply {
            RespValue::Array(Some(v)) => {
                assert_eq!(v.len(), 3);
                assert_eq!(
                    &v[0],
                    &RespValue::BulkString(Some(Bytes::from_static(b"1")))
                );
                assert_eq!(v[1], RespValue::BulkString(None));
                assert_eq!(
                    &v[2],
                    &RespValue::BulkString(Some(Bytes::from_static(b"2")))
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn zunion_computes_union() {
        let session = test_session();
        zadd_pairs(&session, b"z1", &[("a", 1.0), ("b", 2.0)]).await;
        zadd_pairs(&session, b"z2", &[("b", 3.0), ("c", 4.0)]).await;
        let reply = exec(
            &session,
            zunion(
                &session,
                "sum",
                &[],
                false,
                &[Bytes::from_static(b"z1"), Bytes::from_static(b"z2")],
            ),
        )
        .await;
        let list = expect_array(&reply);
        let members: HashSet<Vec<u8>> = list.into_iter().collect();
        let expected: HashSet<Vec<u8>> = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into();
        assert_eq!(members, expected);
    }

    #[tokio::test]
    async fn zunionstore_writes_destination() {
        let session = test_session();
        zadd_pairs(&session, b"z1", &[("a", 1.0)]).await;
        zadd_pairs(&session, b"z2", &[("b", 2.0)]).await;
        assert_eq!(
            exec(
                &session,
                zunionstore(
                    &session,
                    b"dst",
                    "sum",
                    &[],
                    &[Bytes::from_static(b"z1"), Bytes::from_static(b"z2"),]
                ),
            )
            .await,
            RespValue::Integer(2)
        );
        assert_eq!(
            exec(&session, zcard(&session, b"dst")).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn zinter_computes_intersection() {
        let session = test_session();
        zadd_pairs(&session, b"z1", &[("a", 1.0), ("b", 2.0)]).await;
        zadd_pairs(&session, b"z2", &[("b", 3.0), ("c", 4.0)]).await;
        let reply = exec(
            &session,
            zinter(
                &session,
                "sum",
                &[],
                false,
                &[Bytes::from_static(b"z1"), Bytes::from_static(b"z2")],
            ),
        )
        .await;
        assert_eq!(expect_array(&reply), vec![b"b".to_vec()]);
    }

    #[tokio::test]
    async fn zdiff_computes_difference() {
        let session = test_session();
        zadd_pairs(&session, b"z1", &[("a", 1.0), ("b", 2.0)]).await;
        zadd_pairs(&session, b"z2", &[("b", 3.0)]).await;
        let reply = exec(
            &session,
            zdiff(
                &session,
                false,
                &[Bytes::from_static(b"z1"), Bytes::from_static(b"z2")],
            ),
        )
        .await;
        assert_eq!(expect_array(&reply), vec![b"a".to_vec()]);
    }

    #[tokio::test]
    async fn negative_and_inf_scores() {
        let session = test_session();
        zadd_pairs(
            &session,
            b"z",
            &[
                ("neg", -5.0),
                ("pos", 5.0),
                ("inf", f64::INFINITY),
                ("ninf", f64::NEG_INFINITY),
            ],
        )
        .await;
        let all = expect_array(&exec(&session, zrange(&session, b"z", 0, -1, false)).await);
        assert_eq!(
            all,
            vec![
                b"ninf".to_vec(),
                b"neg".to_vec(),
                b"pos".to_vec(),
                b"inf".to_vec()
            ]
        );
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"inf")).await),
            b"inf"
        );
        assert_eq!(
            expect_bulk(&exec(&session, zscore(&session, b"z", b"ninf")).await),
            b"-inf"
        );
    }

    #[tokio::test]
    async fn zrangestore_writes_range() {
        let session = test_session();
        zadd_pairs(&session, b"src", &[("a", 1.0), ("b", 2.0), ("c", 3.0)]).await;
        assert_eq!(
            exec(&session, zrangestore(&session, b"dst", b"src", 0, 1)).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            expect_array(&exec(&session, zrange(&session, b"dst", 0, -1, false)).await),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[tokio::test]
    async fn wrong_type_on_non_zset_key() {
        let session = test_session();
        // A plain string key set through the raw store, then touched by zset
        // reads/writes, should produce WRONGTYPE.
        crate::strings::set(&session, b"strkey", b"plainstring");
        let _ = exec(
            &session,
            crate::strings::set(&session, b"strkey", b"plainstring"),
        )
        .await;

        for op in [
            zadd(
                &session,
                b"strkey",
                &[Bytes::from_static(b"1"), Bytes::from_static(b"m")],
            ),
            zcard(&session, b"strkey"),
            zrange(&session, b"strkey", 0, -1, false),
            zpopmin(&session, b"strkey", 1),
        ] {
            let err = expect_error(&exec(&session, op).await);
            assert_eq!(
                err,
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            );
        }
    }

    #[tokio::test]
    async fn zadd_option_compat_errors() {
        let session = test_session();
        let incompatible = vec![
            Bytes::from_static(b"NX"),
            Bytes::from_static(b"XX"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"a"),
        ];
        let err = expect_error(&exec(&session, zadd(&session, b"z", &incompatible)).await);
        assert_eq!(
            err,
            b"ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible"
        );
    }

    /// Runs a `BZPOPMIN`/`BZPOPMAX` reply and a subsequent `ZADD` on the same
    /// store+registry, verifying the write claims and serves the blocked
    /// waiter through the real async path (mirrors Go `TestBZPopMinServedBy*`).
    async fn serve_blocked_with_zadd(want_min: bool) {
        let store = test_store();
        let registry = Arc::new(WatchRegistry::new());
        let waiter = Session::new(store.clone(), registry.clone());
        let writer = Session::new(store, registry);

        let keys: Vec<&[u8]> = vec![b"bk"];
        let reply_fut = bzpop_reply(&waiter, &keys, 2.0, want_min);
        let handle = tokio::spawn(reply_fut);

        // Give the waiter time to register on the key before writing.
        tokio::time::sleep(Duration::from_millis(30)).await;

        // ZADD on the same key claims the blocked waiter inside its DbOp.
        let add = zadd(
            &writer,
            b"bk",
            &[Bytes::from_static(b"1"), Bytes::from_static(b"m")],
        );
        let store2 = writer.store();
        let tx = store2.begin(true).await.expect("tx");
        let outcome = add.db_op.run(&*tx).await.expect("ZADD ok");
        tx.commit().await.expect("commit");
        add.wire_op.reply(Ok(outcome));

        let reply = handle.await.expect("task completed");
        match reply {
            RespValue::Array(Some(v)) => {
                assert_eq!(v.len(), 3);
                assert_eq!(
                    &v[0],
                    &RespValue::BulkString(Some(Bytes::from_static(b"bk")))
                );
                assert_eq!(
                    &v[1],
                    &RespValue::BulkString(Some(Bytes::from_static(b"m")))
                );
                assert_eq!(
                    &v[2],
                    &RespValue::BulkString(Some(Bytes::from_static(b"1")))
                );
            }
            other => panic!("expected array, got {other:?}"),
        }

        // The popped member must be gone from the set.
        assert_eq!(
            exec(&waiter, zcard(&waiter, b"bk")).await,
            RespValue::Integer(0)
        );
    }

    #[tokio::test]
    async fn bzpopmin_served_by_zadd() {
        serve_blocked_with_zadd(true).await;
    }

    #[tokio::test]
    async fn bzpopmax_served_by_zadd() {
        serve_blocked_with_zadd(false).await;
    }

    #[tokio::test]
    async fn bzpop_immediate_pop_when_data_exists() {
        let session = test_session();
        zadd_pairs(&session, b"bk", &[("a", 1.0), ("b", 2.0)]).await;
        let key: Vec<&[u8]> = vec![b"bk"];
        let reply = bzpop_reply(&session, &key, 1.0, false).await;
        match reply {
            RespValue::Array(Some(v)) => {
                assert_eq!(v.len(), 3);
                assert_eq!(
                    &v[0],
                    &RespValue::BulkString(Some(Bytes::from_static(b"bk")))
                );
                // BZPOPMAX pops the highest score member.
                assert_eq!(
                    &v[1],
                    &RespValue::BulkString(Some(Bytes::from_static(b"b")))
                );
                assert_eq!(
                    &v[2],
                    &RespValue::BulkString(Some(Bytes::from_static(b"2")))
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(
            exec(&session, zcard(&session, b"bk")).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn bzpop_timeout_returns_null() {
        let session = test_session();
        let key: Vec<&[u8]> = vec![b"nothing"];
        let reply = bzpop_reply(&session, &key, 0.05, true).await;
        assert_eq!(reply, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn bzpop_non_blocking_degrades_to_immediate_in_multi() {
        // In a MULTI/script context should_block() is false, so BZPOP* must not
        // block; it replies null immediately.
        let mut session = test_session();
        session.enter_multi();
        let key: Vec<&[u8]> = vec![b"bk"];
        let reply = bzpop_reply(&session, &key, 0.0, true).await;
        assert_eq!(reply, RespValue::BulkString(None));
    }
}
