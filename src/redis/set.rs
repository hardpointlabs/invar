//! Redis set commands: `SADD`, `SREM`, `SCARD`, `SMEMBERS`, `SISMEMBER`,
//! `SPOP`, `SRANDMEMBER`, `SMOVE`, `SDIFF`, `SINTER`, `SUNION`, `SDIFFSTORE`,
//! `SINTERSTORE` and `SUNIONSTORE`.
//!
//! Port of the Go `redis/set` package. A set is a flat family of member
//! entries under private keys fronted by a small sentinel entry under the
//! public key, matching Go's on-disk layout exactly:
//!
//! * The **sentinel** (public key, metadata `ValueType::Set`) holds the set
//!   cardinality as a 4-byte big-endian uint32. When a set empties (or is
//!   overwritten by the `*STORE` commands) the sentinel is deleted or
//!   rewritten accordingly. See [`read_sentinel`]/[`write_sentinel`].
//! * Each **member** lives under the private key built from
//!   `-<db>:<setname>\x00<member>` (compounded from the captured session
//!   prefix by [`internal_member_key`]); its stored value is the member
//!   itself. Members are enumerated with a prefix iterator over
//!   `-<db>:<setname>\x00` (see [`members_prefix`]).
//!
//! Unlike Go's `set` package (which never validates types), the sentinel is
//! verified to carry `ValueType::Set` metadata before any operation touches
//! its members, so a command aimed at a key holding another value type
//! replies `WRONGTYPE` — matching the Go list/strings ports and real Redis.
//!
//! Every command returns a [`QueuedOp`] with a [`DbOp`] half that reads and
//! mutates the set inside the session-managed transaction and a [`WireOp`]
//! half that renders the result to RESP.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Metadata type byte stamped on every set entry (sentinel and members),
/// matching `ValueType::Set`.
const TYPE_SET: u8 = ValueType::Set as u8;

/// Anything the wire side of a crashed `DbOp` is allowed to claim if the
/// result shape is unexpected (a "can't happen" guard).
fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

/// Monotonic counter mixed into [`rand_index`]/[`rand_perm`] so successive
/// calls in the same nanosecond still differ.
static RAND_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a pseudo-random index in `[0, n)`, or 0 when `n == 0`. A xorshift
/// sequence seeded from the wall clock and a process-local counter gives
/// fresh values across calls without pulling in an external `rand` crate.
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
fn rand_perm(n: usize) -> Vec<usize> {
    let mut perm: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rand_index((i + 1) as u32);
        perm.swap(i, j);
    }
    perm
}

// --- Storage layout ---

/// Builds the private storage key of a member from the captured
/// `-<db>:<setname>` prefix and the member, i.e.
/// `-<db>:<setname>\x00<member>` (mirroring Go's `internalSetKey`, which
/// private-keys the compound `{setname}\x00{member}`).
fn internal_member_key(node_prefix: &[u8], member: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(node_prefix.len() + 1 + member.len());
    key.extend_from_slice(node_prefix);
    key.push(0);
    key.extend_from_slice(member);
    key
}

/// Builds the prefix enumerating every member of a set:
/// `-<db>:<setname>\x00` (mirroring Go's `membersPrefix`).
fn members_prefix(node_prefix: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(node_prefix.len() + 1);
    prefix.extend_from_slice(node_prefix);
    prefix.push(0);
    prefix
}

/// Extracts the member from an internal storage key: everything after the
/// last null separator byte (mirroring Go's `MemberFromInternalKey`).
fn member_from_internal_key(key: &[u8]) -> Vec<u8> {
    match key.iter().rposition(|&b| b == 0) {
        Some(idx) => key[idx + 1..].to_vec(),
        None => Vec::new(),
    }
}

/// Reads the set sentinel, verifying the entry is a set. A missing key maps
/// to [`KvError::KeyNotFound`]; a key holding any other type is
/// [`DbError::WrongType`]; a value too short to hold a count is treated as a
/// missing key.
async fn read_sentinel(tx: &dyn Tx, public_key: &[u8]) -> Result<u32, DbError> {
    let item = tx.get(public_key).await?;
    if item.metadata() != TYPE_SET {
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
async fn set_count(tx: &dyn Tx, public_key: &[u8]) -> Result<Option<u32>, DbError> {
    match read_sentinel(tx, public_key).await {
        Ok(count) => Ok(Some(count)),
        Err(DbError::Kv(KvError::KeyNotFound)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Writes the set sentinel entry with set metadata.
fn write_sentinel(tx: &dyn Tx, public_key: &[u8], count: u32) -> Result<(), DbError> {
    tx.set(Entry::new(public_key.to_vec(), count.to_be_bytes().to_vec()).metadata(TYPE_SET))?;
    Ok(())
}

/// Loads every member of a set into a set, mirroring Go's `loadSetMembers`.
/// An absent key yields an empty set; a key holding another value type is an
/// error.
async fn load_set_members(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &[u8],
) -> Result<HashSet<Vec<u8>>, DbError> {
    if set_count(tx, public_key).await?.is_none() {
        return Ok(HashSet::new());
    }
    let mut it = tx.new_prefix_iterator(&members_prefix(node_prefix)).await?;
    let mut members = HashSet::new();
    while it.next().await {
        if let Some(item) = it.item() {
            members.insert(item.value().to_vec());
        }
    }
    let err = it.err().cloned();
    if let Some(e) = err {
        it.close().await?;
        return Err(DbError::Kv(e));
    }
    it.close().await?;
    Ok(members)
}

/// Deletes every member entry under `node_prefix`, then the sentinel itself
/// (mirroring Go's `ClearPrefixedKeys`). Used by the `*STORE` commands to
/// clear a destination before writing its new result.
async fn clear_set(tx: &dyn Tx, public_key: &[u8], node_prefix: &[u8]) -> Result<(), DbError> {
    let mut it = tx.new_prefix_iterator(&members_prefix(node_prefix)).await?;
    let mut keys = Vec::new();
    while it.next().await {
        if let Some(item) = it.item() {
            keys.push(item.key().to_vec());
        }
    }
    let err = it.err().cloned();
    if let Some(e) = err {
        it.close().await?;
        return Err(DbError::Kv(e));
    }
    it.close().await?;
    for key in keys {
        tx.delete(&key)?;
    }
    tx.delete(public_key)?;
    Ok(())
}

/// Writes `members` as a fresh set under `dest`, replacing anything already
/// there, and returns the number of members stored (mirroring Go's
/// `storeSetResult`). A nil result leaves an empty sentinel in place.
async fn store_set_result(
    tx: &dyn Tx,
    dest_public_key: &[u8],
    dest_node_prefix: &[u8],
    members: &[Vec<u8>],
) -> Result<i64, DbError> {
    clear_set(tx, dest_public_key, dest_node_prefix).await?;
    for member in members {
        tx.set(
            Entry::new(
                internal_member_key(dest_node_prefix, member),
                member.clone(),
            )
            .metadata(TYPE_SET),
        )?;
    }
    write_sentinel(tx, dest_public_key, members.len() as u32)?;
    Ok(members.len() as i64)
}

// --- Command functions ---

/// `SADD key member [member ...]` — adds members, returning how many were new.
pub fn sadd(session: &Session, key: &[u8], members: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SAddOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            members: members.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SREM key member [member ...]` — removes members, returning how many were
/// removed.
pub fn srem(session: &Session, key: &[u8], members: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SRemOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            members: members.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SCARD key` — returns the set cardinality, 0 if missing.
pub fn scard(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SCardOp {
            public_key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SMEMBERS key` — returns all members of the set, empty if missing.
pub fn smembers(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SMembersOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SISMEMBER key member` — reports whether `member` is in the set.
pub fn sismember(session: &Session, key: &[u8], member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SIsMemberOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            member: member.to_vec(),
        }),
        wire_op: Box::new(BoolWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SPOP key` — removes and returns a random member, or nil if missing.
pub fn spop(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SPopOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SRANDMEMBER key [count]` — returns random members without removing them.
/// Positive or zero `count` samples without replacement; a negative count
/// samples with replacement.
pub fn srandmember(session: &Session, key: &[u8], count: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SRandMemberOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            count,
        }),
        wire_op: Box::new(SRandMemberWire { count }),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SMOVE source destination member` — moves a member between sets, replying
/// whether the member existed in `source`.
pub fn smove(session: &Session, src: &[u8], dst: &[u8], member: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SMoveOp {
            src_public_key: session.public_key(src),
            src_node_prefix: session.private_key(src),
            dst_public_key: session.public_key(dst),
            dst_node_prefix: session.private_key(dst),
            member: member.to_vec(),
        }),
        wire_op: Box::new(BoolWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SDIFF key [key ...]` — members present in the first set but none of the
/// others.
pub fn sdiff(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BinarySetOp::new(session, keys, BinarySetOpKind::Diff)),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SINTER key [key ...]` — members present in every given set.
pub fn sinter(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BinarySetOp::new(session, keys, BinarySetOpKind::Inter)),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SUNION key [key ...]` — the union of the given sets.
pub fn sunion(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BinarySetOp::new(session, keys, BinarySetOpKind::Union)),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SDIFFSTORE destination key [key ...]` — stores the set difference,
/// returning the stored cardinality.
pub fn sdiffstore(session: &Session, dest: &[u8], keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StoreOp::new(session, dest, keys, BinarySetOpKind::Diff)),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SINTERSTORE destination key [key ...]` — stores the set intersection,
/// returning the stored cardinality.
pub fn sinterstore(session: &Session, dest: &[u8], keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StoreOp::new(session, dest, keys, BinarySetOpKind::Inter)),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SUNIONSTORE destination key [key ...]` — stores the set union, returning
/// the stored cardinality.
pub fn sunionstore(session: &Session, dest: &[u8], keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StoreOp::new(session, dest, keys, BinarySetOpKind::Union)),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SSCAN key cursor [MATCH pattern] [COUNT count]` — iterates set members.
/// Cursor `0` starts a new scan; returned cursor `0` means iteration complete.
/// Returns `(cursor, [members...])`.
pub fn sscan(
    session: &Session,
    key: &[u8],
    cursor: &[u8],
    count: usize,
    pattern: Option<Vec<u8>>,
) -> QueuedOp {
    let cursor = if cursor == b"0" {
        Vec::new()
    } else {
        cursor.to_vec()
    };
    QueuedOp {
        db_op: Box::new(SScanOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            cursor,
            count,
            pattern,
        }),
        wire_op: Box::new(SScanWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// --- DbOp halves ---

/// A pair of `(public_key, node_prefix)` identifying a set on disk.
#[derive(Clone)]
struct SetRef {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl SetRef {
    fn new(session: &Session, key: &[u8]) -> Self {
        Self {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }
    }
}

struct SAddOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    members: Vec<Vec<u8>>,
}

impl DbOp for SAddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let members = self.members.clone();
        Box::pin(async move {
            let mut count = match read_sentinel(tx, &public_key).await {
                Ok(count) => count,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };
            let mut added = 0i64;
            for member in &members {
                let internal = internal_member_key(&node_prefix, member);
                match tx.get(&internal).await {
                    Ok(_) => {}
                    Err(KvError::KeyNotFound) => {
                        tx.set(Entry::new(internal, member.clone()).metadata(TYPE_SET))?;
                        added += 1;
                        count += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            write_sentinel(tx, &public_key, count)?;
            let result: DbResult = Box::new(added);
            Ok(result)
        })
    }
}

struct SRemOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    members: Vec<Vec<u8>>,
}

impl DbOp for SRemOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let members = self.members.clone();
        Box::pin(async move {
            let mut count = match read_sentinel(tx, &public_key).await {
                Ok(count) => count,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let mut removed = 0i64;
            for member in &members {
                let internal = internal_member_key(&node_prefix, member);
                match tx.get(&internal).await {
                    Ok(_) => {
                        tx.delete(&internal)?;
                        removed += 1;
                        count -= 1;
                    }
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if count == 0 {
                tx.delete(&public_key)?;
            } else {
                write_sentinel(tx, &public_key, count)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

struct SCardOp {
    public_key: Vec<u8>,
}

impl DbOp for SCardOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        Box::pin(async move {
            let count = match read_sentinel(tx, &public_key).await {
                Ok(count) => count,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };
            let result: DbResult = Box::new(count as i64);
            Ok(result)
        })
    }
}

struct SMembersOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl DbOp for SMembersOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        Box::pin(async move {
            let members = load_set_members(tx, &public_key, &node_prefix).await?;
            let result: DbResult = Box::new(members.into_iter().collect::<Vec<Vec<u8>>>());
            Ok(result)
        })
    }
}

struct SIsMemberOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    member: Vec<u8>,
}

impl DbOp for SIsMemberOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let member = self.member.clone();
        Box::pin(async move {
            match set_count(tx, &public_key).await? {
                None => {
                    let result: DbResult = Box::new(false);
                    Ok(result)
                }
                Some(_) => {
                    let present = tx
                        .get(&internal_member_key(&node_prefix, &member))
                        .await
                        .is_ok();
                    let result: DbResult = Box::new(present);
                    Ok(result)
                }
            }
        })
    }
}

struct SPopOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl DbOp for SPopOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        Box::pin(async move {
            let count = match read_sentinel(tx, &public_key).await {
                Ok(count) => count,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let idx = rand_index(count);
            let mut it = match tx.new_prefix_iterator(&members_prefix(&node_prefix)).await {
                Ok(it) => it,
                Err(e) => return Err(e.into()),
            };
            let mut member: Option<Vec<u8>> = None;
            let mut found = 0usize;
            while it.next().await {
                if let Some(item) = it.item() {
                    if found == idx {
                        let internal = item.key().to_vec();
                        member = Some(member_from_internal_key(&internal));
                        if tx.delete(&internal).is_err() {
                            it.close().await?;
                            return Err(DbError::Kv(KvError::Undefined));
                        }
                    }
                    found += 1;
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            if member.is_some() {
                let remaining = count - 1;
                if remaining == 0 {
                    tx.delete(&public_key)?;
                } else {
                    write_sentinel(tx, &public_key, remaining)?;
                }
            }
            let result: DbResult = Box::new(member);
            Ok(result)
        })
    }
}

struct SRandMemberOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    count: i64,
}

impl DbOp for SRandMemberOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let count = self.count;
        Box::pin(async move {
            let all = load_set_members(tx, &public_key, &node_prefix).await?;
            let members: Vec<Vec<u8>> = all.into_iter().collect();

            if count == 0 || members.is_empty() {
                let empty: Vec<Vec<u8>> = Vec::new();
                let result: DbResult = Box::new(empty);
                return Ok(result);
            }

            let result: DbResult = if count > 0 {
                let out = if (count as usize) >= members.len() {
                    members
                } else {
                    let perm = rand_perm(members.len());
                    perm.iter()
                        .take(count as usize)
                        .map(|&i| members[i].clone())
                        .collect()
                };
                Box::new(out)
            } else {
                let n = (-count) as usize;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    let i = rand_index(members.len() as u32);
                    out.push(members[i].clone());
                }
                Box::new(out)
            };
            Ok(result)
        })
    }
}

struct SMoveOp {
    src_public_key: Vec<u8>,
    src_node_prefix: Vec<u8>,
    dst_public_key: Vec<u8>,
    dst_node_prefix: Vec<u8>,
    member: Vec<u8>,
}

impl DbOp for SMoveOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let src_public_key = self.src_public_key.clone();
        let src_node_prefix = self.src_node_prefix.clone();
        let dst_public_key = self.dst_public_key.clone();
        let dst_node_prefix = self.dst_node_prefix.clone();
        let member = self.member.clone();
        let same_set = src_public_key == dst_public_key;
        Box::pin(async move {
            if same_set {
                // Moving within the same set is a no-op; only the member's
                // presence matters (mirroring Go's early return).
                let present = match set_count(tx, &src_public_key).await? {
                    None => false,
                    Some(_) => tx
                        .get(&internal_member_key(&src_node_prefix, &member))
                        .await
                        .is_ok(),
                };
                let result: DbResult = Box::new(present);
                return Ok(result);
            }

            let Some(src_count) = set_count(tx, &src_public_key).await? else {
                let result: DbResult = Box::new(false);
                return Ok(result);
            };
            let src_internal = internal_member_key(&src_node_prefix, &member);
            if tx.get(&src_internal).await.is_err() {
                let result: DbResult = Box::new(false);
                return Ok(result);
            }
            tx.delete(&src_internal)?;

            let src_remaining = src_count - 1;
            if src_remaining == 0 {
                tx.delete(&src_public_key)?;
            } else {
                write_sentinel(tx, &src_public_key, src_remaining)?;
            }

            let dst_internal = internal_member_key(&dst_node_prefix, &member);
            match tx.get(&dst_internal).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    tx.set(Entry::new(dst_internal, member.clone()).metadata(TYPE_SET))?;
                    let dst_count = match set_count(tx, &dst_public_key).await? {
                        None => 1,
                        Some(dst_count) => dst_count + 1,
                    };
                    write_sentinel(tx, &dst_public_key, dst_count)?;
                }
                Err(e) => return Err(e.into()),
            }

            let result: DbResult = Box::new(true);
            Ok(result)
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BinarySetOpKind {
    Diff,
    Inter,
    Union,
}

#[derive(Clone)]
struct BinarySetOp {
    sets: Vec<SetRef>,
    kind: BinarySetOpKind,
}

impl BinarySetOp {
    fn new(session: &Session, keys: &[Bytes], kind: BinarySetOpKind) -> Self {
        Self {
            sets: keys.iter().map(|k| SetRef::new(session, k)).collect(),
            kind,
        }
    }
}

impl DbOp for BinarySetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let sets = self.sets.clone();
        let kind = self.kind;
        Box::pin(async move {
            let mut result: HashSet<Vec<u8>> = HashSet::new();
            if let Some(first) = sets.first() {
                result = load_set_members(tx, &first.public_key, &first.node_prefix).await?;
            }
            match kind {
                BinarySetOpKind::Diff => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        for member in other {
                            result.remove(&member);
                        }
                    }
                }
                BinarySetOpKind::Inter => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        result.retain(|m| other.contains(m));
                    }
                }
                BinarySetOpKind::Union => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        result.extend(other);
                    }
                }
            }
            let out: Vec<Vec<u8>> = result.into_iter().collect();
            let result: DbResult = Box::new(out);
            Ok(result)
        })
    }
}

struct StoreOp {
    dest: SetRef,
    source: BinarySetOp,
}

impl StoreOp {
    fn new(session: &Session, dest: &[u8], keys: &[Bytes], kind: BinarySetOpKind) -> Self {
        Self {
            dest: SetRef::new(session, dest),
            source: BinarySetOp::new(session, keys, kind),
        }
    }
}

impl DbOp for StoreOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let dest_public_key = self.dest.public_key.clone();
        let dest_node_prefix = self.dest.node_prefix.clone();
        let source = self.source.clone();
        Box::pin(async move {
            let kind = source.kind;
            let sets = source.sets;
            let mut result: HashSet<Vec<u8>> = HashSet::new();
            if let Some(first) = sets.first() {
                result = load_set_members(tx, &first.public_key, &first.node_prefix).await?;
            }
            match kind {
                BinarySetOpKind::Diff => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        for member in other {
                            result.remove(&member);
                        }
                    }
                }
                BinarySetOpKind::Inter => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        result.retain(|m| other.contains(m));
                    }
                }
                BinarySetOpKind::Union => {
                    for set in &sets[1..] {
                        let other = load_set_members(tx, &set.public_key, &set.node_prefix).await?;
                        result.extend(other);
                    }
                }
            }
            let members: Vec<Vec<u8>> = result.into_iter().collect();
            let stored =
                store_set_result(tx, &dest_public_key, &dest_node_prefix, &members).await?;
            let result: DbResult = Box::new(stored);
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

/// Replies `1`/`0` for a boolean result.
struct BoolWire;

impl WireOp for BoolWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<bool>() {
                Ok(value) => RespValue::Integer(if *value { 1 } else { 0 }),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a null bulk string for `None`, a bulk string otherwise.
struct NullableBulkWire;

impl WireOp for NullableBulkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<Vec<u8>>>() {
                Ok(boxed) => match *boxed {
                    Some(value) => RespValue::BulkString(Some(Bytes::from(value))),
                    None => RespValue::BulkString(None),
                },
                Err(_) => internal_error(),
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

/// Replies `SRANDMEMBER`: a single bulk string (null when empty) for a count
/// of 1, otherwise an array of bulk strings.
struct SRandMemberWire {
    count: i64,
}

impl WireOp for SRandMemberWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => {
                let Ok(boxed) = res.downcast::<Vec<Vec<u8>>>() else {
                    return internal_error();
                };
                if self.count == 1 {
                    match boxed.first() {
                        Some(member) => RespValue::BulkString(Some(Bytes::copy_from_slice(member))),
                        None => RespValue::BulkString(None),
                    }
                } else {
                    RespValue::Array(Some(
                        boxed
                            .iter()
                            .map(|v| RespValue::BulkString(Some(Bytes::copy_from_slice(v))))
                            .collect(),
                    ))
                }
            }
            Err(e) => err_resp(&e),
        }
    }
}

// --- SSCAN support ---

struct SScanResult {
    cursor: Vec<u8>,
    members: Vec<Vec<u8>>,
}

struct SScanOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    cursor: Vec<u8>,
    count: usize,
    pattern: Option<Vec<u8>>,
}

impl DbOp for SScanOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let cursor = self.cursor.clone();
        let count = self.count.max(1);
        let pattern = self.pattern.clone();
        Box::pin(async move {
            // Verify key exists and is a set.
            match set_count(tx, &public_key).await {
                Ok(None) => {
                    let result: DbResult = Box::new(SScanResult {
                        cursor: Vec::new(),
                        members: Vec::new(),
                    });
                    return Ok(result);
                }
                Ok(Some(_)) => {}
                Err(e) => return Err(e),
            }
            // Load all members into sorted order.
            let mut it = tx.new_prefix_iterator(&members_prefix(&node_prefix)).await?;
            let mut all_members: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    all_members.push(item.value().to_vec());
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            // Determine the starting offset from the cursor.
            let start_offset = if cursor.is_empty() {
                0
            } else {
                // Cursor is the last member returned (base64-encoded index or raw member).
                // For simplicity, use the cursor as an offset string.
                std::str::from_utf8(&cursor)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0)
            };

            // Filter by pattern if given.
            let filtered: Vec<Vec<u8>> = if let Some(ref pat) = pattern {
                let pat_str = std::str::from_utf8(pat).unwrap_or("");
                all_members
                    .into_iter()
                    .filter(|m| {
                        let m_str = std::str::from_utf8(m).unwrap_or("");
                        glob_match(pat_str, m_str)
                    })
                    .collect()
            } else {
                all_members
            };

            // Paginate.
            let end = (start_offset + count).min(filtered.len());
            let page: Vec<Vec<u8>> = filtered[start_offset..end].to_vec();
            let next_cursor = if end < filtered.len() {
                end.to_string().into_bytes()
            } else {
                Vec::new() // cursor 0 = done
            };

            let result: DbResult = Box::new(SScanResult {
                cursor: next_cursor,
                members: page,
            });
            Ok(result)
        })
    }
}

/// Simple glob pattern matcher (mirrors Go's `path.Match`). Supports `*`
/// (any sequence of chars) and `?` (any single char). No character classes.
fn glob_match(pattern: &str, s: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let pat: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = s.chars().collect();
    glob_match_inner(&pat, &s)
}

fn glob_match_inner(pat: &[char], s: &[char]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some('*'), _) => {
            if glob_match_inner(&pat[1..], s) {
                return true;
            }
            if s.is_empty() {
                return false;
            }
            glob_match_inner(pat, &s[1..])
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &s[1..]),
        (Some('['), _) => {
            let close = pat[1..].iter().position(|&c| c == ']');
            if let Some(idx) = close {
                let charset = &pat[2..idx + 1];
                let negate = charset.first() == Some(&'^');
                let chars = if negate { &charset[1..] } else { charset };
                let matched = s.first().map_or(false, |&c| {
                    chars.contains(&c) || chars.windows(2).any(|w| w[0] == '-' && c >= w[0] && c <= w[2])
                });
                if negate {
                    return glob_match_inner(&pat[idx + 2..], &s[1..]);
                }
                return matched && glob_match_inner(&pat[idx + 2..], &s[1..]);
            }
            false
        }
        (Some(&p), Some(&c)) if p == c => glob_match_inner(&pat[1..], &s[1..]),
        _ => false,
    }
}

struct SScanWire;

impl WireOp for SScanWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<SScanResult>() {
                Ok(boxed) => {
                    let cursor = if boxed.cursor.is_empty() {
                        Bytes::from_static(b"0")
                    } else {
                        Bytes::copy_from_slice(&boxed.cursor)
                    };
                    let members = boxed
                        .members
                        .iter()
                        .map(|m| RespValue::BulkString(Some(Bytes::copy_from_slice(m))))
                        .collect();
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(cursor)),
                        RespValue::Array(Some(members)),
                    ]))
                }
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Runs one op through its own transaction and commits if it mutates,
    /// then renders the reply (the unit-test equivalent of Go's
    /// `kvs.Update`/`kvs.Read` calls).
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Loads the members stored under `key` for direct inspection.
    async fn load(session: &Session, key: &[u8]) -> HashSet<Vec<u8>> {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        let public_key = session.public_key(key);
        let node_prefix = session.private_key(key);
        let members = load_set_members(&*tx, &public_key, &node_prefix)
            .await
            .expect("load");
        drop(tx);
        members
    }

    /// The member values, sorted for stable comparison.
    fn sorted(ll: HashSet<Vec<u8>>) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = ll.into_iter().collect();
        out.sort();
        out
    }

    fn expect_int(replies: &[RespValue]) -> i64 {
        match &replies[0] {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_bulk(replies: &[RespValue]) -> Option<Bytes> {
        match &replies[0] {
            RespValue::BulkString(b) => b.clone(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    fn expect_bulk_array(replies: &[RespValue]) -> Vec<Bytes> {
        match &replies[0] {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::BulkString(b) => b.clone().expect("non-null element"),
                    other => panic!("expected bulk string element, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn expect_error(replies: &[RespValue]) -> Bytes {
        match &replies[0] {
            RespValue::Error(e) => e.clone(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// Serves `SADD` without asserting the reply, for setup.
    async fn add(session: &Session, key: &[u8], members: &[&[u8]]) {
        let bytes: Vec<Bytes> = members.iter().map(|m| Bytes::copy_from_slice(m)).collect();
        let replies = exec(session, sadd(session, key, &bytes)).await;
        assert!(matches!(replies, RespValue::Integer(_)));
    }

    #[tokio::test]
    async fn sadd_adds_members() {
        let session = test_session();
        let key = b"myset";

        let replies = exec(
            &session,
            sadd(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 3);
        let members = load(&session, key).await;
        assert_eq!(members.len(), 3);
        assert!(members.contains(b"a".as_slice()));
        assert!(members.contains(b"b".as_slice()));
        assert!(members.contains(b"c".as_slice()));

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 3);
    }

    #[tokio::test]
    async fn sadd_duplicates_are_ignored() {
        let session = test_session();
        let key = b"myset";

        let replies = exec(
            &session,
            sadd(
                &session,
                key,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 2);

        let replies = exec(
            &session,
            sadd(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 1);

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 3);
    }

    #[tokio::test]
    async fn sadd_with_no_members_adds_none() {
        let session = test_session();
        let replies = exec(&session, sadd(&session, b"myset", &[])).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn srem_removes_members() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a", b"b", b"c"]).await;

        let replies = exec(
            &session,
            srem(
                &session,
                key,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"x")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 1);

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 2);
        let members = load(&session, key).await;
        assert_eq!(sorted(members), vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn srem_missing_key_removes_nothing() {
        let session = test_session();
        let replies = exec(
            &session,
            srem(&session, b"nonexistent", &[Bytes::from_static(b"a")]),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn srem_last_member_deletes_the_set() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a"]).await;

        let replies = exec(&session, srem(&session, key, &[Bytes::from_static(b"a")])).await;
        assert_eq!(expect_int(&[replies]), 1);

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 0);
        let members = load(&session, key).await;
        assert!(members.is_empty());
    }

    #[tokio::test]
    async fn scard_missing_key_is_zero() {
        let session = test_session();
        let replies = exec(&session, scard(&session, b"nonexistent")).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn smembers_returns_every_member() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a", b"b", b"c"]).await;

        let replies = exec(&session, smembers(&session, key)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 3);
        let got: HashSet<Vec<u8>> = got.iter().map(|b| b.to_vec()).collect();
        assert_eq!(got, load(&session, key).await);
    }

    #[tokio::test]
    async fn smembers_missing_key_is_empty() {
        let session = test_session();
        let replies = exec(&session, smembers(&session, b"nonexistent")).await;
        assert_eq!(expect_bulk_array(&[replies]).len(), 0);
    }

    #[tokio::test]
    async fn sismember_reports_presence() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a"]).await;

        let replies = exec(&session, sismember(&session, key, b"a")).await;
        assert_eq!(expect_int(&[replies]), 1);
        let replies = exec(&session, sismember(&session, key, b"x")).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn sismember_missing_key_is_absent() {
        let session = test_session();
        let replies = exec(&session, sismember(&session, b"nonexistent", b"a")).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn spop_removes_a_random_member() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a", b"b", b"c"]).await;

        let replies = exec(&session, spop(&session, key)).await;
        let popped = expect_bulk(&[replies]).expect("pop");
        assert!(
            matches!(popped.as_ref(), b"a" | b"b" | b"c"),
            "unexpected member {popped:?}"
        );

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 2);
        let members = load(&session, key).await;
        assert_eq!(members.len(), 2);
    }

    #[tokio::test]
    async fn spop_missing_key_is_null() {
        let session = test_session();
        let replies = exec(&session, spop(&session, b"nonexistent")).await;
        assert_eq!(expect_bulk(&[replies]), None);
    }

    #[tokio::test]
    async fn spop_last_member_empties_the_set() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a"]).await;

        let replies = exec(&session, spop(&session, key)).await;
        assert!(expect_bulk(&[replies]).is_some());

        let members = load(&session, key).await;
        assert!(members.is_empty());
        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn srandmember_samples_without_removing() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a", b"b", b"c"]).await;

        let replies = exec(&session, srandmember(&session, key, 1)).await;
        let got = expect_bulk(&[replies]).expect("member");
        assert!(matches!(got.as_ref(), b"a" | b"b" | b"c"));

        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 3);
    }

    #[tokio::test]
    async fn srandmember_positive_count_without_replacement() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a", b"b", b"c"]).await;

        let replies = exec(&session, srandmember(&session, key, 2)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 2);
        // No duplicate in a without-replacement sample.
        assert_ne!(got[0], got[1]);
    }

    #[tokio::test]
    async fn srandmember_negative_count_with_replacement() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"a"]).await;

        let replies = exec(&session, srandmember(&session, key, -3)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 3);
        for member in &got {
            assert_eq!(member.as_ref(), b"a");
        }
    }

    #[tokio::test]
    async fn srandmember_missing_key_is_empty() {
        let session = test_session();
        let replies = exec(&session, srandmember(&session, b"nonexistent", 1)).await;
        assert_eq!(expect_bulk(&[replies]), None);
    }

    #[tokio::test]
    async fn smove_moves_a_member() {
        let session = test_session();
        let src = b"src";
        let dst = b"dst";
        add(&session, src, &[b"m"]).await;

        let replies = exec(&session, smove(&session, src, dst, b"m")).await;
        assert_eq!(expect_int(&[replies]), 1);

        let replies = exec(&session, scard(&session, src)).await;
        assert_eq!(expect_int(&[replies]), 0);
        let replies = exec(&session, scard(&session, dst)).await;
        assert_eq!(expect_int(&[replies]), 1);
        let dst_members = load(&session, dst).await;
        assert!(dst_members.contains(b"m".as_slice()));
    }

    #[tokio::test]
    async fn smove_missing_member_fails() {
        let session = test_session();
        let replies = exec(&session, smove(&session, b"src", b"dst", b"m")).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn smove_within_same_set_is_a_noop() {
        let session = test_session();
        let key = b"myset";
        add(&session, key, &[b"m"]).await;

        let replies = exec(&session, smove(&session, key, key, b"m")).await;
        assert_eq!(expect_int(&[replies]), 1);
        let replies = exec(&session, smove(&session, key, key, b"x")).await;
        assert_eq!(expect_int(&[replies]), 0);
        let replies = exec(&session, scard(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 1);
    }

    #[tokio::test]
    async fn smove_member_already_in_destination() {
        let session = test_session();
        add(&session, b"src", &[b"m"]).await;
        add(&session, b"dst", &[b"m"]).await;

        let replies = exec(&session, smove(&session, b"src", b"dst", b"m")).await;
        assert_eq!(expect_int(&[replies]), 1);
        let replies = exec(&session, scard(&session, b"src")).await;
        assert_eq!(expect_int(&[replies]), 0);
        let replies = exec(&session, scard(&session, b"dst")).await;
        assert_eq!(expect_int(&[replies]), 1);
    }

    #[tokio::test]
    async fn sdiff_computes_set_difference() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b", b"c"]).await;
        add(&session, b"s2", &[b"b", b"d"]).await;

        let replies = exec(
            &session,
            sdiff(
                &session,
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 2);
        let got: HashSet<Vec<u8>> = got.iter().map(|b| b.to_vec()).collect();
        assert_eq!(got, HashSet::from([b"a".to_vec(), b"c".to_vec()]));
    }

    #[tokio::test]
    async fn sinter_computes_set_intersection() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b", b"c"]).await;
        add(&session, b"s2", &[b"b", b"c", b"d"]).await;

        let replies = exec(
            &session,
            sinter(
                &session,
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 2);
        let got: HashSet<Vec<u8>> = got.iter().map(|b| b.to_vec()).collect();
        assert_eq!(got, HashSet::from([b"b".to_vec(), b"c".to_vec()]));
    }

    #[tokio::test]
    async fn sunion_computes_set_union() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b"]).await;
        add(&session, b"s2", &[b"b", b"c"]).await;

        let replies = exec(
            &session,
            sunion(
                &session,
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        let got: HashSet<Vec<u8>> = expect_bulk_array(&[replies])
            .iter()
            .map(|b| b.to_vec())
            .collect();
        assert_eq!(
            got,
            HashSet::from([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
    }

    #[tokio::test]
    async fn sdiff_store_writes_the_destination() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b", b"c"]).await;
        add(&session, b"s2", &[b"b", b"d"]).await;
        add(&session, b"dest", &[b"x"]).await;

        let replies = exec(
            &session,
            sdiffstore(
                &session,
                b"dest",
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 2);

        let members = load(&session, b"dest").await;
        assert_eq!(sorted(members), vec![b"a".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn sinter_store_writes_the_destination() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b"]).await;
        add(&session, b"s2", &[b"b", b"c"]).await;
        add(&session, b"dest", &[b"x"]).await;

        let replies = exec(
            &session,
            sinterstore(
                &session,
                b"dest",
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 1);

        let members = load(&session, b"dest").await;
        assert_eq!(sorted(members), vec![b"b".to_vec()]);
    }

    #[tokio::test]
    async fn sunion_store_writes_the_destination() {
        let session = test_session();
        add(&session, b"s1", &[b"a", b"b"]).await;
        add(&session, b"s2", &[b"b", b"c"]).await;
        add(&session, b"dest", &[b"x"]).await;

        let replies = exec(
            &session,
            sunionstore(
                &session,
                b"dest",
                &[Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 3);

        let members = load(&session, b"dest").await;
        assert_eq!(
            sorted(members),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[tokio::test]
    async fn wrong_type_operations_error() {
        let session = test_session();
        let key = b"strkey";
        exec(&session, crate::strings::set(&session, key, b"plainstring", None)).await;

        for op in [
            sadd(&session, key, &[Bytes::from_static(b"m")]),
            scard(&session, key),
            spop(&session, key),
            smembers(&session, key),
        ] {
            let replies = exec(&session, op).await;
            assert_eq!(
                expect_error(&[replies]),
                Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            );
        }
    }

    #[test]
    fn internal_key_honours_null_separator() {
        let session = test_session();
        let node_prefix = session.private_key(b"myset");
        let internal = internal_member_key(&node_prefix, b"member");
        let restored = member_from_internal_key(&internal);
        assert_eq!(restored, b"member".to_vec());
        assert!(internal.starts_with(b"-0:myset\x00"));
    }

    #[test]
    fn perm_is_a_bijection() {
        for n in 1..20 {
            let mut perm = rand_perm(n);
            perm.sort_unstable();
            let expected: Vec<usize> = (0..n).collect();
            assert_eq!(perm, expected, "perm must cover [0, {n})");
        }
    }
}
