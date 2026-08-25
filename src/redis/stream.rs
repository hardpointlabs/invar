//! Redis stream commands: `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`,
//! `XDEL`, `XTRIM`, `XSETID`, and `XINFO`.
//!
//! A stream is a family of entry sub-entries under private keys, fronted by a
//! sentinel entry under the public key:
//!
//! * The **sentinel** (public key, metadata `ValueType::Stream`) stores a
//!   fixed 72-byte payload encoding the version counter, entry count, last
//!   generated entry ID, first live entry ID, max-deleted entry ID, and
//!   lifetime entries-added counter. See [`StreamMeta`].
//! * Each **entry** lives under the private key
//!   `-<db>:<keyname>\x00<version>:<ms_padded>-<seq_padded>` where the
//!   version counter starts at 1 and is bumped on `XTRIM` (orphaning old
//!   entries for LSM compaction), and `ms`/`seq` are 20-digit zero-padded
//!   decimals so lexicographic ordering matches numeric ordering. The stored
//!   value is the serialized field-value pairs (see [`encode_entry_value`]).

use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Metadata type byte stamped on the sentinel entry.
const TYPE_STREAM: u8 = ValueType::Stream as u8;

/// Sentinel payload size: 9 × u64 = 72 bytes.
const SENTINEL_SIZE: usize = 9 * 8;

/// Width of the zero-padded decimal representation of a `u64` in entry keys.
const ENTRY_ID_WIDTH: usize = 20;

fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

// ---------------------------------------------------------------------------
// StreamEntryId
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamEntryId {
    ms: u64,
    seq: u64,
}

impl StreamEntryId {
    const ZERO: Self = Self { ms: 0, seq: 0 };

    fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    fn pad(v: u64) -> String {
        format!("{:0width$}", v, width = ENTRY_ID_WIDTH)
    }

    /// The key suffix used in storage: `<ms_padded>-<seq_padded>`.
    fn key_part(&self) -> String {
        format!("{}-{}", Self::pad(self.ms), Self::pad(self.seq))
    }

    fn display(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn from_str_bytes(s: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(s).ok()?;
        let mut parts = s.splitn(2, '-');
        let ms = parts.next()?.parse::<u64>().ok()?;
        let seq = parts.next()?.parse::<u64>().ok()?;
        Some(Self { ms, seq })
    }
}

/// Parses a stream ID that may be a bare `ms-seq` or the special `$` marker.
/// `None` is returned for the bare `$` (meaning "last entry" for XREAD).
pub fn parse_stream_id_arg(s: &[u8]) -> Option<Option<StreamEntryId>> {
    if s == b"$" {
        Some(None)
    } else {
        Some(Some(StreamEntryId::from_str_bytes(s)?))
    }
}

// ---------------------------------------------------------------------------
// StreamMeta  (72-byte sentinel payload)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StreamMeta {
    version: u64,
    count: u64,
    last_id: StreamEntryId,
    first_id: StreamEntryId,
    max_deleted_id: StreamEntryId,
    entries_added: u64,
}

impl StreamMeta {
    fn new() -> Self {
        Self {
            version: 1,
            count: 0,
            last_id: StreamEntryId::ZERO,
            first_id: StreamEntryId::ZERO,
            max_deleted_id: StreamEntryId::ZERO,
            entries_added: 0,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SENTINEL_SIZE);
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.count.to_be_bytes());
        buf.extend_from_slice(&self.last_id.ms.to_be_bytes());
        buf.extend_from_slice(&self.last_id.seq.to_be_bytes());
        buf.extend_from_slice(&self.first_id.ms.to_be_bytes());
        buf.extend_from_slice(&self.first_id.seq.to_be_bytes());
        buf.extend_from_slice(&self.max_deleted_id.ms.to_be_bytes());
        buf.extend_from_slice(&self.max_deleted_id.seq.to_be_bytes());
        buf.extend_from_slice(&self.entries_added.to_be_bytes());
        buf
    }

    fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < SENTINEL_SIZE {
            return None;
        }
        let mut o = 0;
        let mut ru = || -> u64 {
            let v = u64::from_be_bytes(data[o..o + 8].try_into().unwrap());
            o += 8;
            v
        };
        Some(Self {
            version: ru(),
            count: ru(),
            last_id: StreamEntryId::new(ru(), ru()),
            first_id: StreamEntryId::new(ru(), ru()),
            max_deleted_id: StreamEntryId::new(ru(), ru()),
            entries_added: ru(),
        })
    }
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Builds the full private key for an entry:
/// `-<db>:<key>\x00<version>:<ms_padded>-<seq_padded>`
fn entry_key(node_prefix: &[u8], version: u64, id: &StreamEntryId) -> Vec<u8> {
    let version_str = version.to_string();
    let id_part = id.key_part();
    let mut key = Vec::with_capacity(
        node_prefix.len() + 1 + version_str.len() + 1 + id_part.len(),
    );
    key.extend_from_slice(node_prefix);
    key.push(0);
    key.extend_from_slice(version_str.as_bytes());
    key.push(b':');
    key.extend_from_slice(id_part.as_bytes());
    key
}

/// Builds the prefix that enumerates all entries for a given version:
/// `-<db>:<key>\x00<version>:`
fn entry_prefix(node_prefix: &[u8], version: u64) -> Vec<u8> {
    let version_str = version.to_string();
    let mut prefix = Vec::with_capacity(node_prefix.len() + 1 + version_str.len() + 1);
    prefix.extend_from_slice(node_prefix);
    prefix.push(0);
    prefix.extend_from_slice(version_str.as_bytes());
    prefix.push(b':');
    prefix
}

/// Extracts the [`StreamEntryId`] from the key suffix after the version
/// prefix. `prefix_len` is the byte length of the prefix up to and including
/// the `:` separator.
fn entry_id_from_key(key: &[u8], prefix_len: usize) -> Option<StreamEntryId> {
    let suffix = key.get(prefix_len..)?;
    let s = std::str::from_utf8(suffix).ok()?;
    let mut parts = s.splitn(2, '-');
    let ms_s = parts.next()?;
    let seq_s = parts.next()?;
    let ms = ms_s.trim_start_matches('0').parse::<u64>().unwrap_or(0);
    let seq = seq_s.trim_start_matches('0').parse::<u64>().unwrap_or(0);
    Some(StreamEntryId::new(ms, seq))
}

/// Returns the `Bound` for the start of an entry range. If `id` is `None`,
/// the range starts at the beginning of the version prefix.
fn range_start_bound(
    node_prefix: &[u8],
    version: u64,
    id: Option<&StreamEntryId>,
) -> Bound<Vec<u8>> {
    match id {
        Some(id) => Bound::Included(entry_key(node_prefix, version, id)),
        None => Bound::Included(entry_prefix(node_prefix, version)),
    }
}

/// Returns the `Bound` for the end of an entry range. If `id` is `None`,
/// the range extends to the end of the keyspace.
fn range_end_bound(
    node_prefix: &[u8],
    version: u64,
    id: Option<&StreamEntryId>,
) -> Bound<Vec<u8>> {
    match id {
        Some(id) => Bound::Included(entry_key(node_prefix, version, id)),
        None => Bound::Unbounded,
    }
}

// ---------------------------------------------------------------------------
// Sentinel read/write
// ---------------------------------------------------------------------------

async fn read_sentinel(tx: &dyn Tx, public_key: &[u8]) -> Result<StreamMeta, DbError> {
    let item = tx.get(public_key).await?;
    if item.metadata() != TYPE_STREAM {
        return Err(DbError::WrongType);
    }
    StreamMeta::decode(item.value()).ok_or(DbError::Kv(KvError::KeyNotFound))
}

fn write_sentinel(tx: &dyn Tx, public_key: &[u8], meta: &StreamMeta) -> Result<(), DbError> {
    tx.set(Entry::new(public_key.to_vec(), meta.encode()).metadata(TYPE_STREAM))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry value encoding / decoding  (4-byte length-prefixed field-value pairs)
// ---------------------------------------------------------------------------

fn encode_entry_value(fields: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (field, value) in fields {
        buf.extend_from_slice(&(field.len() as u32).to_be_bytes());
        buf.extend_from_slice(field);
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value);
    }
    buf
}

fn decode_entry_value(data: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut pairs = Vec::new();
    let mut o = 0;
    while o + 4 <= data.len() {
        let fl = u32::from_be_bytes(data[o..o + 4].try_into().unwrap()) as usize;
        o += 4;
        if o + fl + 4 > data.len() {
            return None;
        }
        let field = data[o..o + fl].to_vec();
        o += fl;
        let vl = u32::from_be_bytes(data[o..o + 4].try_into().unwrap()) as usize;
        o += 4;
        if o + vl > data.len() {
            return None;
        }
        let value = data[o..o + vl].to_vec();
        o += vl;
        pairs.push((field, value));
    }
    Some(pairs)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn entry_id_resp(id: &StreamEntryId) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(id.display())))
}

/// Renders a single stream entry for RESP: `["<id>", ["f1", "v1", ...]]`.
fn entry_resp(id: &StreamEntryId, fields: &[(Vec<u8>, Vec<u8>)]) -> RespValue {
    let mut items = Vec::with_capacity(2);
    items.push(entry_id_resp(id));
    let flat: Vec<RespValue> = fields
        .iter()
        .flat_map(|(f, v)| {
            [
                RespValue::BulkString(Some(Bytes::copy_from_slice(f))),
                RespValue::BulkString(Some(Bytes::copy_from_slice(v))),
            ]
        })
        .collect();
    items.push(RespValue::Array(Some(flat)));
    RespValue::Array(Some(items))
}

/// Shared range-scan logic used by XRANGE, XREVRANGE, and XREAD.
/// Returns entries in ascending ID order.
async fn scan_entries(
    tx: &dyn Tx,
    node_prefix: &[u8],
    version: u64,
    start: Option<&StreamEntryId>,
    end: Option<&StreamEntryId>,
    count: usize,
) -> Result<Vec<(StreamEntryId, Vec<(Vec<u8>, Vec<u8>)>)>, DbError> {
    let prefix = entry_prefix(node_prefix, version);
    let prefix_len = prefix.len();
    let s = range_start_bound(node_prefix, version, start);
    let e = range_end_bound(node_prefix, version, end);
    let s_ref = s.as_ref().map(|v| v.as_slice());
    let e_ref = e.as_ref().map(|v| v.as_slice());
    let mut it = tx.new_range_iterator(s_ref, e_ref).await?;
    let mut results = Vec::new();
    while it.next().await {
        if let Some(item) = it.item() {
            if !item.key().starts_with(&prefix) {
                break;
            }
            if let Some(id) = entry_id_from_key(item.key(), prefix_len) {
                let fields = decode_entry_value(item.value()).unwrap_or_default();
                results.push((id, fields));
                if results.len() >= count {
                    break;
                }
            }
        }
    }
    let err = it.err().cloned();
    if let Some(e) = err {
        it.close().await?;
        return Err(DbError::Kv(e));
    }
    it.close().await?;
    Ok(results)
}

// ===========================================================================
// Command factory functions
// ===========================================================================

/// `XADD key [NOMKSTREAM] [MAXLEN [~] count | MINID [~] ms-seq] *|id field
/// value [field value ...]`
pub fn xadd(
    session: &Session,
    key: &[u8],
    nomkstream: bool,
    max_len: Option<u64>,
    min_id: Option<StreamEntryId>,
    id: Option<StreamEntryId>,
    field_values: &[Bytes],
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XAddOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            nomkstream,
            max_len,
            min_id,
            explicit_id: id,
            field_values: field_values.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(XAddWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XLEN key`
pub fn xlen(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XLenOp {
            public_key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XRANGE key start end [COUNT count]`
pub fn xrange(
    session: &Session,
    key: &[u8],
    start: Option<StreamEntryId>,
    end: Option<StreamEntryId>,
    count: usize,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XRangeOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            start,
            end,
            count,
            reverse: false,
        }),
        wire_op: Box::new(XRangeWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XREVRANGE key end start [COUNT count]`
pub fn xrevrange(
    session: &Session,
    key: &[u8],
    start: Option<StreamEntryId>,
    end: Option<StreamEntryId>,
    count: usize,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XRangeOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            start,
            end,
            count,
            reverse: true,
        }),
        wire_op: Box::new(XRangeWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XREAD [COUNT count] STREAMS key [key ...] id [id ...]`
pub fn xread(
    session: &Session,
    keys: &[Bytes],
    ids: Vec<Option<StreamEntryId>>,
    count: usize,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XReadOp {
            streams: keys
                .iter()
                .map(|k| XReadStream {
                    public_key: session.public_key(k),
                    node_prefix: session.private_key(k),
                    name: Bytes::copy_from_slice(k),
                })
                .collect(),
            ids,
            count,
        }),
        wire_op: Box::new(XReadWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XDEL key id [id ...]`
pub fn xdel(session: &Session, key: &[u8], ids: &[StreamEntryId]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XDelOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            ids: ids.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XTRIM key MAXLEN [~] count | MINID [~] ms-seq`
pub fn xtrim_maxlen(session: &Session, key: &[u8], max_len: u64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XTrimOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            max_len: Some(max_len),
            min_id: None,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XTRIM key MINID [~] ms-seq`
pub fn xtrim_minid(
    session: &Session,
    key: &[u8],
    min_id: StreamEntryId,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XTrimOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            max_len: None,
            min_id: Some(min_id),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XSETID key last-entry-id`
pub fn xsetid(
    session: &Session,
    key: &[u8],
    last_id: StreamEntryId,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XSetIdOp {
            public_key: session.public_key(key),
            last_id,
        }),
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `XINFO STREAM key`
pub fn xinfo_stream(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(XInfoStreamOp {
            public_key: session.public_key(key),
        }),
        wire_op: Box::new(XInfoStreamWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// ===========================================================================
// DbOp implementations
// ===========================================================================

// --- XADD ---

struct XAddOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    nomkstream: bool,
    max_len: Option<u64>,
    min_id: Option<StreamEntryId>,
    explicit_id: Option<StreamEntryId>,
    field_values: Vec<Vec<u8>>,
}

impl DbOp for XAddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let nomkstream = self.nomkstream;
        let max_len = self.max_len;
        let min_id = self.min_id;
        let explicit_id = self.explicit_id;
        let field_values = self.field_values.clone();
        Box::pin(async move {
            let mut meta = match read_sentinel(tx, &public_key).await {
                Ok(m) => m,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    if nomkstream {
                        return Err(DbError::Redis(
                            "no such key".to_string(),
                        ));
                    }
                    StreamMeta::new()
                }
                Err(e) => return Err(e),
            };

            // Resolve entry ID.
            let id = match explicit_id {
                Some(id) => {
                    if id.ms <= meta.last_id.ms && id.seq <= meta.last_id.seq
                        && !(id.ms == 0 && id.seq == 0 && meta.last_id == StreamEntryId::ZERO)
                    {
                        return Err(DbError::Redis(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                        ));
                    }
                    if id == StreamEntryId::ZERO {
                        return Err(DbError::Redis(
                            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                        ));
                    }
                    id
                }
                None => {
                    let now = current_time_ms();
                    if now > meta.last_id.ms {
                        StreamEntryId::new(now, 0)
                    } else {
                        StreamEntryId::new(meta.last_id.ms, meta.last_id.seq + 1)
                    }
                }
            };

            // Parse field-value pairs.
            if field_values.len() % 2 != 0 {
                return Err(DbError::Redis(
                    "ERR wrong number of arguments for XADD".to_string(),
                ));
            }
            let fields: Vec<(Vec<u8>, Vec<u8>)> = field_values
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();

            // Write entry.
            let key = entry_key(&node_prefix, meta.version, &id);
            let value = encode_entry_value(&fields);
            tx.set(Entry::new(key, value).metadata(TYPE_STREAM))?;

            // Update sentinel.
            let is_first = meta.count == 0;
            meta.count += 1;
            meta.entries_added += 1;
            meta.last_id = id;
            if is_first {
                meta.first_id = id;
            }
            write_sentinel(tx, &public_key, &meta)?;

            // Trim if requested.
            if max_len.is_some() || min_id.is_some() {
                let _ = do_trim(tx, &public_key, &node_prefix, &mut meta, max_len, min_id).await?;
                write_sentinel(tx, &public_key, &meta)?;
            }

            let result: DbResult = Box::new(id.display());
            Ok(result)
        })
    }
}

// --- XLEN ---

struct XLenOp {
    public_key: Vec<u8>,
}

impl DbOp for XLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        Box::pin(async move {
            let count = match read_sentinel(tx, &public_key).await {
                Ok(m) => m.count,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };
            let result: DbResult = Box::new(count as i64);
            Ok(result)
        })
    }
}

// --- XRANGE / XREVRANGE ---

struct XRangeOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    start: Option<StreamEntryId>,
    end: Option<StreamEntryId>,
    count: usize,
    reverse: bool,
}

impl DbOp for XRangeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let start = self.start;
        let end = self.end;
        let count = self.count;
        let reverse = self.reverse;
        Box::pin(async move {
            let meta = match read_sentinel(tx, &public_key).await {
                Ok(m) => m,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(Vec::<(
                        StreamEntryId,
                        Vec<(Vec<u8>, Vec<u8>)>,
                    )>::new());
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let mut entries = scan_entries(
                tx,
                &node_prefix,
                meta.version,
                start.as_ref(),
                end.as_ref(),
                count,
            )
            .await?;
            if reverse {
                entries.reverse();
            }
            let result: DbResult = Box::new(entries);
            Ok(result)
        })
    }
}

// --- XREAD ---

#[derive(Clone)]
struct XReadStream {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    name: Bytes,
}

struct XReadOp {
    streams: Vec<XReadStream>,
    ids: Vec<Option<StreamEntryId>>,
    count: usize,
}

impl DbOp for XReadOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let streams = self.streams.clone();
        let ids = self.ids.clone();
        let count = self.count;
        Box::pin(async move {
            let mut results: Vec<(Bytes, Vec<(StreamEntryId, Vec<(Vec<u8>, Vec<u8>)>)>)> =
                Vec::new();
            for (stream, id_arg) in streams.iter().zip(ids.iter()) {
                let meta = match read_sentinel(tx, &stream.public_key).await {
                    Ok(m) => m,
                    Err(DbError::Kv(KvError::KeyNotFound)) => {
                        results.push((stream.name.clone(), Vec::new()));
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                // Resolve `$` → last_id (exclusive), so we read entries AFTER it.
                let after_id = match id_arg {
                    None => Some(meta.last_id), // $ → after last
                    Some(id) => {
                        // Read entries strictly after `id`.
                        // Shift seq by 1 to make it exclusive.
                        Some(StreamEntryId::new(id.ms, id.seq))
                    }
                };
                // We want entries AFTER after_id. Since scan_entries is
                // inclusive on the start bound, we bump seq by 1 to make the
                // scan start from the next possible entry.
                let scan_start = after_id.map(|a| {
                    // If seq is u64::MAX, bump ms and reset seq. Otherwise
                    // just bump seq.
                    if a.seq < u64::MAX {
                        StreamEntryId::new(a.ms, a.seq + 1)
                    } else {
                        StreamEntryId::new(a.ms + 1, 0)
                    }
                });
                let entries = scan_entries(
                    tx,
                    &stream.node_prefix,
                    meta.version,
                    scan_start.as_ref(),
                    None,
                    count,
                )
                .await?;
                results.push((stream.name.clone(), entries));
            }
            let result: DbResult = Box::new(results);
            Ok(result)
        })
    }
}

// --- XDEL ---

struct XDelOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    ids: Vec<StreamEntryId>,
}

impl DbOp for XDelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let ids = self.ids.clone();
        Box::pin(async move {
            let mut meta = match read_sentinel(tx, &public_key).await {
                Ok(m) => m,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let mut deleted = 0i64;
            for id in &ids {
                let key = entry_key(&node_prefix, meta.version, id);
                match tx.get(&key).await {
                    Ok(_) => {
                        tx.delete(&key)?;
                        deleted += 1;
                        meta.count = meta.count.saturating_sub(1);
                        if *id > meta.max_deleted_id {
                            meta.max_deleted_id = *id;
                        }
                    }
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            // Recompute first_id / last_id if the stream is now empty.
            if meta.count == 0 {
                meta.first_id = StreamEntryId::ZERO;
                meta.last_id = StreamEntryId::ZERO;
            }
            write_sentinel(tx, &public_key, &meta)?;
            let result: DbResult = Box::new(deleted);
            Ok(result)
        })
    }
}

// --- XTRIM ---

struct XTrimOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    max_len: Option<u64>,
    min_id: Option<StreamEntryId>,
}

impl DbOp for XTrimOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let max_len = self.max_len;
        let min_id = self.min_id;
        Box::pin(async move {
            let mut meta = match read_sentinel(tx, &public_key).await {
                Ok(m) => m,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let before = meta.count;
            do_trim(tx, &public_key, &node_prefix, &mut meta, max_len, min_id).await?;
            write_sentinel(tx, &public_key, &meta)?;
            let result: DbResult = Box::new((before - meta.count) as i64);
            Ok(result)
        })
    }
}

/// Core trim logic shared by XADD's inline trim and XTRIM's DbOp.
/// Updates `meta` in-place and writes/deletes entries in the store.
async fn do_trim(
    tx: &dyn Tx,
    _public_key: &[u8],
    node_prefix: &[u8],
    meta: &mut StreamMeta,
    max_len: Option<u64>,
    min_id: Option<StreamEntryId>,
) -> Result<(), DbError> {
    if meta.count == 0 {
        return Ok(());
    }

    if let Some(limit) = max_len {
        if meta.count <= limit {
            return Ok(());
        }
        // Delete the oldest (count - limit) entries.
        let to_remove = meta.count - limit;
        let prefix = entry_prefix(node_prefix, meta.version);
        let prefix_len = prefix.len();
        let prefix_ref: &[u8] = &prefix;
        let mut it = tx.new_range_iterator(Bound::Included(prefix_ref), Bound::Unbounded).await?;
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
        let mut first_kept: Option<StreamEntryId> = None;
        let mut seen = 0u64;
        while it.next().await {
            if let Some(item) = it.item() {
                if !item.key().starts_with(&prefix) {
                    break;
                }
                seen += 1;
                if seen <= to_remove {
                    keys_to_delete.push(item.key().to_vec());
                } else if first_kept.is_none() {
                    first_kept = entry_id_from_key(item.key(), prefix_len);
                }
            }
        }
        let err = it.err().cloned();
        if let Some(e) = err {
            it.close().await?;
            return Err(DbError::Kv(e));
        }
        it.close().await?;
        for key in &keys_to_delete {
            tx.delete(key)?;
        }
        meta.count = meta.count.saturating_sub(to_remove);
        if let Some(id) = first_kept {
            meta.first_id = id;
        }
        if meta.count == 0 {
            meta.first_id = StreamEntryId::ZERO;
            meta.last_id = StreamEntryId::ZERO;
        }
    }

    if let Some(min) = min_id {
        if meta.count == 0 {
            return Ok(());
        }
        // Delete all entries with ID < min_id.
        let prefix = entry_prefix(node_prefix, meta.version);
        let min_key = entry_key(node_prefix, meta.version, &min);
        let prefix_ref: &[u8] = &prefix;
        let min_key_ref: &[u8] = &min_key;
        let mut it = tx
            .new_range_iterator(Bound::Included(prefix_ref), Bound::Excluded(min_key_ref))
            .await?;
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
        while it.next().await {
            if let Some(item) = it.item() {
                keys_to_delete.push(item.key().to_vec());
            }
        }
        let err = it.err().cloned();
        if let Some(e) = err {
            it.close().await?;
            return Err(DbError::Kv(e));
        }
        it.close().await?;
        let removed = keys_to_delete.len() as u64;
        for key in &keys_to_delete {
            tx.delete(key)?;
        }
        meta.count = meta.count.saturating_sub(removed);
        // Recompute first_id from the scan range's exclusive end if needed.
        if meta.count == 0 {
            meta.first_id = StreamEntryId::ZERO;
            meta.last_id = StreamEntryId::ZERO;
        } else if removed > 0 {
            // The first live entry is now the first one >= min_id.
            meta.first_id = min;
        }
        if min > meta.max_deleted_id {
            meta.max_deleted_id = min;
        }
    }

    Ok(())
}

// --- XSETID ---

struct XSetIdOp {
    public_key: Vec<u8>,
    last_id: StreamEntryId,
}

impl DbOp for XSetIdOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let last_id = self.last_id;
        Box::pin(async move {
            let mut meta = match read_sentinel(tx, &public_key).await {
                Ok(m) => m,
                Err(DbError::Kv(KvError::KeyNotFound)) => StreamMeta::new(),
                Err(e) => return Err(e),
            };
            meta.last_id = last_id;
            write_sentinel(tx, &public_key, &meta)?;
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

// --- XINFO STREAM ---

struct XInfoStreamOp {
    public_key: Vec<u8>,
}

impl DbOp for XInfoStreamOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        Box::pin(async move {
            let meta = read_sentinel(tx, &public_key).await?;
            let result: DbResult = Box::new(meta);
            Ok(result)
        })
    }
}

// ===========================================================================
// WireOp implementations
// ===========================================================================

struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(v) => RespValue::Integer(*v),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

struct OkWire;

impl WireOp for OkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => err_resp(&e),
        }
    }
}

struct XAddWire;

impl WireOp for XAddWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<String>() {
                Ok(id) => RespValue::BulkString(Some(Bytes::from(id.as_str().to_string()))),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

struct XRangeWire;

impl WireOp for XRangeWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => {
                let Ok(entries) = res.downcast::<Vec<(StreamEntryId, Vec<(Vec<u8>, Vec<u8>)>)>>() else {
                    return internal_error();
                };
                let items: Vec<RespValue> = entries
                    .iter()
                    .map(|(id, fields)| entry_resp(id, fields))
                    .collect();
                RespValue::Array(Some(items))
            }
            Err(e) => err_resp(&e),
        }
    }
}

struct XReadWire;

impl WireOp for XReadWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => {
                let Ok(streams) = res.downcast::<Vec<(
                    Bytes,
                    Vec<(StreamEntryId, Vec<(Vec<u8>, Vec<u8>)>)>,
                )>>() else {
                    return internal_error();
                };
                // nil when no stream has entries.
                let any_entries = streams.iter().any(|(_, e)| !e.is_empty());
                if !any_entries {
                    return RespValue::Array(None);
                }
                let items: Vec<RespValue> = streams
                    .iter()
                    .map(|(name, entries)| {
                        let entry_items: Vec<RespValue> = entries
                            .iter()
                            .map(|(id, fields)| entry_resp(id, fields))
                            .collect();
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(name.clone())),
                            RespValue::Array(Some(entry_items)),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(items))
            }
            Err(e) => err_resp(&e),
        }
    }
}

struct XInfoStreamWire;

impl WireOp for XInfoStreamWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => {
                let Ok(meta) = res.downcast::<StreamMeta>() else {
                    return internal_error();
                };
                let items = vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"length"))),
                    RespValue::Integer(meta.count as i64),
                    RespValue::BulkString(Some(Bytes::from_static(b"radix-tree-keys"))),
                    RespValue::Integer(0),
                    RespValue::BulkString(Some(Bytes::from_static(b"radix-tree-nodes"))),
                    RespValue::Integer(0),
                    RespValue::BulkString(Some(Bytes::from_static(b"groups"))),
                    RespValue::Integer(0),
                    RespValue::BulkString(Some(Bytes::from_static(b"last-generated-id"))),
                    RespValue::BulkString(Some(Bytes::from(meta.last_id.display()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"max-deleted-entry-id"))),
                    RespValue::BulkString(Some(Bytes::from(meta.max_deleted_id.display()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"entries-added"))),
                    RespValue::Integer(meta.entries_added as i64),
                    RespValue::BulkString(Some(Bytes::from_static(b"recorded-first-entry-id"))),
                    RespValue::BulkString(Some(Bytes::from(meta.first_id.display()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"first-entry-id"))),
                    RespValue::BulkString(Some(Bytes::from(meta.first_id.display()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"last-accessed-entry-id"))),
                    RespValue::BulkString(Some(Bytes::from(meta.last_id.display()))),
                ];
                RespValue::Array(Some(items))
            }
            Err(e) => err_resp(&e),
        }
    }
}

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    fn expect_int(reply: &RespValue) -> i64 {
        match reply {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_bulk(reply: &RespValue) -> Option<Bytes> {
        match reply {
            RespValue::BulkString(b) => b.clone(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    fn expect_array(reply: &RespValue) -> Vec<RespValue> {
        match reply {
            RespValue::Array(Some(items)) => items.clone(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn expect_error(reply: &RespValue) -> Bytes {
        match reply {
            RespValue::Error(e) => e.clone(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    fn entry_id(reply: &RespValue) -> String {
        match reply {
            RespValue::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
            RespValue::Array(Some(items)) if !items.is_empty() => match &items[0] {
                RespValue::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                other => panic!("expected bulk string for entry id, got {other:?}"),
            },
            other => panic!("expected bulk string or array, got {other:?}"),
        }
    }

    // --- XADD ---

    #[tokio::test]
    async fn xadd_auto_id() {
        let session = test_session();
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                None,
                &[
                    Bytes::from_static(b"f1"),
                    Bytes::from_static(b"v1"),
                ],
            ),
        )
        .await;
        let id_str = expect_bulk(&r).expect("id");
        let id = StreamEntryId::from_str_bytes(&id_str).expect("parse");
        assert!(id.ms > 0);
        assert_eq!(id.seq, 0);
    }

    #[tokio::test]
    async fn xadd_explicit_id() {
        let session = test_session();
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                Some(StreamEntryId::new(100, 1)),
                &[
                    Bytes::from_static(b"f"),
                    Bytes::from_static(b"v"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_bulk(&r).unwrap().as_ref(), b"100-1");
    }

    #[tokio::test]
    async fn xadd_duplicate_id_errors() {
        let session = test_session();
        let id = Some(StreamEntryId::new(100, 0));
        let fv = &[
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
        ];
        exec(&session, xadd(&session, b"s", false, None, None, id, fv)).await;
        let r = exec(&session, xadd(&session, b"s", false, None, None, id, fv)).await;
        let err = expect_error(&r);
        assert!(
            err.starts_with(b"ERR"),
            "expected ERR, got {err:?}"
        );
    }

    #[tokio::test]
    async fn xadd_zero_id_errors() {
        let session = test_session();
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                Some(StreamEntryId::new(0, 0)),
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v")],
            ),
        )
        .await;
        let err = expect_error(&r);
        assert!(err.starts_with(b"ERR"));
    }

    #[tokio::test]
    async fn xadd_nomkstream_missing_key() {
        let session = test_session();
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                true,
                None,
                None,
                Some(StreamEntryId::new(1, 0)),
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v")],
            ),
        )
        .await;
        let err = expect_error(&r);
        assert!(err.starts_with(b"ERR no such key"));
    }

    #[tokio::test]
    async fn xadd_nomkstream_existing_key() {
        let session = test_session();
        exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                Some(StreamEntryId::new(1, 0)),
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v")],
            ),
        )
        .await;
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                true,
                None,
                None,
                Some(StreamEntryId::new(2, 0)),
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v2")],
            ),
        )
        .await;
        assert_eq!(expect_bulk(&r).unwrap().as_ref(), b"2-0");
    }

    // --- XLEN ---

    #[tokio::test]
    async fn xlen_missing_key() {
        let session = test_session();
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 0);
    }

    #[tokio::test]
    async fn xlen_after_adds() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(
            &session,
            xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv),
        )
        .await;
        exec(
            &session,
            xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv),
        )
        .await;
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 2);
    }

    // --- XRANGE ---

    #[tokio::test]
    async fn xrange_missing_key() {
        let session = test_session();
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        assert_eq!(expect_array(&r).len(), 0);
    }

    #[tokio::test]
    async fn xrange_returns_all_entries() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 3);
        assert_eq!(entry_id(&items[0]), "1-0");
        assert_eq!(entry_id(&items[2]), "3-0");
    }

    #[tokio::test]
    async fn xrange_with_count() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(&session, xrange(&session, b"s", None, None, 2)).await;
        assert_eq!(expect_array(&r).len(), 2);
    }

    #[tokio::test]
    async fn xrange_filtered_range() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(
            &session,
            xrange(
                &session,
                b"s",
                Some(StreamEntryId::new(2, 0)),
                Some(StreamEntryId::new(2, 0)),
                100,
            ),
        )
        .await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 1);
        assert_eq!(entry_id(&items[0]), "2-0");
    }

    // --- XREVRANGE ---

    #[tokio::test]
    async fn xrevrange_returns_reversed() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(
            &session,
            xrevrange(
                &session,
                b"s",
                Some(StreamEntryId::new(1, 0)),
                Some(StreamEntryId::new(3, 0)),
                100,
            ),
        )
        .await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 3);
        assert_eq!(entry_id(&items[0]), "3-0");
        assert_eq!(entry_id(&items[2]), "1-0");
    }

    // --- XDEL ---

    #[tokio::test]
    async fn xdel_removes_entries() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(
            &session,
            xdel(&session, b"s", &[StreamEntryId::new(2, 0)]),
        )
        .await;
        assert_eq!(expect_int(&r), 1);
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 2);
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 2);
        assert_eq!(entry_id(&items[0]), "1-0");
        assert_eq!(entry_id(&items[1]), "3-0");
    }

    #[tokio::test]
    async fn xdel_missing_entry() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        let r = exec(
            &session,
            xdel(&session, b"s", &[StreamEntryId::new(99, 0)]),
        )
        .await;
        assert_eq!(expect_int(&r), 0);
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 1);
    }

    #[tokio::test]
    async fn xdel_all_entries_removes_stream() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xdel(&session, b"s", &[StreamEntryId::new(1, 0)])).await;
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 0);
    }

    // --- XTRIM ---

    #[tokio::test]
    async fn xtrim_maxlen_basic() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        for i in 1..=5u64 {
            exec(
                &session,
                xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(i, 0)), fv),
            )
            .await;
        }
        let r = exec(&session, xtrim_maxlen(&session, b"s", 3)).await;
        assert_eq!(expect_int(&r), 2);
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 3);
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 3);
        assert_eq!(entry_id(&items[0]), "3-0");
        assert_eq!(entry_id(&items[2]), "5-0");
    }

    #[tokio::test]
    async fn xtrim_minid_basic() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(
            &session,
            xtrim_minid(&session, b"s", StreamEntryId::new(2, 0)),
        )
        .await;
        assert_eq!(expect_int(&r), 1);
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 2);
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 2);
        assert_eq!(entry_id(&items[0]), "2-0");
        assert_eq!(entry_id(&items[1]), "3-0");
    }

    #[tokio::test]
    async fn xtrim_maxlen_in_xadd() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        // Fill to 5 entries.
        for i in 1..=5u64 {
            exec(
                &session,
                xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(i, 0)), fv),
            )
            .await;
        }
        // XADD with MAXLEN 3 should trim old entries.
        exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                Some(3),
                None,
                Some(StreamEntryId::new(6, 0)),
                fv,
            ),
        )
        .await;
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 3);
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(entry_id(&items[0]), "4-0");
        assert_eq!(entry_id(&items[2]), "6-0");
    }

    // --- XSETID ---

    #[tokio::test]
    async fn xsetid_creates_stream() {
        let session = test_session();
        let r = exec(
            &session,
            xsetid(&session, b"s", StreamEntryId::new(100, 5)),
        )
        .await;
        assert_eq!(
            r,
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(expect_int(&exec(&session, xlen(&session, b"s")).await), 0);
        // XADD after XSETID should use the set ID as the base.
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                None,
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v")],
            ),
        )
        .await;
        let id_str = expect_bulk(&r).unwrap();
        let id = StreamEntryId::from_str_bytes(&id_str).unwrap();
        assert!(id.ms >= 100);
    }

    #[tokio::test]
    async fn xsetid_updates_existing() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(
            &session,
            xsetid(&session, b"s", StreamEntryId::new(500, 0)),
        )
        .await;
        // Auto-ID should now be after 500-0.
        let r = exec(
            &session,
            xadd(
                &session,
                b"s",
                false,
                None,
                None,
                None,
                &[Bytes::from_static(b"f"), Bytes::from_static(b"v")],
            ),
        )
        .await;
        let id_str = expect_bulk(&r).unwrap();
        let id = StreamEntryId::from_str_bytes(&id_str).unwrap();
        assert!(id.ms >= 500);
    }

    // --- XINFO STREAM ---

    #[tokio::test]
    async fn xinfo_stream_missing_key() {
        let session = test_session();
        let r = exec(&session, xinfo_stream(&session, b"s")).await;
        expect_error(&r); // should error
    }

    #[tokio::test]
    async fn xinfo_stream_with_entries() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(10, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(20, 1)), fv)).await;
        let r = exec(&session, xinfo_stream(&session, b"s")).await;
        let items = expect_array(&r);
        // Verify some key-value pairs.
        // length
        assert_eq!(
            items[0],
            RespValue::BulkString(Some(Bytes::from_static(b"length")))
        );
        assert_eq!(items[1], RespValue::Integer(2));
        // last-generated-id
        assert_eq!(
            items[8],
            RespValue::BulkString(Some(Bytes::from_static(b"last-generated-id")))
        );
        assert_eq!(
            items[9],
            RespValue::BulkString(Some(Bytes::from_static(b"20-1")))
        );
    }

    // --- Entry value encoding round-trip ---

    #[test]
    fn encode_decode_roundtrip() {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"name".to_vec(), b"Alice".to_vec()),
            (b"age".to_vec(), b"30".to_vec()),
            (b"empty".to_vec(), b"".to_vec()),
        ];
        let encoded = encode_entry_value(&pairs);
        let decoded = decode_entry_value(&encoded).expect("decode");
        assert_eq!(pairs, decoded);
    }

    #[test]
    fn encode_empty() {
        let encoded = encode_entry_value(&[]);
        let decoded = decode_entry_value(&encoded).expect("decode");
        assert!(decoded.is_empty());
    }

    // --- StreamMeta round-trip ---

    #[test]
    fn meta_encode_decode_roundtrip() {
        let meta = StreamMeta {
            version: 42,
            count: 100,
            last_id: StreamEntryId::new(1692590000, 5),
            first_id: StreamEntryId::new(1692590000, 0),
            max_deleted_id: StreamEntryId::new(1692589999, 99),
            entries_added: 200,
        };
        let encoded = meta.encode();
        assert_eq!(encoded.len(), SENTINEL_SIZE);
        let decoded = StreamMeta::decode(&encoded).expect("decode");
        assert_eq!(meta.version, decoded.version);
        assert_eq!(meta.count, decoded.count);
        assert_eq!(meta.last_id, decoded.last_id);
        assert_eq!(meta.first_id, decoded.first_id);
        assert_eq!(meta.max_deleted_id, decoded.max_deleted_id);
        assert_eq!(meta.entries_added, decoded.entries_added);
    }

    // --- WRONGTYPE ---

    #[tokio::test]
    async fn wrong_type_xlen() {
        let session = test_session();
        exec(&session, crate::strings::set(&session, b"key", b"val", None)).await;
        let r = exec(&session, xlen(&session, b"key")).await;
        assert_eq!(
            expect_error(&r),
            Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[tokio::test]
    async fn wrong_type_xrange() {
        let session = test_session();
        exec(&session, crate::strings::set(&session, b"key", b"val", None)).await;
        let r = exec(&session, xrange(&session, b"key", None, None, 10)).await;
        assert_eq!(
            expect_error(&r),
            Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    // --- XREAD ---

    #[tokio::test]
    async fn xread_missing_stream() {
        let session = test_session();
        let r = exec(
            &session,
            xread(
                &session,
                &[Bytes::from_static(b"s")],
                vec![Some(StreamEntryId::new(0, 0))],
                10,
            ),
        )
        .await;
        assert_eq!(r, RespValue::Array(None));
    }

    #[tokio::test]
    async fn xread_after_specific_id() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(2, 0)), fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(3, 0)), fv)).await;
        let r = exec(
            &session,
            xread(
                &session,
                &[Bytes::from_static(b"s")],
                vec![Some(StreamEntryId::new(1, 0))],
                10,
            ),
        )
        .await;
        // Should return entries AFTER 1-0, i.e. 2-0 and 3-0.
        match &r {
            RespValue::Array(Some(streams)) => {
                assert_eq!(streams.len(), 1);
                match &streams[0] {
                    RespValue::Array(Some(items)) => {
                        assert_eq!(items.len(), 2);
                    }
                    other => panic!("expected inner array, got {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xread_dollar_id() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        exec(&session, xadd(&session, b"s", false, None, None, Some(StreamEntryId::new(1, 0)), fv)).await;
        // $ means "after last entry" → should return nil.
        let r = exec(
            &session,
            xread(
                &session,
                &[Bytes::from_static(b"s")],
                vec![None], // None = $
                10,
            ),
        )
        .await;
        assert_eq!(r, RespValue::Array(None));
    }

    // --- Multiple entries with sequence numbers ---

    #[tokio::test]
    async fn xadd_multiple_same_ms() {
        let session = test_session();
        let fv = &[Bytes::from_static(b"f"), Bytes::from_static(b"v")];
        let id1 = Some(StreamEntryId::new(100, 0));
        let id2 = Some(StreamEntryId::new(100, 1));
        let id3 = Some(StreamEntryId::new(100, 5));
        exec(&session, xadd(&session, b"s", false, None, None, id1, fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, id2, fv)).await;
        exec(&session, xadd(&session, b"s", false, None, None, id3, fv)).await;
        let r = exec(&session, xrange(&session, b"s", None, None, 100)).await;
        let items = expect_array(&r);
        assert_eq!(items.len(), 3);
        assert_eq!(entry_id(&items[0]), "100-0");
        assert_eq!(entry_id(&items[1]), "100-1");
        assert_eq!(entry_id(&items[2]), "100-5");
    }
}
