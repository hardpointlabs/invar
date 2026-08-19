//! Redis hash commands: `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`,
//! `HKEYS`, `HVALS`, `HGETALL`, `HMGET`, `HMSET`, `HINCRBY`,
//! `HINCRBYFLOAT`, `HRANDFIELD`, `HSETNX`, `HSTRLEN` and `HSCAN`.
//!
//! Port of the Go `redis/hash` package. A hash is a flat family of field
//! entries under private keys fronted by a small sentinel entry under the
//! public key, matching Go's on-disk layout exactly:
//!
//! * The **sentinel** (public key, metadata `ValueType::Hash`) holds the
//!   field count as a 4-byte big-endian uint32. When a hash becomes empty
//!   (all fields deleted) the sentinel is removed. See
//!   [`read_sentinel`]/[`write_sentinel`].
//! * Each **field** lives under the private key
//!   `-<db>:<hashname>\x00<fieldname>` (compounded from the captured session
//!   prefix by [`field_key`]); its stored value is the field value. Fields
//!   are enumerated with a prefix iterator over `-<db>:<hashname>\x00`
//!   (see [`fields_prefix`]).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Metadata type byte stamped on every hash entry (sentinel and fields).
const TYPE_HASH: u8 = ValueType::Hash as u8;

fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

// --- Storage layout helpers ---

/// Builds the private key for a specific field:
/// `-<db>:<hashname>\x00<fieldname>` (mirroring Go's `internalKey`).
fn field_key(node_prefix: &[u8], field: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(node_prefix.len() + 1 + field.len());
    key.extend_from_slice(node_prefix);
    key.push(0);
    key.extend_from_slice(field);
    key
}

/// Builds the prefix for iterating every field of a hash:
/// `-<db>:<hashname>\x00` (mirroring Go's `fieldPrefix`).
fn fields_prefix(node_prefix: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(node_prefix.len() + 1);
    prefix.extend_from_slice(node_prefix);
    prefix.push(0);
    prefix
}

/// Extracts the field name from an internal storage key: everything after
/// the last null separator byte (mirroring Go's `MemberFromInternalKey`).
fn field_from_internal_key(key: &[u8]) -> Vec<u8> {
    match key.iter().rposition(|&b| b == 0) {
        Some(idx) => key[idx + 1..].to_vec(),
        None => Vec::new(),
    }
}

/// Reads the hash sentinel, verifying the entry is a hash. A missing key
/// maps to `Err(KvError::KeyNotFound)`; a key holding another type is
/// `Err(DbError::WrongType)`.
async fn read_sentinel(tx: &dyn Tx, public_key: &[u8]) -> Result<u32, DbError> {
    let item = tx.get(public_key).await?;
    if item.metadata() != TYPE_HASH {
        return Err(DbError::WrongType);
    }
    let val = item.value();
    if val.len() < 4 {
        return Err(DbError::Kv(KvError::KeyNotFound));
    }
    Ok(u32::from_be_bytes(val[0..4].try_into().expect("slice in range")))
}

/// Writes the hash sentinel entry with hash metadata.
fn write_sentinel(tx: &dyn Tx, public_key: &[u8], count: u32) -> Result<(), DbError> {
    tx.set(Entry::new(public_key.to_vec(), count.to_be_bytes().to_vec()).metadata(TYPE_HASH))?;
    Ok(())
}

/// Formats a float the way Go's `redis/common.FormatFloat` does.
fn format_float(v: f64) -> String {
    if v > 0.0 && v.is_infinite() {
        return "inf".to_string();
    }
    if v < 0.0 && v.is_infinite() {
        return "-inf".to_string();
    }
    format!("{v}")
}

// --- Random helpers (same xorshift pattern as set.rs) ---

static RAND_COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn rand_perm(n: usize) -> Vec<usize> {
    let mut perm: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rand_index((i + 1) as u32);
        perm.swap(i, j);
    }
    perm
}

// --- Command factory functions ---

/// `HSET key field value [field value ...]` — sets fields, returning the
/// number of new fields added.
pub fn hset(session: &Session, key: &[u8], field_values: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HSetOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            field_values: field_values.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HSETNX key field value` — sets a field only if it does not already exist.
/// Returns 1 if set, 0 if the field was already present.
pub fn hsetnx(session: &Session, key: &[u8], field: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HSetNxOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            field: field.to_vec(),
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HGET key field` — returns the value of a field, or null if missing.
pub fn hget(session: &Session, key: &[u8], field: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HGetOp {
            node_prefix: session.private_key(key),
            field: field.to_vec(),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HMGET key field [field ...]` — returns values for multiple fields; missing
/// fields map to null bulk strings.
pub fn hmget(session: &Session, key: &[u8], fields: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HMGetOp {
            node_prefix: session.private_key(key),
            fields: fields.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(NullableArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HDEL key field [field ...]` — removes fields, returning how many were
/// actually present and removed.
pub fn hdel(session: &Session, key: &[u8], fields: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HDelOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            fields: fields.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HEXISTS key field` — returns 1 if the field exists, 0 otherwise.
pub fn hexists(session: &Session, key: &[u8], field: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HExistsOp {
            node_prefix: session.private_key(key),
            field: field.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HLEN key` — returns the number of fields, 0 if the key is missing.
pub fn hlen(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HLenOp {
            public_key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HKEYS key` — returns all field names, empty array if the key is missing.
pub fn hkeys(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HKeysOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HVALS key` — returns all field values, empty array if the key is missing.
pub fn hvals(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HValsOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HGETALL key` — returns all field-value pairs as a flat array, empty if
/// the key is missing.
pub fn hgetall(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HGetAllOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HMSET key field value [field value ...]` — identical to `HSET` but
/// replies `+OK` instead of an integer (deprecated in Redis 4.0).
pub fn hmset(session: &Session, key: &[u8], field_values: &[Bytes]) -> QueuedOp {
    let inner = hset(session, key, field_values);
    QueuedOp {
        db_op: inner.db_op,
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HINCRBY key field increment` — increments the integer value of a field by
/// `amount`, creating it (at 0) if absent.
pub fn hincrby(session: &Session, key: &[u8], field: &[u8], amount: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HIncrByOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            field: field.to_vec(),
            amount,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HINCRBYFLOAT key field increment` — increments the float value of a field
/// by `amount`, creating it (at 0) if absent.
pub fn hincrbyfloat(session: &Session, key: &[u8], field: &[u8], amount: f64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HIncrByFloatOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            field: field.to_vec(),
            amount,
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// `HRANDFIELD key [count [WITHVALUES]]` — returns random fields (and
/// optionally their values).  `count > 0` → at most `count` distinct fields
/// without replacement; `count < 0` → `-count` fields with replacement;
/// missing key → null.
pub fn hrandfield(session: &Session, key: &[u8], count: i64, with_values: bool) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HRandFieldOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            count,
            with_values,
        }),
        wire_op: Box::new(HRandFieldWire { count }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HSTRLEN key field` — returns the length of the field's string value, 0 if
/// the field is absent.
pub fn hstrlen(session: &Session, key: &[u8], field: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HStrLenOp {
            node_prefix: session.private_key(key),
            field: field.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `HSCAN key cursor [MATCH pattern] [COUNT count]` — iterates fields. The
/// cursor is always returned as "0" (full scan every time), mirroring Go.
pub fn hscan(session: &Session, key: &[u8], pattern: Vec<u8>, count: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(HScanOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            pattern,
            count,
        }),
        wire_op: Box::new(HScanWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

// --- DbOp structs and impls ---

struct HSetOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    field_values: Vec<Vec<u8>>,
}

impl DbOp for HSetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let field_values = self.field_values.clone();
        Box::pin(async move {
            let mut count = match read_sentinel(tx, &public_key).await {
                Ok(c) => c,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };

            let mut added = 0i64;
            let mut i = 0;
            while i + 1 < field_values.len() {
                let field = &field_values[i];
                let value = &field_values[i + 1];
                let fk = field_key(&node_prefix, field);
                match tx.get(&fk).await {
                    Ok(_) => {}
                    Err(KvError::KeyNotFound) => {
                        added += 1;
                        count += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
                tx.set(Entry::new(fk, value.clone()).metadata(TYPE_HASH))?;
                i += 2;
            }

            write_sentinel(tx, &public_key, count)?;
            let result: DbResult = Box::new(added);
            Ok(result)
        })
    }
}

struct HSetNxOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    field: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for HSetNxOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        let value = self.value.clone();
        Box::pin(async move {
            let fk = field_key(&node_prefix, &field);
            match tx.get(&fk).await {
                Ok(_) => {
                    // Field already exists — do nothing.
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(KvError::KeyNotFound) => {}
                Err(e) => return Err(e.into()),
            }

            tx.set(Entry::new(fk, value).metadata(TYPE_HASH))?;

            let mut count = match read_sentinel(tx, &public_key).await {
                Ok(c) => c,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };
            count += 1;
            write_sentinel(tx, &public_key, count)?;

            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct HGetOp {
    node_prefix: Vec<u8>,
    field: Vec<u8>,
}

impl DbOp for HGetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        Box::pin(async move {
            let fk = field_key(&node_prefix, &field);
            match tx.get(&fk).await {
                Ok(item) => {
                    let result: DbResult = Box::new(Some(item.value().to_vec()));
                    Ok(result)
                }
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    Ok(result)
                }
                Err(e) => Err(e.into()),
            }
        })
    }
}

struct HMGetOp {
    node_prefix: Vec<u8>,
    fields: Vec<Vec<u8>>,
}

impl DbOp for HMGetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node_prefix = self.node_prefix.clone();
        let fields = self.fields.clone();
        Box::pin(async move {
            let mut results: Vec<Option<Vec<u8>>> = Vec::with_capacity(fields.len());
            for field in &fields {
                let fk = field_key(&node_prefix, field);
                match tx.get(&fk).await {
                    Ok(item) => results.push(Some(item.value().to_vec())),
                    Err(KvError::KeyNotFound) => results.push(None),
                    Err(e) => return Err(e.into()),
                }
            }
            let result: DbResult = Box::new(results);
            Ok(result)
        })
    }
}

struct HDelOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    fields: Vec<Vec<u8>>,
}

impl DbOp for HDelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let fields = self.fields.clone();
        Box::pin(async move {
            let mut count = match read_sentinel(tx, &public_key).await {
                Ok(c) => c,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };

            let mut removed = 0i64;
            for field in &fields {
                let fk = field_key(&node_prefix, field);
                match tx.get(&fk).await {
                    Ok(_) => {
                        tx.delete(&fk)?;
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

struct HExistsOp {
    node_prefix: Vec<u8>,
    field: Vec<u8>,
}

impl DbOp for HExistsOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        Box::pin(async move {
            let fk = field_key(&node_prefix, &field);
            let exists = tx.get(&fk).await.is_ok();
            let result: DbResult = Box::new(if exists { 1i64 } else { 0i64 });
            Ok(result)
        })
    }
}

struct HLenOp {
    public_key: Vec<u8>,
}

impl DbOp for HLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        Box::pin(async move {
            let count = match read_sentinel(tx, &public_key).await {
                Ok(c) => c,
                Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                Err(e) => return Err(e),
            };
            let result: DbResult = Box::new(count as i64);
            Ok(result)
        })
    }
}

struct HKeysOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl DbOp for HKeysOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        Box::pin(async move {
            match tx.get(&public_key).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(Vec::<Vec<u8>>::new());
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            }

            let mut it = tx.new_prefix_iterator(&fields_prefix(&node_prefix)).await?;
            let mut keys: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    keys.push(field_from_internal_key(item.key()));
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            let result: DbResult = Box::new(keys);
            Ok(result)
        })
    }
}

struct HValsOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl DbOp for HValsOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        Box::pin(async move {
            match tx.get(&public_key).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(Vec::<Vec<u8>>::new());
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            }

            let mut it = tx.new_prefix_iterator(&fields_prefix(&node_prefix)).await?;
            let mut vals: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    vals.push(item.value().to_vec());
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            let result: DbResult = Box::new(vals);
            Ok(result)
        })
    }
}

struct HGetAllOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
}

impl DbOp for HGetAllOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        Box::pin(async move {
            match tx.get(&public_key).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(Vec::<Vec<u8>>::new());
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            }

            let mut it = tx.new_prefix_iterator(&fields_prefix(&node_prefix)).await?;
            let mut pairs: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    pairs.push(field_from_internal_key(item.key()));
                    pairs.push(item.value().to_vec());
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            let result: DbResult = Box::new(pairs);
            Ok(result)
        })
    }
}

struct HIncrByOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    field: Vec<u8>,
    amount: i64,
}

impl DbOp for HIncrByOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        let amount = self.amount;
        Box::pin(async move {
            let fk = field_key(&node_prefix, &field);
            let (current, field_existed) = match tx.get(&fk).await {
                Ok(item) => {
                    let val = item.value().to_vec();
                    let n = std::str::from_utf8(&val)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or_else(|| {
                            DbError::Redis("hash value is not an integer".to_string())
                        })?;
                    (n, true)
                }
                Err(KvError::KeyNotFound) => (0i64, false),
                Err(e) => return Err(e.into()),
            };

            let new_val = current + amount;
            tx.set(
                Entry::new(fk, new_val.to_string().into_bytes()).metadata(TYPE_HASH),
            )?;

            if !field_existed {
                let mut count = match read_sentinel(tx, &public_key).await {
                    Ok(c) => c,
                    Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                    Err(e) => return Err(e),
                };
                count += 1;
                write_sentinel(tx, &public_key, count)?;
            }

            let result: DbResult = Box::new(new_val);
            Ok(result)
        })
    }
}

struct HIncrByFloatOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    field: Vec<u8>,
    amount: f64,
}

impl DbOp for HIncrByFloatOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        let amount = self.amount;
        Box::pin(async move {
            if amount.is_nan() {
                return Err(DbError::Redis("value is not a valid float".to_string()));
            }

            let fk = field_key(&node_prefix, &field);
            let (current, field_existed) = match tx.get(&fk).await {
                Ok(item) => {
                    let val = item.value().to_vec();
                    let f = std::str::from_utf8(&val)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .ok_or_else(|| {
                            DbError::Redis("hash value is not a float".to_string())
                        })?;
                    (f, true)
                }
                Err(KvError::KeyNotFound) => (0.0f64, false),
                Err(e) => return Err(e.into()),
            };

            let new_val = current + amount;
            let new_str = format_float(new_val);
            tx.set(
                Entry::new(fk, new_str.clone().into_bytes()).metadata(TYPE_HASH),
            )?;

            if !field_existed {
                let mut count = match read_sentinel(tx, &public_key).await {
                    Ok(c) => c,
                    Err(DbError::Kv(KvError::KeyNotFound)) => 0,
                    Err(e) => return Err(e),
                };
                count += 1;
                write_sentinel(tx, &public_key, count)?;
            }

            let result: DbResult = Box::new(Some(new_str.into_bytes()));
            Ok(result)
        })
    }
}

struct HRandFieldOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    count: i64,
    with_values: bool,
}

impl DbOp for HRandFieldOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let count = self.count;
        let with_values = self.with_values;
        Box::pin(async move {
            match tx.get(&public_key).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<Vec<Vec<u8>>>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            }

            let mut it = tx.new_prefix_iterator(&fields_prefix(&node_prefix)).await?;
            let mut field_list: Vec<Vec<u8>> = Vec::new();
            let mut val_list: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    field_list.push(field_from_internal_key(item.key()));
                    val_list.push(item.value().to_vec());
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            if count == 0 || field_list.is_empty() {
                let result: DbResult = Box::new(Some(Vec::<Vec<u8>>::new()));
                return Ok(result);
            }

            let out: Vec<Vec<u8>> = if count > 0 {
                let n = count as usize;
                if n >= field_list.len() {
                    // Return all fields (possibly with values).
                    if with_values {
                        let mut r = Vec::with_capacity(field_list.len() * 2);
                        for i in 0..field_list.len() {
                            r.push(field_list[i].clone());
                            r.push(val_list[i].clone());
                        }
                        r
                    } else {
                        field_list
                    }
                } else {
                    // Sample without replacement.
                    let perm = rand_perm(field_list.len());
                    if with_values {
                        let mut r = Vec::with_capacity(n * 2);
                        for &idx in perm.iter().take(n) {
                            r.push(field_list[idx].clone());
                            r.push(val_list[idx].clone());
                        }
                        r
                    } else {
                        perm.iter().take(n).map(|&idx| field_list[idx].clone()).collect()
                    }
                }
            } else {
                // Negative count: sample with replacement.
                let n = (-count) as usize;
                if with_values {
                    let mut r = Vec::with_capacity(n * 2);
                    for _ in 0..n {
                        let idx = rand_index(field_list.len() as u32);
                        r.push(field_list[idx].clone());
                        r.push(val_list[idx].clone());
                    }
                    r
                } else {
                    (0..n)
                        .map(|_| field_list[rand_index(field_list.len() as u32)].clone())
                        .collect()
                }
            };

            let result: DbResult = Box::new(Some(out));
            Ok(result)
        })
    }
}

struct HStrLenOp {
    node_prefix: Vec<u8>,
    field: Vec<u8>,
}

impl DbOp for HStrLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let node_prefix = self.node_prefix.clone();
        let field = self.field.clone();
        Box::pin(async move {
            let fk = field_key(&node_prefix, &field);
            match tx.get(&fk).await {
                Ok(item) => {
                    let result: DbResult = Box::new(item.value().len() as i64);
                    Ok(result)
                }
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    Ok(result)
                }
                Err(e) => Err(e.into()),
            }
        })
    }
}

struct HScanOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    pattern: Vec<u8>,
    count: i64,
}

impl DbOp for HScanOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let pattern = self.pattern.clone();
        let count = self.count;
        Box::pin(async move {
            match tx.get(&public_key).await {
                Ok(_) => {}
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(Vec::<Vec<u8>>::new());
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            }

            let match_pattern = !pattern.is_empty();
            let mut it = tx.new_prefix_iterator(&fields_prefix(&node_prefix)).await?;
            let mut pairs: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                if let Some(item) = it.item() {
                    let field = field_from_internal_key(item.key());

                    if match_pattern
                        && !glob_match(
                            std::str::from_utf8(&pattern).unwrap_or(""),
                            std::str::from_utf8(&field).unwrap_or(""),
                        )
                    {
                        continue;
                    }

                    pairs.push(field);
                    pairs.push(item.value().to_vec());

                    if count > 0 && (pairs.len() / 2) as i64 >= count {
                        break;
                    }
                }
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;

            let result: DbResult = Box::new(pairs);
            Ok(result)
        })
    }
}

/// Simple glob pattern matcher (mirrors Go's `path.Match`). Supports `*`
/// (any sequence of chars) and `?` (any single char). No character classes.
fn glob_match(pattern: &str, s: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = s.chars().collect();
    glob_match_inner(&pat, &s)
}

fn glob_match_inner(pat: &[char], s: &[char]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some('*'), _) => {
            // '*' can match zero or more characters.
            if glob_match_inner(&pat[1..], s) {
                return true;
            }
            if s.is_empty() {
                return false;
            }
            glob_match_inner(pat, &s[1..])
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &s[1..]),
        (Some(p), Some(c)) if p == c => glob_match_inner(&pat[1..], &s[1..]),
        _ => false,
    }
}

// --- WireOp halves ---

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

/// Replies an array where each element is either a bulk string or null.
struct NullableArrayWire;

impl WireOp for NullableArrayWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Option<Vec<u8>>>>() {
                Ok(boxed) => RespValue::Array(Some(
                    boxed
                        .iter()
                        .map(|v| match v {
                            Some(val) => RespValue::BulkString(Some(Bytes::copy_from_slice(val))),
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

/// Replies an array of non-null bulk strings.
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

/// Replies `+OK`.
struct OkWire;

impl WireOp for OkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `HRANDFIELD`: a null bulk string when the key is missing, an array
/// of bulk strings otherwise.
struct HRandFieldWire {
    count: i64,
}

impl WireOp for HRandFieldWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<Vec<Vec<u8>>>>() {
                Ok(boxed) => match *boxed {
                    None => RespValue::BulkString(None),
                    Some(items) => {
                        if self.count == 1 && !items.is_empty() {
                            // When count is 1 with no WITHVALUES, return a single bulk string.
                            RespValue::BulkString(Some(Bytes::copy_from_slice(&items[0])))
                        } else {
                            RespValue::Array(Some(
                                items
                                    .iter()
                                    .map(|v| RespValue::BulkString(Some(Bytes::copy_from_slice(v))))
                                    .collect(),
                            ))
                        }
                    }
                },
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `HSCAN`: `[cursor, [field, val, ...]]` where cursor is always "0".
struct HScanWire;

impl WireOp for HScanWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Vec<u8>>>() {
                Ok(pairs) => RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"0"))),
                    RespValue::Array(Some(
                        pairs
                            .iter()
                            .map(|v| RespValue::BulkString(Some(Bytes::copy_from_slice(v))))
                            .collect(),
                    )),
                ])),
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

    fn expect_bulk_array(reply: &RespValue) -> Vec<Vec<u8>> {
        match reply {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::BulkString(Some(b)) => b.to_vec(),
                    other => panic!("expected non-null bulk, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hset_new_and_update() {
        let session = test_session();
        // New field: returns 1.
        let r = exec(&session, hset(&session, b"h", &[Bytes::from_static(b"f"), Bytes::from_static(b"v")])).await;
        assert_eq!(expect_int(&r), 1);
        // Update: returns 0.
        let r = exec(&session, hset(&session, b"h", &[Bytes::from_static(b"f"), Bytes::from_static(b"v2")])).await;
        assert_eq!(expect_int(&r), 0);
    }

    #[tokio::test]
    async fn hset_multiple_fields() {
        let session = test_session();
        let r = exec(
            &session,
            hset(
                &session,
                b"h",
                &[
                    Bytes::from_static(b"a"), Bytes::from_static(b"1"),
                    Bytes::from_static(b"b"), Bytes::from_static(b"2"),
                    Bytes::from_static(b"c"), Bytes::from_static(b"3"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_int(&r), 3);
        assert_eq!(expect_int(&exec(&session, hlen(&session, b"h")).await), 3);
    }

    #[tokio::test]
    async fn hget_existing_and_missing() {
        let session = test_session();
        exec(&session, hset(&session, b"h", &[Bytes::from_static(b"f"), Bytes::from_static(b"v")])).await;
        let r = exec(&session, hget(&session, b"h", b"f")).await;
        assert_eq!(expect_bulk(&r), Some(Bytes::from_static(b"v")));
        let r = exec(&session, hget(&session, b"h", b"missing")).await;
        assert_eq!(expect_bulk(&r), None);
        let r = exec(&session, hget(&session, b"no_key", b"f")).await;
        assert_eq!(expect_bulk(&r), None);
    }

    #[tokio::test]
    async fn hdel_removes_and_cleans_sentinel() {
        let session = test_session();
        exec(
            &session,
            hset(&session, b"h", &[Bytes::from_static(b"f1"), Bytes::from_static(b"v1"), Bytes::from_static(b"f2"), Bytes::from_static(b"v2")]),
        )
        .await;
        let r = exec(&session, hdel(&session, b"h", &[Bytes::from_static(b"f1")])).await;
        assert_eq!(expect_int(&r), 1);
        assert_eq!(expect_int(&exec(&session, hlen(&session, b"h")).await), 1);

        // Delete last field: sentinel should also go away.
        let r = exec(&session, hdel(&session, b"h", &[Bytes::from_static(b"f2")])).await;
        assert_eq!(expect_int(&r), 1);
        assert_eq!(expect_int(&exec(&session, hlen(&session, b"h")).await), 0);
    }

    #[tokio::test]
    async fn hexists_and_hsetnx() {
        let session = test_session();
        exec(&session, hset(&session, b"h", &[Bytes::from_static(b"f"), Bytes::from_static(b"v")])).await;
        assert_eq!(expect_int(&exec(&session, hexists(&session, b"h", b"f")).await), 1);
        assert_eq!(expect_int(&exec(&session, hexists(&session, b"h", b"x")).await), 0);

        // HSETNX on existing → 0.
        assert_eq!(expect_int(&exec(&session, hsetnx(&session, b"h", b"f", b"new")).await), 0);
        // HSETNX on new field → 1.
        assert_eq!(expect_int(&exec(&session, hsetnx(&session, b"h", b"g", b"new")).await), 1);
        assert_eq!(expect_int(&exec(&session, hlen(&session, b"h")).await), 2);
    }

    #[tokio::test]
    async fn hmget_nullable_array() {
        let session = test_session();
        exec(
            &session,
            hset(&session, b"h", &[Bytes::from_static(b"a"), Bytes::from_static(b"1"), Bytes::from_static(b"b"), Bytes::from_static(b"2")]),
        )
        .await;
        let r = exec(
            &session,
            hmget(&session, b"h", &[Bytes::from_static(b"a"), Bytes::from_static(b"x"), Bytes::from_static(b"b")]),
        )
        .await;
        match r {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from_static(b"1"))));
                assert_eq!(items[1], RespValue::BulkString(None));
                assert_eq!(items[2], RespValue::BulkString(Some(Bytes::from_static(b"2"))));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hkeys_hvals_hgetall() {
        let session = test_session();
        exec(
            &session,
            hset(&session, b"h", &[Bytes::from_static(b"a"), Bytes::from_static(b"1"), Bytes::from_static(b"b"), Bytes::from_static(b"2")]),
        )
        .await;

        let mut keys = expect_bulk_array(&exec(&session, hkeys(&session, b"h")).await);
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        let mut vals = expect_bulk_array(&exec(&session, hvals(&session, b"h")).await);
        vals.sort();
        assert_eq!(vals, vec![b"1".to_vec(), b"2".to_vec()]);

        let all = expect_bulk_array(&exec(&session, hgetall(&session, b"h")).await);
        assert_eq!(all.len(), 4);

        // Missing key → empty.
        assert_eq!(expect_bulk_array(&exec(&session, hkeys(&session, b"missing")).await).len(), 0);
    }

    #[tokio::test]
    async fn hincrby_and_hincrbyfloat() {
        let session = test_session();
        // HINCRBY on new field.
        let r = exec(&session, hincrby(&session, b"h", b"c", 5)).await;
        assert_eq!(expect_int(&r), 5);
        let r = exec(&session, hincrby(&session, b"h", b"c", 3)).await;
        assert_eq!(expect_int(&r), 8);

        // HINCRBYFLOAT on new field.
        let r = exec(&session, hincrbyfloat(&session, b"h", b"s", 1.5)).await;
        assert_eq!(expect_bulk(&r), Some(Bytes::from_static(b"1.5")));
        let r = exec(&session, hincrbyfloat(&session, b"h", b"s", 2.5)).await;
        assert_eq!(expect_bulk(&r), Some(Bytes::from_static(b"4")));
    }

    #[tokio::test]
    async fn hstrlen_existing_and_missing() {
        let session = test_session();
        exec(&session, hset(&session, b"h", &[Bytes::from_static(b"f"), Bytes::from_static(b"hello!")])).await;
        assert_eq!(expect_int(&exec(&session, hstrlen(&session, b"h", b"f")).await), 6);
        assert_eq!(expect_int(&exec(&session, hstrlen(&session, b"h", b"missing")).await), 0);
    }

    #[tokio::test]
    async fn hrandfield_missing_key_is_null() {
        let session = test_session();
        let r = exec(&session, hrandfield(&session, b"missing", 1, false)).await;
        assert_eq!(expect_bulk(&r), None);
    }

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h*llo", "hello"));
        assert!(glob_match("h?llo", "hello"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(!glob_match("exact", "other"));
        assert!(glob_match("", ""));
        assert!(!glob_match("", "a"));
    }
}
