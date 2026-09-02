//! Redis string commands: `SET`, `GET`, `SETEX`/`PSETEX`, `GETSET`, `GETDEL`,
//! `STRLEN`, `SUBSTR`/`GETRANGE`, `SETNX`, `APPEND`, `GETEX`, `INCRBYFLOAT`,
//! `MSET`/`MSETNX`, `SETRANGE`, and `INCR`/`INCRBY`/`DECR`/`DECRBY`.
//!
//! Port of the Go `redis/strings` package. String values are stored as a
//! single entry under the public key (derived via the session) stamped with
//! `ValueType::String` metadata, optionally carrying a TTL. Read paths verify
//! the metadata byte and reply `WRONGTYPE` when the key holds another value
//! type, mirroring Go.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// The metadata byte stored on every string entry.
const TYPE_STRING: u8 = ValueType::String as u8;

/// Formats a float the way Go's `redis/common.FormatFloat` does (used by
/// `INCRBYFLOAT`); `inf`/`-inf` for the infinities, otherwise shortest
/// round-trip decimal digits.
fn format_float(v: f64) -> String {
    if v > 0.0 && v.is_infinite() {
        return "inf".to_string();
    }
    if v < 0.0 && v.is_infinite() {
        return "-inf".to_string();
    }
    format!("{v}")
}

/// Parses a base-10 signed 64-bit integer, rejecting trailing garbage.
fn parse_int(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Reads the string value at public key `key`, mapping a missing key to
/// `None`, and a key holding another value type to [`DbError::WrongType`].
async fn read_string(tx: &dyn Tx, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
    let item = match tx.get(key).await {
        Ok(item) => item,
        Err(KvError::KeyNotFound) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if item.metadata() != TYPE_STRING {
        return Err(DbError::WrongType);
    }
    Ok(Some(item.value().to_vec()))
}

/// `SET key value [EX seconds] [PX milliseconds]` — stores a string value,
/// optionally with a TTL.
pub fn set(session: &Session, key: &[u8], value: &[u8], ttl: Option<Duration>) -> QueuedOp {
    set_full(session, key, value, ttl, SetMode::None, false, false)
}

/// The conditional component of `SET key value [NX|XX]`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetMode {
    /// Plain unconditional `SET`.
    None,
    /// `NX`: set only if the key does not exist.
    Nx,
    /// `XX`: set only if the key already exists.
    Xx,
}

/// Fully-featured `SET` supporting `NX`/`XX` conditions, the `GET` option
/// (returns the previous value), and `KEEPTTL` (retain the existing TTL).
pub fn set_full(
    session: &Session,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
    mode: SetMode,
    get: bool,
    keepttl: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SetOp {
            key: session.public_key(key),
            value: value.to_vec(),
            ttl,
            mode,
            get,
            keepttl,
        }),
        wire_op: Box::new(SetWire { get }),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SETEX key seconds value` — stores a string value with a TTL in seconds.
pub fn set_ex(session: &Session, key: &[u8], value: &[u8], seconds: i64) -> QueuedOp {
    let ttl = u64::try_from(seconds)
        .ok()
        .map(Duration::from_secs)
        .unwrap_or(Duration::ZERO);
    set_full(session, key, value, Some(ttl), SetMode::None, false, false)
}

/// `PSETEX key milliseconds value` — stores a string value with a TTL in
/// milliseconds.
pub fn pset_ex(session: &Session, key: &[u8], value: &[u8], ms: i64) -> QueuedOp {
    let ttl = u64::try_from(ms)
        .ok()
        .map(Duration::from_millis)
        .unwrap_or(Duration::ZERO);
    set_full(session, key, value, Some(ttl), SetMode::None, false, false)
}

/// `GET key` — returns the string value stored at key, or nil if missing.
pub fn get(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `GETSET key value` — sets the value at key and returns the previous value,
/// or nil if the key was missing.
pub fn get_set(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetSetOp {
            key: session.public_key(key),
            value: value.to_vec(),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `GETDEL key` — returns the string value at key and deletes it, or nil if
/// the key is missing.
pub fn get_del(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetDelOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `STRLEN key` — returns the length of the string value at key, or 0 if
/// missing.
pub fn strlen(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StrlenOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SUBSTR key start end` / `GETRANGE key start end` — returns a substring of
/// the value in the inclusive range `[start, end]`, supporting negative
/// indices. An empty bulk is returned for a missing key or an out-of-range
/// slice.
pub fn substr(session: &Session, key: &[u8], start: i64, end: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SubstrOp {
            key: session.public_key(key),
            start,
            end,
        }),
        wire_op: Box::new(BulkWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SETNX key value` — stores a string value only if the key doesn't exist,
/// returning 1 if the value was set and 0 otherwise.
pub fn set_nx(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SetNXOp {
            key: session.public_key(key),
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `APPEND key value` — appends value to the string at key, creating it if
/// missing, and returns the new length.
pub fn append(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(AppendOp {
            key: session.public_key(key),
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `GETEX key [EX seconds | PX ms | EXAT unix-secs | PXAT unix-ms | PERSIST]` —
/// returns the value at key, optionally setting a TTL or clearing one.
/// `args[0]` is the key; options follow.
pub fn get_ex(session: &Session, args: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetExOp {
            key: session.public_key(&args[0]),
            args: args[1..].to_vec(),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `INCRBYFLOAT key increment` — increments the value at key by `amount`,
/// creating it if missing, and returns the new value as a bulk string.
pub fn incr_by_float(session: &Session, key: &[u8], amount: f64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(IncrByFloatOp {
            key: session.public_key(key),
            amount,
        }),
        wire_op: Box::new(BulkStringWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `MSET key value [key value ...]` — sets multiple string keys atomically.
/// `args` is an alternating key/value list.
pub fn mset(session: &Session, args: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MSetOp {
            pairs: args
                .chunks(2)
                .map(|pair| (session.public_key(&pair[0]), pair[1].to_vec()))
                .collect(),
        }),
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `MSETNX key value [key value ...]` — sets multiple string keys only if
/// none of them exist, returning 1 if all were set and 0 otherwise.
pub fn mset_nx(session: &Session, args: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MSetNXOp {
            pairs: args
                .chunks(2)
                .map(|pair| (session.public_key(&pair[0]), pair[1].to_vec()))
                .collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SETRANGE key offset value` — overwrites part of the string at key
/// starting at offset, returning the new length.
pub fn set_range(session: &Session, key: &[u8], offset: i64, value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SetRangeOp {
            key: session.public_key(key),
            offset,
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `INCR`/`INCRBY`/`DECR`/`DECRBY` — adds `amount` to the integer value at
/// key, creating it if missing, and returns the new value.
pub fn increment(session: &Session, key: &[u8], amount: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(IncrementOp {
            key: session.public_key(key),
            amount,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// --- DbOp halves ---

/// The owned result of a conditional `SET`, distinguishing "not modified"
/// (e.g. `NX` when the key exists) from a successful write.
struct SetResult {
    modified: bool,
    old: Option<Vec<u8>>,
}

/// A string write, with an optional TTL (`SET`, `SETEX`, `PSETEX`).
struct SetOp {
    key: Vec<u8>,
    value: Vec<u8>,
    ttl: Option<Duration>,
    mode: SetMode,
    get: bool,
    keepttl: bool,
}

impl DbOp for SetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let value = self.value.clone();
        let ttl = self.ttl;
        let mode = self.mode;
        let get = self.get;
        let keepttl = self.keepttl;
        Box::pin(async move {
            let existing = match tx.get(&key).await {
                Ok(item) => {
                    if get && item.metadata() != TYPE_STRING {
                        return Err(DbError::WrongType);
                    }
                    Some(item)
                }
                Err(KvError::KeyNotFound) => None,
                Err(e) => return Err(e.into()),
            };

            let allowed = match mode {
                SetMode::None => true,
                SetMode::Nx => existing.is_none(),
                SetMode::Xx => existing.is_some(),
            };
            let old = existing.as_ref().map(|i| i.value().to_vec());

            if allowed {
                let effective_ttl = if keepttl {
                    existing
                        .as_ref()
                        .map(|i| i.ttl()).unwrap()
                } else {
                    ttl
                };
                let mut entry = Entry::new(key, value).metadata(TYPE_STRING);
                if effective_ttl.is_some() {
                    entry = entry.ttl(effective_ttl.unwrap());
                }
                tx.set(entry)?;
            }

            let result: DbResult = Box::new(SetResult { modified: allowed, old });
            Ok(result)
        })
    }
}

/// Wire half for [`SetOp`]: `+OK` when modified, a null bulk when the
/// condition failed, or the previous value when the `GET` option was given.
struct SetWire {
    get: bool,
}

impl WireOp for SetWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<SetResult>() {
                Ok(set_result) => {
                    if self.get {
                        RespValue::BulkString(
                            set_result
                                .old
                                .map(Bytes::from)
                                .map(Some)
                                .unwrap_or(None),
                        )
                    } else if set_result.modified {
                        RespValue::SimpleString(Bytes::from_static(b"OK"))
                    } else {
                        RespValue::BulkString(None)
                    }
                }
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error")),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// A string read returning the value (used by `GET` and friends' DbOp shape).
struct GetOp {
    key: Vec<u8>,
}

impl DbOp for GetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let result: DbResult = Box::new(read_string(tx, &key).await?);
            Ok(result)
        })
    }
}

struct GetSetOp {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for GetSetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let value = self.value.clone();
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    tx.set(Entry::new(key, value).metadata(TYPE_STRING))?;
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            let old = item.value().to_vec();
            tx.set(Entry::new(key, value).metadata(TYPE_STRING))?;
            let result: DbResult = Box::new(Some(old));
            Ok(result)
        })
    }
}

struct GetDelOp {
    key: Vec<u8>,
}

impl DbOp for GetDelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            if item.metadata() != TYPE_STRING {
                return Err(DbError::WrongType);
            }
            let val = item.value().to_vec();
            if tx.delete(&key).is_err() {
                // Mirror Go: an error deleting yields a nil reply.
                let result: DbResult = Box::new(None::<Vec<u8>>);
                return Ok(result);
            }
            let result: DbResult = Box::new(Some(val));
            Ok(result)
        })
    }
}

struct StrlenOp {
    key: Vec<u8>,
}

impl DbOp for StrlenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let len = match read_string(tx, &key).await? {
                Some(val) => val.len() as i64,
                None => 0,
            };
            let result: DbResult = Box::new(len);
            Ok(result)
        })
    }
}

struct SubstrOp {
    key: Vec<u8>,
    start: i64,
    end: i64,
}

impl DbOp for SubstrOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let (mut start, mut end) = (self.start, self.end);
        Box::pin(async move {
            let val = read_string(tx, &key).await?.unwrap_or_default();
            let n = val.len() as i64;
            if start < 0 {
                start += n;
            }
            if end < 0 {
                end += n;
            }
            if start < 0 {
                start = 0;
            }
            if end >= n {
                end = n - 1;
            }
            let sliced: Vec<u8> = if start > end || start >= n {
                Vec::new()
            } else {
                val[start as usize..=end as usize].to_vec()
            };
            let result: DbResult = Box::new(sliced);
            Ok(result)
        })
    }
}

struct SetNXOp {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for SetNXOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let value = self.value.clone();
        Box::pin(async move {
            match tx.get(&key).await {
                Ok(_) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(KvError::KeyNotFound) => {
                    tx.set(Entry::new(key, value).metadata(TYPE_STRING))?;
                }
                Err(e) => return Err(e.into()),
            }
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct AppendOp {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for AppendOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let value = self.value.clone();
        Box::pin(async move {
            let new_val = match read_string(tx, &key).await? {
                Some(old) => {
                    let mut new_val = old;
                    new_val.extend_from_slice(&value);
                    new_val
                }
                None => value.clone(),
            };
            tx.set(Entry::new(key, new_val.clone()).metadata(TYPE_STRING))?;
            let result: DbResult = Box::new(new_val.len() as i64);
            Ok(result)
        })
    }
}

struct GetExOp {
    key: Vec<u8>,
    args: Vec<Bytes>,
}

impl DbOp for GetExOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let args = self.args.clone();
        Box::pin(async move {
            // Parse options.
            let mut ex_sec: Option<i64> = None;
            let mut px_ms: Option<i64> = None;
            let mut exat_sec: Option<i64> = None;
            let mut pxat_ms: Option<i64> = None;
            let mut persist = false;
            let mut i = 0;
            while i < args.len() {
                let opt = args[i]
                    .iter()
                    .map(u8::to_ascii_lowercase)
                    .collect::<Vec<u8>>();
                match opt.as_slice() {
                    b"persist" => persist = true,
                    b"ex" | b"px" | b"exat" | b"pxat" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(DbError::Redis("syntax error".into()));
                        }
                        let Some(v) = parse_int(&args[i]) else {
                            return Err(DbError::Redis(
                                "value is not an integer or out of range".into(),
                            ));
                        };
                        match opt.as_slice() {
                            b"ex" => ex_sec = Some(v),
                            b"px" => px_ms = Some(v),
                            b"exat" => exat_sec = Some(v),
                            _ => pxat_ms = Some(v),
                        }
                    }
                    _ => return Err(DbError::Redis("syntax error".into())),
                }
                i += 1;
            }

            // Read the value.
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            if item.metadata() != TYPE_STRING {
                return Err(DbError::WrongType);
            }
            let val = item.value().to_vec();

            // Apply the requested TTL change.
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let write = if persist {
                Some(Duration::ZERO)
            } else if let Some(secs) = ex_sec {
                u64::try_from(secs).ok().map(Duration::from_secs)
            } else if let Some(ms) = px_ms {
                u64::try_from(ms).ok().map(Duration::from_millis)
            } else if let Some(secs) = exat_sec {
                let ttl_ms = secs.saturating_mul(1000).saturating_sub(now_ms);
                Some(Duration::from_millis(ttl_ms.max(0) as u64))
            } else if let Some(ms) = pxat_ms {
                let ttl_ms = ms.saturating_sub(now_ms);
                Some(Duration::from_millis(ttl_ms.max(0) as u64))
            } else {
                None
            };
            if let Some(ttl) = write {
                let mut entry = Entry::new(key, val.clone()).metadata(TYPE_STRING);
                if !ttl.is_zero() {
                    entry = entry.ttl(ttl);
                }
                tx.set(entry)?;
            }
            let result: DbResult = Box::new(Some(val));
            Ok(result)
        })
    }
}

struct IncrByFloatOp {
    key: Vec<u8>,
    amount: f64,
}

impl DbOp for IncrByFloatOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let amount = self.amount;
        Box::pin(async move {
            if amount.is_nan() {
                return Err(DbError::Redis("value is not a valid float".into()));
            }
            let result_str = match read_string(tx, &key).await? {
                None => {
                    let result = amount;
                    tx.set(
                        Entry::new(key, format_float(result).into_bytes()).metadata(TYPE_STRING),
                    )?;
                    format_float(result)
                }
                Some(val) => {
                    let parsed = std::str::from_utf8(&val)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok());
                    let Some(current) = parsed else {
                        return Err(DbError::Redis("value is not a float".into()));
                    };
                    if current.is_infinite() {
                        return Err(DbError::Redis("value is not a float".into()));
                    }
                    let result = current + amount;
                    if result.is_infinite() {
                        // Mirror Go: an overflow writes no value, only the
                        // wire reply indicates the infinity.
                        if result.is_sign_negative() {
                            return Ok(Box::new("-inf".to_string()) as DbResult);
                        }
                        return Ok(Box::new("inf".to_string()) as DbResult);
                    }
                    tx.set(
                        Entry::new(key, format_float(result).into_bytes()).metadata(TYPE_STRING),
                    )?;
                    format_float(result)
                }
            };
            let result: DbResult = Box::new(result_str);
            Ok(result)
        })
    }
}

struct MSetOp {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl DbOp for MSetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let pairs = self.pairs.clone();
        Box::pin(async move {
            for (key, value) in &pairs {
                tx.set(Entry::new(key.clone(), value.clone()).metadata(TYPE_STRING))?;
            }
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

struct MSetNXOp {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl DbOp for MSetNXOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let pairs = self.pairs.clone();
        Box::pin(async move {
            for (key, _) in &pairs {
                match tx.get(key).await {
                    Ok(_) => {
                        let result: DbResult = Box::new(0i64);
                        return Ok(result);
                    }
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            for (key, value) in &pairs {
                tx.set(Entry::new(key.clone(), value.clone()).metadata(TYPE_STRING))?;
            }
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct SetRangeOp {
    key: Vec<u8>,
    offset: i64,
    value: Vec<u8>,
}

impl DbOp for SetRangeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let offset = self.offset as usize;
        let value = self.value.clone();
        Box::pin(async move {
            let new_val = match read_string(tx, &key).await? {
                None => {
                    let mut new_val = vec![0u8; offset + value.len()];
                    new_val[offset..offset + value.len()].copy_from_slice(&value);
                    new_val
                }
                Some(old) => {
                    let new_len = (offset + value.len()).max(old.len());
                    let mut new_val = vec![0u8; new_len];
                    new_val[..old.len()].copy_from_slice(&old);
                    new_val[offset..offset + value.len()].copy_from_slice(&value);
                    new_val
                }
            };
            let len = new_val.len();
            tx.set(Entry::new(key, new_val).metadata(TYPE_STRING))?;
            let result: DbResult = Box::new(len as i64);
            Ok(result)
        })
    }
}

struct IncrementOp {
    key: Vec<u8>,
    amount: i64,
}

impl DbOp for IncrementOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let amount = self.amount;
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let current = amount;
                    tx.set(
                        Entry::new(key, current.to_string().into_bytes()).metadata(TYPE_STRING),
                    )?;
                    let result: DbResult = Box::new(current);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            let current = std::str::from_utf8(item.value())
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| DbError::Redis("value is not an integer or out of range".into()))?;
            let new_value = current.wrapping_add(amount);
            tx.set(Entry::new(key, new_value.to_string().into_bytes()).metadata(item.metadata()))?;
            let result: DbResult = Box::new(new_value);
            Ok(result)
        })
    }
}

// --- WireOp halves ---

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

/// Replies an integer result.
struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(value) => RespValue::Integer(*value),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad int result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a null bulk for `None`, a bulk otherwise.
struct NullableBulkWire;

impl WireOp for NullableBulkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<Vec<u8>>>() {
                Ok(boxed) => match *boxed {
                    Some(value) => RespValue::BulkString(Some(Bytes::from(value))),
                    None => RespValue::BulkString(None),
                },
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad bulk result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an always-present bulk (used by `SUBSTR`/`GETRANGE`, which return
/// an empty bulk rather than null for a missing key).
struct BulkWire;

impl WireOp for BulkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<u8>>() {
                Ok(value) => RespValue::BulkString(Some(Bytes::from(*value))),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad bulk result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a bulk built from a `String` result (used by `INCRBYFLOAT`).
struct BulkStringWire;

impl WireOp for BulkStringWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<String>() {
                Ok(value) => RespValue::BulkString(Some(Bytes::from(*value))),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad bulk result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ValueType;
    use crate::testutil::test_session;

    /// Runs one op through its own transaction, committing if it mutates, and
    /// renders the reply.
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Runs a db op expecting success, returning the raw result.
    async fn exec_db(session: &Session, op: QueuedOp) -> DbResult {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await.expect("op failed");
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        outcome
    }

    /// Runs a db op expecting a Redis error, returning the message Bytes.
    async fn exec_db_err(session: &Session, op: QueuedOp) -> Bytes {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let err = op.db_op.run(&*tx).await.expect_err("expected op error");
        match err_resp(&err) {
            RespValue::Error(msg) => msg,
            other => panic!("expected error reply, got {other:?}"),
        }
    }

    /// Seeds a string value directly under the public key.
    async fn seed_string(session: &Session, key: &[u8], val: &[u8]) {
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(Entry::new(session.public_key(key), val.to_vec()).metadata(TYPE_STRING))
            .expect("seed");
        tx.commit().await.expect("commit");
    }

    /// Seeds a key holding a non-string value type.
    async fn seed_wrong_type(session: &Session, key: &[u8]) {
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(Entry::new(session.public_key(key), b"v".to_vec()).metadata(ValueType::List as u8))
            .expect("seed");
        tx.commit().await.expect("commit");
    }

    /// Reads the stored value at key (any metadata), or None if missing.
    async fn stored(session: &Session, key: &[u8]) -> Option<Vec<u8>> {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        match tx.get(&session.public_key(key)).await {
            Ok(item) => Some(item.value().to_vec()),
            Err(KvError::KeyNotFound) => None,
            Err(e) => panic!("get failed: {e:?}"),
        }
    }

    /// Reads the stored TTL at key
    async fn stored_ttl(session: &Session, key: &[u8]) -> Option<Duration> {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        match tx.get(&session.public_key(key)).await {
            Ok(item) => item.ttl(),
            Err(e) => panic!("get failed: {e:?}"),
        }
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

    #[tokio::test]
    async fn set_get_roundtrip() {
        let session = test_session();

        assert_eq!(
            exec(&session, set(&session, b"k", b"v1", None)).await,
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            expect_bulk(&exec(&session, get(&session, b"k")).await),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            expect_bulk(&exec(&session, get(&session, b"missing")).await),
            None
        );
        assert_eq!(
            exec(&session, get(&session, b"missing")).await,
            RespValue::BulkString(None)
        );

        seed_wrong_type(&session, b"wt").await;
        let err = exec(&session, get(&session, b"wt")).await;
        assert_eq!(
            err,
            RespValue::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ))
        );
    }

    #[tokio::test]
    async fn set_ex_sets_ttl() {
        let session = test_session();
        exec(&session, set_ex(&session, b"k", b"v", 100)).await;
        assert_ne!(stored_ttl(&session, b"k").await, None);
    }

    #[tokio::test]
    async fn pset_ex_sets_ttl() {
        let session = test_session();
        assert_eq!(
            exec(&session, pset_ex(&session, b"k", b"v", 5000)).await,
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_ne!(stored_ttl(&session, b"k").await, None);
    }

    #[tokio::test]
    async fn getset_returns_old_value() {
        let session = test_session();
        seed_string(&session, b"k", b"old").await;

        let reply = exec(&session, get_set(&session, b"k", b"new")).await;
        assert_eq!(expect_bulk(&reply), Some(Bytes::from_static(b"old")));
        assert_eq!(stored(&session, b"k").await, Some(b"new".to_vec()));

        let reply = exec(&session, get_set(&session, b"missing", b"v")).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn getdel_returns_and_deletes() {
        let session = test_session();
        seed_string(&session, b"k", b"v").await;

        let reply = exec(&session, get_del(&session, b"k")).await;
        assert_eq!(expect_bulk(&reply), Some(Bytes::from_static(b"v")));
        assert_eq!(stored(&session, b"k").await, None);

        let reply = exec(&session, get_del(&session, b"missing")).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn strlen_returns_length() {
        let session = test_session();
        seed_string(&session, b"k", b"hello").await;

        let reply = exec(&session, strlen(&session, b"k")).await;
        assert_eq!(expect_int(&reply), 5);
        let reply = exec(&session, strlen(&session, b"missing")).await;
        assert_eq!(expect_int(&reply), 0);

        seed_wrong_type(&session, b"wt").await;
        let err = exec(&session, strlen(&session, b"wt")).await;
        assert!(matches!(err, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn substr_slices() {
        let session = test_session();
        seed_string(&session, b"k", b"hello").await;

        let cases: [(i64, i64, &[u8]); 7] = [
            (0, -1, b"hello"),
            (1, 3, b"ell"),
            (-3, -1, b"llo"),
            (0, 100, b"hello"),
            (10, 20, b""),
            (3, 1, b""),
            (0, -100, b""),
        ];
        for (start, end, want) in cases {
            let reply = exec(&session, substr(&session, b"k", start, end)).await;
            assert_eq!(expect_bulk(&reply), Some(Bytes::copy_from_slice(want)));
        }

        let reply = exec(&session, substr(&session, b"missing", 0, -1)).await;
        assert_eq!(expect_bulk(&reply), Some(Bytes::new()));
    }

    #[tokio::test]
    async fn setnx_sets_only_if_missing() {
        let session = test_session();
        let reply = exec(&session, set_nx(&session, b"k", b"v1")).await;
        assert_eq!(expect_int(&reply), 1);
        let reply = exec(&session, set_nx(&session, b"k", b"v2")).await;
        assert_eq!(expect_int(&reply), 0);
        assert_eq!(stored(&session, b"k").await, Some(b"v1".to_vec()));
    }

    #[tokio::test]
    async fn append_concatenates() {
        let session = test_session();
        let reply = exec(&session, append(&session, b"k", b"Hello")).await;
        assert_eq!(expect_int(&reply), 5);
        let reply = exec(&session, append(&session, b"k", b" World")).await;
        assert_eq!(expect_int(&reply), 11);

        seed_wrong_type(&session, b"wt").await;
        let err = exec_db_err(&session, append(&session, b"wt", b"x")).await;
        assert_eq!(
            err,
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[tokio::test]
    async fn getex_sets_ttl_and_parses_options() {
        let session = test_session();
        seed_string(&session, b"k", b"v").await;

        let reply = exec(
            &session,
            get_ex(
                &session,
                &[
                    Bytes::from_static(b"k"),
                    Bytes::from_static(b"ex"),
                    Bytes::from_static(b"100"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_bulk(&reply), Some(Bytes::from_static(b"v")));
        assert_ne!(stored_ttl(&session, b"k").await, None);

        let err = exec_db_err(
            &session,
            get_ex(
                &session,
                &[Bytes::from_static(b"k"), Bytes::from_static(b"bogus")],
            ),
        )
        .await;
        assert_eq!(err, "ERR syntax error");
        let err = exec_db_err(
            &session,
            get_ex(
                &session,
                &[
                    Bytes::from_static(b"k"),
                    Bytes::from_static(b"px"),
                    Bytes::from_static(b"notanum"),
                ],
            ),
        )
        .await;
        assert_eq!(err, "ERR value is not an integer or out of range");
    }

    #[tokio::test]
    async fn incrbyfloat_increments() {
        let session = test_session();
        let result = exec_db(&session, incr_by_float(&session, b"k", 5.5)).await;
        assert_eq!(result.downcast::<String>().unwrap().as_str(), "5.5");
        let result = exec_db(&session, incr_by_float(&session, b"k", 1.25)).await;
        assert_eq!(result.downcast::<String>().unwrap().as_str(), "6.75");

        seed_string(&session, b"bad", b"x").await;
        let err = exec_db_err(&session, incr_by_float(&session, b"bad", 1.0)).await;
        assert_eq!(err, "ERR value is not a float");

        seed_wrong_type(&session, b"wt").await;
        let err = exec_db_err(&session, incr_by_float(&session, b"wt", 1.0)).await;
        assert_eq!(
            err,
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[tokio::test]
    async fn mset_sets_multiple() {
        let session = test_session();
        exec(
            &session,
            mset(
                &session,
                &[
                    Bytes::from_static(b"k1"),
                    Bytes::from_static(b"v1"),
                    Bytes::from_static(b"k2"),
                    Bytes::from_static(b"v2"),
                ],
            ),
        )
        .await;
        assert_eq!(stored(&session, b"k2").await, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn msetnx_only_all_missing() {
        let session = test_session();
        seed_string(&session, b"existing", b"v").await;

        let reply = exec(
            &session,
            mset_nx(
                &session,
                &[
                    Bytes::from_static(b"existing"),
                    Bytes::from_static(b"x"),
                    Bytes::from_static(b"new"),
                    Bytes::from_static(b"y"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_int(&reply), 0);
        assert_eq!(stored(&session, b"new").await, None);

        let reply = exec(
            &session,
            mset_nx(
                &session,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"2"),
                ],
            ),
        )
        .await;
        assert_eq!(expect_int(&reply), 1);
    }

    #[tokio::test]
    async fn setrange_overwrites() {
        let session = test_session();
        seed_string(&session, b"k", b"Hello World").await;

        let reply = exec(&session, set_range(&session, b"k", 6, b"Redis")).await;
        assert_eq!(expect_int(&reply), 11);
        assert_eq!(stored(&session, b"k").await, Some(b"Hello Redis".to_vec()));

        let reply = exec(&session, set_range(&session, b"new", 3, b"xyz")).await;
        assert_eq!(expect_int(&reply), 6);

        seed_wrong_type(&session, b"wt").await;
        let err = exec_db_err(&session, set_range(&session, b"wt", 0, b"x")).await;
        assert!(err.starts_with(b"WRONGTYPE"));
    }

    #[tokio::test]
    async fn increment_adds_amounts() {
        let session = test_session();
        let reply = exec(&session, increment(&session, b"k", 1)).await;
        assert_eq!(expect_int(&reply), 1);
        let reply = exec(&session, increment(&session, b"k", -1)).await;
        assert_eq!(expect_int(&reply), 0);

        seed_string(&session, b"bad", b"notanumber").await;
        let err = exec_db_err(&session, increment(&session, b"bad", 1)).await;
        assert_eq!(err, "ERR value is not an integer or out of range");

        seed_wrong_type(&session, b"wt").await;
        let err = exec_db_err(&session, increment(&session, b"wt", 1)).await;
        assert_eq!(err, "ERR value is not an integer or out of range");
    }

    #[tokio::test]
    async fn getex_persist_clears_ttl() {
        let session = test_session();
        seed_string(&session, b"k", b"v").await;
        exec(&session, set_ex(&session, b"k", b"v", 100)).await;
        assert_ne!(stored_ttl(&session, b"k").await, None);

        let reply = exec(
            &session,
            get_ex(
                &session,
                &[Bytes::from_static(b"k"), Bytes::from_static(b"persist")],
            ),
        )
        .await;
        assert_eq!(expect_bulk(&reply), Some(Bytes::from_static(b"v")));
        assert_eq!(stored_ttl(&session, b"k").await, None);
    }
}
