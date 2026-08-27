//! Redis keyspace commands: `EXISTS`, `MGET`, `MOVE`, `RENAME`, `RENAMENX`,
//! `EXPIRE`, `TTL`, `PTTL`, `TYPE`, `DEL`/`UNLINK`, and `SCAN`.
//!
//! Port of the Go `redis/keys` package. These commands operate on the public
//! key of whatever value type happens to be stored there: they deliberately
//! do **not** inspect the metadata byte (so `MGET` on a list key returns its
//! raw stored bytes, and `DEL` removes just the public key without cleaning
//! up any internal node keys), mirroring Go. `SCAN` is the exception: it
//! iterates the current DB's public keyspace via a range iterator, filtering
//! out internal (`-`-prefixed) keys.

use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, NoOp, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::pubsub::redis_glob_match;
use crate::resp::RespValue;

/// The current UNIX time in whole seconds, matching Go's `time.Now().Unix()`.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// `EXISTS key [key ...]` — returns the number of given keys that exist.
pub fn exists(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ExistsOp {
            keys: keys.iter().map(|k| session.public_key(k)).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `MGET key [key ...]` — returns the values of the given keys, or null for
/// missing keys. Metadata is not checked.
pub fn mget(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MGetOp {
            keys: keys.iter().map(|k| session.public_key(k)).collect(),
        }),
        wire_op: Box::new(MGetWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `MOVE key db` — moves the key to another DB, returning 1 if moved and 0 if
/// the source key doesn't exist. The target entry preserves the metadata but
/// not any TTL.
pub fn move_op(session: &Session, key: &[u8], target_db: i32) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MoveOp {
            source: session.public_key(key),
            target: session.public_key_for_db(target_db, key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `RENAME key newkey` — renames a key, overwriting any existing newkey.
/// Replies `ERR no such key` when the source is missing.
pub fn rename(session: &Session, old_key: &[u8], new_key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RenameOp {
            old: session.public_key(old_key),
            new: session.public_key(new_key),
        }),
        wire_op: Box::new(RenameWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `RENAMENX key newkey` — renames a key only if newkey doesn't exist,
/// returning 1 on success and 0 if newkey already exists. Replies a bare
/// `no such key` when the source is missing.
pub fn rename_nx(session: &Session, old_key: &[u8], new_key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RenameNXOp {
            old: session.public_key(old_key),
            new: session.public_key(new_key),
        }),
        wire_op: Box::new(RenameNXWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `EXPIRE key seconds` — sets a TTL in seconds on a key, returning 1 if the
/// key exists and 0 otherwise.
pub fn expire(session: &Session, key: &[u8], seconds: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ExpireOp {
            key: session.public_key(key),
            seconds,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `PEXPIRE key milliseconds` — sets a TTL in milliseconds on a key, returning
/// 1 if the key exists and 0 otherwise.
pub fn pexpire(session: &Session, key: &[u8], milliseconds: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PExpireOp {
            key: session.public_key(key),
            milliseconds,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `PERSIST key` — removes any existing TTL, returning 1 if the key exists and
/// had an expiry, 0 otherwise.
pub fn persist(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PersistOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `TTL key` — returns the remaining time to live in seconds: `-2` if the
/// key is missing, `-1` if it exists without an expiry.
pub fn ttl(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(TtlOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `PTTL key` — returns the remaining time to live in milliseconds: `-2` if
/// the key is missing, `-1` if it exists without an expiry.
pub fn pttl(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PTtlOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `TYPE key` — returns the type of the value stored at key.
pub fn key_type(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(TypeOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(TypeWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `DEL key [key ...]` / `UNLINK key [key ...]` — removes the given keys,
/// returning the number removed. Only the public key is deleted; internal
/// node keys of compound values are not cleaned up (mirroring Go).
pub fn del(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(DelOp {
            keys: keys.iter().map(|k| session.public_key(k)).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` — iterates the
/// public keys of the current DB. A `0` cursor starts the iteration; each
/// reply carries the next cursor, which must be passed back on the following
/// call, and a `0` cursor when the iteration is complete.
///
/// Every call executes against a fresh snapshot (`is_mutating: false`), so
/// keys written after a given page may or may not show up in later pages —
/// matching Redis's guarantee that the full iteration reflects "mostly" the
/// state at the start. Internal (`-`-prefixed) keys are never returned.
/// `MATCH` applies Redis globbing; `COUNT` is a page-size hint; `TYPE` filters
/// on the value type (an unknown type name matches nothing).
pub fn scan(
    session: &Session,
    cursor: &[u8],
    count: usize,
    pattern: Option<Vec<u8>>,
    type_filter: Option<u8>,
) -> QueuedOp {
    let cursor = if cursor == b"0" {
        Vec::new()
    } else {
        cursor.to_vec()
    };
    QueuedOp {
        db_op: Box::new(ScanOp {
            prefix: session.prefix(),
            cursor,
            count,
            pattern,
            type_filter,
        }),
        wire_op: Box::new(ScanWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct NullWire;

impl WireOp for NullWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        RespValue::Array(None)
    }
}

/// Returns the idle time of a key
/// Invar doesn't currently have any bookkeeping mechanism to track
/// this, but it's implemented to always nil to avoid breaking
/// upstream applications which expect command support
pub fn idle_time(_: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(NullWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// --- DbOp halves ---

struct ExistsOp {
    keys: Vec<Vec<u8>>,
}

impl DbOp for ExistsOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            let mut count = 0i64;
            for key in &keys {
                match tx.get(key).await {
                    Ok(_) => count += 1,
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

struct MGetOp {
    keys: Vec<Vec<u8>>,
}

impl DbOp for MGetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in &keys {
                match tx.get(key).await {
                    Ok(item) => values.push(Some(item.value().to_vec())),
                    Err(KvError::KeyNotFound) => values.push(None),
                    Err(e) => return Err(e.into()),
                }
            }
            let result: DbResult = Box::new(values);
            Ok(result)
        })
    }
}

struct MoveOp {
    source: Vec<u8>,
    target: Vec<u8>,
}

impl DbOp for MoveOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let source = self.source.clone();
        let target = self.target.clone();
        Box::pin(async move {
            let item = match tx.get(&source).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            tx.set(Entry::new(target, item.value().to_vec()).metadata(item.metadata()))?;
            tx.delete(&source)?;
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct RenameOp {
    old: Vec<u8>,
    new: Vec<u8>,
}

impl DbOp for RenameOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let old = self.old.clone();
        let new = self.new.clone();
        Box::pin(async move {
            let item = match tx.get(&old).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<()>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            tx.set(Entry::new(new, item.value().to_vec()).metadata(item.metadata()))?;
            tx.delete(&old)?;
            let result: DbResult = Box::new(Some(()));
            Ok(result)
        })
    }
}

struct RenameNXOp {
    old: Vec<u8>,
    new: Vec<u8>,
}

impl DbOp for RenameNXOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let old = self.old.clone();
        let new = self.new.clone();
        Box::pin(async move {
            match tx.get(&new).await {
                Ok(_) => {
                    let result: DbResult = Box::new(Some(0i64));
                    return Ok(result);
                }
                Err(KvError::KeyNotFound) => {}
                Err(e) => return Err(e.into()),
            }
            let item = match tx.get(&old).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<i64>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            tx.set(Entry::new(new, item.value().to_vec()).metadata(item.metadata()))?;
            tx.delete(&old)?;
            let result: DbResult = Box::new(Some(1i64));
            Ok(result)
        })
    }
}

struct ExpireOp {
    key: Vec<u8>,
    seconds: i64,
}

impl DbOp for ExpireOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let seconds = self.seconds;
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            let ttl = u64::try_from(seconds)
                .ok()
                .map(std::time::Duration::from_secs)
                .unwrap_or(std::time::Duration::ZERO);
            let entry = Entry::new(key, item.value().to_vec())
                .metadata(item.metadata())
                .ttl(ttl);
            tx.set(entry)?;
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct PExpireOp {
    key: Vec<u8>,
    milliseconds: i64,
}

impl DbOp for PExpireOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let milliseconds = self.milliseconds;
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            let ttl = u64::try_from(milliseconds)
                .ok()
                .map(std::time::Duration::from_millis)
                .unwrap_or(std::time::Duration::ZERO);
            let entry = Entry::new(key, item.value().to_vec())
                .metadata(item.metadata())
                .ttl(ttl);
            tx.set(entry)?;
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct PersistOp {
    key: Vec<u8>,
}

impl DbOp for PersistOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            if item.expires_at() == 0 {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }
            let entry = Entry::new(key, item.value().to_vec())
                .metadata(item.metadata());
            tx.set(entry)?;
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

struct TtlOp {
    key: Vec<u8>,
}

impl DbOp for TtlOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let result = ttl_value(tx, &key).await?.unwrap_or(-2);
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

struct PTtlOp {
    key: Vec<u8>,
}

impl DbOp for PTtlOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let result = ttl_value(tx, &key).await?.map_or(-2, |secs| secs * 1000);
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// Returns the remaining TTL of the key in whole seconds, `None` if missing.
/// A present key without an expiry (or already past its expiry) yields `-1`.
async fn ttl_value(tx: &dyn Tx, key: &[u8]) -> Result<Option<i64>, DbError> {
    let item = match tx.get(key).await {
        Ok(item) => item,
        Err(KvError::KeyNotFound) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let expires_at = item.expires_at();
    let now = now_secs();
    if expires_at == 0 || expires_at <= now {
        return Ok(Some(-1));
    }
    Ok(Some((expires_at - now) as i64))
}

struct TypeOp {
    key: Vec<u8>,
}

impl DbOp for TypeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let name = match tx.get(&key).await {
                Ok(item) => type_name(item.metadata()).to_string(),
                Err(KvError::KeyNotFound) => "none".to_string(),
                Err(e) => return Err(e.into()),
            };
            let result: DbResult = Box::new(name);
            Ok(result)
        })
    }
}

struct DelOp {
    keys: Vec<Vec<u8>>,
}

impl DbOp for DelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            let mut count = 0i64;
            for key in &keys {
                match tx.get(key).await {
                    Err(KvError::KeyNotFound) => continue,
                    Err(e) => return Err(e.into()),
                    Ok(_) => {}
                }
                tx.delete(key)?;
                count += 1;
            }
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

/// The result of a `SCAN` page: the next cursor (`Vec::new()` means the
/// iteration is complete, i.e. cursor `0`) and the public key names returned.
struct ScanResult {
    cursor: Vec<u8>,
    keys: Vec<Vec<u8>>,
}

struct ScanOp {
    prefix: Vec<u8>,
    cursor: Vec<u8>,
    count: usize,
    pattern: Option<Vec<u8>>,
    type_filter: Option<u8>,
}

impl DbOp for ScanOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let prefix = self.prefix.clone();
        let cursor = self.cursor.clone();
        let count = self.count.max(1);
        let pattern = self.pattern.clone();
        let type_filter = self.type_filter;
        Box::pin(async move {
            let mut start = prefix.clone();
            if !cursor.is_empty() {
                start.extend_from_slice(&cursor);
            }
            let mut it = tx
                .new_range_iterator(
                    if cursor.is_empty() {
                        Bound::Included(prefix.as_slice())
                    } else {
                        Bound::Excluded(start.as_slice())
                    },
                    Bound::Unbounded,
                )
                .await?;

            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut last_seen: Vec<u8> = Vec::new();
            let mut exhausted = false;
            while keys.len() < count {
                if !it.next().await {
                    exhausted = true;
                    break;
                }
                let item = match it.item() {
                    Some(item) => item,
                    None => {
                        exhausted = true;
                        break;
                    }
                };
                let full = item.key();
                if !full.starts_with(prefix.as_slice()) {
                    exhausted = true;
                    break;
                }
                let name = &full[prefix.len()..];
                last_seen = name.to_vec();
                if let Some(pattern) = &pattern {
                    if !redis_glob_match(pattern, name, false) {
                        continue;
                    }
                }
                if let Some(want) = type_filter {
                    if item.metadata() != want {
                        continue;
                    }
                }
                keys.push(name.to_vec());
            }

            if !exhausted && keys.len() == count && (!it.next().await || it.item().is_none_or(|i| !i.key().starts_with(prefix.as_slice()))) {
                    exhausted = true;
            }

            // When the page ended without exhausting the keyspace, resume from
            // the last key returned (or the last key examined, when the page
            // contained no matches) so filtered-out keys are not revisited.
            let cursor = if exhausted {
                Vec::new()
            } else {
                keys.last().cloned().unwrap_or(last_seen)
            };
            let result: DbResult = Box::new(ScanResult { cursor, keys });
            Ok(result)
        })
    }
}

/// The Redis type name for a metadata byte, mirroring Go's `typeName`.
fn type_name(meta: u8) -> &'static str {
    match meta {
        b if b == ValueType::String as u8 => "string",
        b if b == ValueType::List as u8 => "list",
        b if b == ValueType::Set as u8 => "set",
        b if b == ValueType::SortedSet as u8 => "zset",
        b if b == ValueType::Hash as u8 => "hash",
        b if b == ValueType::Stream as u8 => "stream",
        b if b == ValueType::VectorSet as u8 => "vectorset",
        b if b == ValueType::Bloom as u8 => "bloom",
        b if b == ValueType::Json as u8 => "json",
        _ => "unknown",
    }
}

/// The metadata byte for a Redis type name, or `None` for names not produced
/// by [`type_name`] (a `SCAN ... TYPE` filter with such a name matches
/// nothing, matching Redis).
pub fn type_byte(name: &[u8]) -> Option<u8> {
    Some(match name {
        b"string" => ValueType::String as u8,
        b"list" => ValueType::List as u8,
        b"set" => ValueType::Set as u8,
        b"zset" => ValueType::SortedSet as u8,
        b"hash" => ValueType::Hash as u8,
        b"stream" => ValueType::Stream as u8,
        b"vectorset" => ValueType::VectorSet as u8,
        b"bloom" => ValueType::Bloom as u8,
        b"json" => ValueType::Json as u8,
        _ => return None,
    })
}

// --- WireOp halves ---

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

/// Replies an array of bulk-or-null values (`MGET`).
struct MGetWire;

impl WireOp for MGetWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Option<Vec<u8>>>>() {
                Ok(boxed) => RespValue::Array(Some(
                    boxed
                        .iter()
                        .map(|v| {
                            RespValue::BulkString(v.as_ref().map(|b| Bytes::copy_from_slice(b)))
                        })
                        .collect(),
                )),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad bulk result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `+OK`, or `ERR no such key` when the source key is missing.
struct RenameWire;

impl WireOp for RenameWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<()>>() {
                Ok(boxed) => match *boxed {
                    Some(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
                    None => RespValue::Error(Bytes::from_static(b"ERR no such key")),
                },
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad rename result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an integer, or a bare `no such key` when the source key is
/// missing (matching Go, which omits the `ERR ` prefix here).
struct RenameNXWire;

impl WireOp for RenameNXWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<i64>>() {
                Ok(boxed) => match *boxed {
                    Some(n) => RespValue::Integer(n),
                    None => RespValue::Error(Bytes::from_static(b"no such key")),
                },
                Err(_) => RespValue::Error(Bytes::from_static(
                    b"ERR internal error: bad renamenx result",
                )),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a simple string type name (`TYPE`).
struct TypeWire;

impl WireOp for TypeWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<String>() {
                Ok(name) => RespValue::SimpleString(Bytes::copy_from_slice(name.as_bytes())),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad type result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies the two-element `SCAN` array: `[next cursor, [key ...]]`.
struct ScanWire;

impl WireOp for ScanWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<ScanResult>() {
                Ok(boxed) => {
                    let cursor = if boxed.cursor.is_empty() {
                        Bytes::from_static(b"0")
                    } else {
                        Bytes::copy_from_slice(&boxed.cursor)
                    };
                    let keys = boxed
                        .keys
                        .iter()
                        .map(|k| RespValue::BulkString(Some(Bytes::copy_from_slice(k))))
                        .collect();
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(cursor)),
                        RespValue::Array(Some(keys)),
                    ]))
                }
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad scan result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// Seeds a string value directly under the public key.
    async fn seed_string(session: &Session, key: &[u8], val: &[u8]) {
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(Entry::new(session.public_key(key), val.to_vec()).metadata(ValueType::String as u8))
            .expect("seed");
        tx.commit().await.expect("commit");
    }

    /// Reads the stored public key via a fresh session on the given DB.
    async fn stored(session: &Session, db: i32, key: &[u8]) -> Option<Vec<u8>> {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        match tx.get(&session.public_key_for_db(db, key)).await {
            Ok(item) => Some(item.value().to_vec()),
            Err(KvError::KeyNotFound) => None,
            Err(e) => panic!("get failed: {e:?}"),
        }
    }

    fn expect_int(reply: &RespValue) -> i64 {
        match reply {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_bulk_array(reply: &RespValue) -> Vec<Option<Bytes>> {
        match reply {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::BulkString(b) => b.clone(),
                    other => panic!("expected bulk element, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exists_counts_keys() {
        let session = test_session();
        seed_string(&session, b"k1", b"v1").await;
        seed_string(&session, b"k2", b"v2").await;

        let keys = [Bytes::from_static(b"missing")];
        let reply = exec(&session, exists(&session, &keys)).await;
        assert_eq!(expect_int(&reply), 0);

        let keys = [
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"missing"),
            Bytes::from_static(b"k2"),
        ];
        let reply = exec(&session, exists(&session, &keys)).await;
        assert_eq!(expect_int(&reply), 2);
    }

    #[tokio::test]
    async fn mget_returns_values_and_null() {
        let session = test_session();
        seed_string(&session, b"k1", b"v1").await;
        seed_string(&session, b"k2", b"").await;

        let keys = [
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"missing"),
            Bytes::from_static(b"k2"),
        ];
        let reply = exec(&session, mget(&session, &keys)).await;
        let vals = expect_bulk_array(&reply);
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0].as_deref(), Some(b"v1".as_slice()));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2].as_deref(), Some(b"".as_slice()));
    }

    #[tokio::test]
    async fn move_moves_key_between_dbs() {
        let session = test_session();
        seed_string(&session, b"k1", b"v1").await;

        let reply = exec(&session, move_op(&session, b"k1", 5)).await;
        assert_eq!(expect_int(&reply), 1);
        assert_eq!(stored(&session, 0, b"k1").await, None);
        assert_eq!(stored(&session, 5, b"k1").await, Some(b"v1".to_vec()));

        let reply = exec(&session, move_op(&session, b"missing", 5)).await;
        assert_eq!(expect_int(&reply), 0);
    }

    #[tokio::test]
    async fn rename_renames_and_overwrites() {
        let session = test_session();
        seed_string(&session, b"old", b"value").await;

        let reply = exec(&session, rename(&session, b"old", b"new")).await;
        assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(stored(&session, 0, b"new").await, Some(b"value".to_vec()));
        assert_eq!(stored(&session, 0, b"old").await, None);

        let reply = exec(&session, rename(&session, b"missing", b"other")).await;
        assert_eq!(
            reply,
            RespValue::Error(Bytes::from_static(b"ERR no such key"))
        );
    }

    #[tokio::test]
    async fn renamenx_respects_existing_target() {
        let session = test_session();
        seed_string(&session, b"old", b"value").await;
        seed_string(&session, b"new", b"existing").await;

        let reply = exec(&session, rename_nx(&session, b"old", b"new")).await;
        assert_eq!(expect_int(&reply), 0);

        let reply = exec(&session, rename_nx(&session, b"old", b"target")).await;
        assert_eq!(expect_int(&reply), 1);
        assert_eq!(
            stored(&session, 0, b"target").await,
            Some(b"value".to_vec())
        );

        let reply = exec(&session, rename_nx(&session, b"missing", b"fresh-target")).await;
        assert_eq!(reply, RespValue::Error(Bytes::from_static(b"no such key")));
    }

    #[tokio::test]
    async fn expire_and_ttl() {
        let session = test_session();
        seed_string(&session, b"k", b"v").await;
        seed_string(&session, b"k2", b"v").await;

        let reply = exec(&session, expire(&session, b"k", 100)).await;
        assert_eq!(expect_int(&reply), 1);

        let result = exec_db(&session, ttl(&session, b"k")).await;
        let ttl_secs = result.downcast::<i64>().unwrap();
        assert!(*ttl_secs > 0 && *ttl_secs <= 100);

        let result = exec_db(&session, pttl(&session, b"k")).await;
        let pttl_ms = result.downcast::<i64>().unwrap();
        assert!(*pttl_ms > 0 && *pttl_ms <= 100_000);

        let result = exec_db(&session, ttl(&session, b"k2")).await;
        assert_eq!(*result.downcast::<i64>().unwrap(), -1);

        let result = exec_db(&session, ttl(&session, b"missing")).await;
        assert_eq!(*result.downcast::<i64>().unwrap(), -2);

        let result = exec_db(&session, pttl(&session, b"missing")).await;
        assert_eq!(*result.downcast::<i64>().unwrap(), -2);

        let reply = exec(&session, expire(&session, b"missing", 100)).await;
        assert_eq!(expect_int(&reply), 0);
    }

    #[tokio::test]
    async fn type_reports_type_names() {
        let session = test_session();
        let types: &[(u8, &str)] = &[
            (ValueType::String as u8, "string"),
            (ValueType::List as u8, "list"),
            (ValueType::Set as u8, "set"),
            (ValueType::SortedSet as u8, "zset"),
            (ValueType::Hash as u8, "hash"),
            (ValueType::Stream as u8, "stream"),
            (ValueType::VectorSet as u8, "vectorset"),
            (ValueType::Bloom as u8, "bloom"),
            (ValueType::Json as u8, "json"),
            (0xFF, "unknown"),
        ];
        for (i, (meta, want)) in types.iter().enumerate() {
            let key = format!("key{i}");
            let store = session.store();
            let tx = store.begin(true).await.expect("tx");
            tx.set(Entry::new(session.public_key(key.as_bytes()), b"v".to_vec()).metadata(*meta))
                .expect("seed");
            tx.commit().await.expect("commit");

            let reply = exec(&session, key_type(&session, key.as_bytes())).await;
            assert_eq!(
                reply,
                RespValue::SimpleString(Bytes::copy_from_slice(want.as_bytes())),
                "type for metadata {meta:#x}"
            );
        }

        let reply = exec(&session, key_type(&session, b"missing")).await;
        assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"none")));
    }

    #[tokio::test]
    async fn del_removes_keys() {
        let session = test_session();
        seed_string(&session, b"k1", b"v1").await;
        seed_string(&session, b"k2", b"v2").await;

        let keys = [
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"missing"),
            Bytes::from_static(b"k2"),
        ];
        let reply = exec(&session, del(&session, &keys)).await;
        assert_eq!(expect_int(&reply), 2);

        let keys = [Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let reply = exec(&session, exists(&session, &keys)).await;
        assert_eq!(expect_int(&reply), 0);
    }

    /// Runs a SCAN op and extracts `(cursor, keys)` from the reply.
    async fn run_scan(
        session: &Session,
        cursor: &[u8],
        count: usize,
        pattern: Option<&[u8]>,
    ) -> (Vec<u8>, Vec<Vec<u8>>) {
        let reply = exec(
            session,
            scan(session, cursor, count, pattern.map(|p| p.to_vec()), None),
        )
        .await;
        match reply {
            RespValue::Array(Some(items)) if items.len() == 2 => {
                let cursor = match &items[0] {
                    RespValue::BulkString(Some(c)) => c.to_vec(),
                    other => panic!("expected bulk cursor, got {other:?}"),
                };
                let keys = match &items[1] {
                    RespValue::Array(Some(keys)) => keys
                        .iter()
                        .map(|k| match k {
                            RespValue::BulkString(Some(k)) => k.to_vec(),
                            other => panic!("expected bulk key, got {other:?}"),
                        })
                        .collect(),
                    other => panic!("expected key array, got {other:?}"),
                };
                (cursor, keys)
            }
            other => panic!("expected scan array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scan_empty_keyspace_returns_zero_cursor() {
        let session = test_session();
        let (cursor, keys) = run_scan(&session, b"0", 10, None).await;
        assert_eq!(cursor, b"0".to_vec());
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn scan_returns_all_public_keys() {
        let session = test_session();
        for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            seed_string(&session, k, b"v").await;
        }

        let (cursor, keys) = run_scan(&session, b"0", 10, None).await;
        assert_eq!(cursor, b"0".to_vec());
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    }

    #[tokio::test]
    async fn scan_paginates_with_cursor() {
        let session = test_session();
        for k in [b"k1".as_slice(), b"k2", b"k3"] {
            seed_string(&session, k, b"v").await;
        }

        let (cursor, keys) = run_scan(&session, b"0", 2, None).await;
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert_eq!(cursor, b"k2".to_vec());

        let (cursor, keys) = run_scan(&session, &cursor, 2, None).await;
        assert_eq!(keys, vec![b"k3".to_vec()]);
        assert_eq!(cursor, b"0".to_vec());
    }

    #[tokio::test]
    async fn scan_hides_internal_keys() {
        let session = test_session();
        seed_string(&session, b"pub", b"v").await;

        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(Entry::new(b"-0:internal".to_vec(), b"v".to_vec()).metadata(ValueType::String as u8))
            .expect("seed internal");
        tx.commit().await.expect("commit");

        let (cursor, keys) = run_scan(&session, b"0", 10, None).await;
        assert_eq!(cursor, b"0".to_vec());
        assert_eq!(keys, vec![b"pub".to_vec()]);
    }

    #[tokio::test]
    async fn scan_matches_pattern() {
        let session = test_session();
        for k in [b"foo1".as_slice(), b"bar", b"foo2"] {
            seed_string(&session, k, b"v").await;
        }

        let (cursor, keys) = run_scan(&session, b"0", 10, Some(b"foo*")).await;
        assert_eq!(cursor, b"0".to_vec());
        assert_eq!(keys, vec![b"foo1".to_vec(), b"foo2".to_vec()]);
    }

    #[tokio::test]
    async fn scan_filters_by_type() {
        let session = test_session();
        seed_string(&session, b"s", b"v").await;
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(
            Entry::new(session.public_key(b"l"), b"v".to_vec()).metadata(ValueType::List as u8),
        )
        .expect("seed list");
        tx.commit().await.expect("commit");

        let reply = exec(
            &session,
            scan(&session, b"0", 10, None, Some(ValueType::String as u8)),
        )
        .await;
        let (cursor, keys) = match reply {
            RespValue::Array(Some(items)) if items.len() == 2 => match &items[1] {
                RespValue::Array(Some(keys)) => (
                    match &items[0] {
                        RespValue::BulkString(Some(c)) => c.to_vec(),
                        other => panic!("expected bulk cursor, got {other:?}"),
                    },
                    keys.iter()
                        .map(|k| match k {
                            RespValue::BulkString(Some(k)) => k.to_vec(),
                            other => panic!("expected bulk key, got {other:?}"),
                        })
                        .collect::<Vec<Vec<u8>>>(),
                ),
                other => panic!("expected key array, got {other:?}"),
            },
            other => panic!("expected scan array, got {other:?}"),
        };
        assert_eq!(cursor, b"0".to_vec());
        assert_eq!(keys, vec![b"s".to_vec()]);
    }
}
