//! Redis keyspace commands: `EXISTS`, `MGET`, `MOVE`, `RENAME`, `RENAMENX`,
//! `EXPIRE`, `TTL`, `PTTL`, `TYPE`, `DEL`/`UNLINK`, and `SCAN`.
//!
//! Port of the Go `redis/keys` package. Most commands operate on the public
//! key of whatever value type happens to be stored there and deliberately do
//! **not** inspect the metadata byte (so `MGET` on a list key returns its raw
//! stored bytes), mirroring Go. `DEL`/`UNLINK` are the exception: real Redis
//! frees the internal node keys of compound values when a key is removed, so
//! `DEL` also clears the field/member/score index entries owned by a set,
//! hash, or sorted set (see [`clear_compound_entries`]). `SCAN` iterates the
//! current DB's public keyspace via a range iterator, filtering out internal
//! (`-`-prefixed) keys.

use std::ops::Bound;

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Item, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, NoOp, QueuedOp, WireOp};
use smallvec::smallvec;
use crate::common::session::Session;
use crate::common::namespace::{self, base_type, encode_sentinel, prefix_of, with_db, ElementPrefix};
use crate::common::ValueType;
use crate::list;
use crate::pattern::{GlobPattern, PatternMatcher};
use crate::resp::RespValue;

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
        keys: Some(session.keys_sv(keys)),
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
        keys: Some(session.keys_sv(keys)),
    }
}

/// `MOVE key db` — moves the key to another DB with its TTL, returning 1 if
/// moved and 0 if the key is missing in this DB or already exists in the
/// target. Moving to the current DB is an error, as in Redis.
///
/// The elements of a compound value are addressed by a prefix that contains
/// its DB number, so they are re-keyed too: O(size of the value).
pub fn move_op(session: &Session, key: &[u8], target_db: i32) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MoveOp {
            source: session.public_key(key),
            target: session.public_key_for_db(target_db, key),
            target_db,
            same_db: target_db == session.current_db(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
        keys: Some(session.key_sv(key)),
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
        keys: Some(smallvec![String::from_utf8_lossy(&session.public_key(old_key)).into_owned(), String::from_utf8_lossy(&session.public_key(new_key)).into_owned()]),
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
        keys: Some(smallvec![String::from_utf8_lossy(&session.public_key(old_key)).into_owned(), String::from_utf8_lossy(&session.public_key(new_key)).into_owned()]),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
    }
}

/// `DEL key [key ...]` / `UNLINK key [key ...]` — removes the given keys,
/// returning the number removed. Compound values (sets, hashes, sorted sets,
/// lists, streams, bloom filters) have their internal entries cleared too, so
/// a later re-creation of the same key starts from a clean slate. As in
/// Redis, that makes the cost proportional to the size of the value.
pub fn del(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(DelOp {
            keys: keys
                .iter()
                .map(|k| (session.public_key(k), session.private_key(k)))
                .collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
        keys: Some(session.keys_sv(keys)),
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
        keys: None,
    }
}

/// `KEYS pattern` — every key of the current DB whose name matches the glob
/// `pattern`, in no particular order (here, byte order).
///
/// Like Redis, this walks the whole keyspace of the DB, so its cost is
/// proportional to the number of keys; it is meant for tests, debugging and
/// tools, not for hot paths (use `SCAN`). Only public keys are listed, so a
/// compound value appears once under its name and its internal entries
/// (`-`-prefixed) never do. Expired keys that have not been reaped yet are
/// skipped.
pub fn keys(session: &Session, pattern: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(KeysOp {
            prefix: session.prefix(),
            pattern: pattern.to_vec(),
        }),
        wire_op: Box::new(KeysWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
        keys: None,
    }
}

struct KeysOp {
    prefix: Vec<u8>,
    pattern: Vec<u8>,
}

impl DbOp for KeysOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            let matcher = GlobPattern::keyspace(self.pattern.clone());
            let mut it = tx.new_prefix_iterator(&self.prefix).await?;
            let mut names: Vec<Vec<u8>> = Vec::new();
            while it.next().await {
                let Some(item) = it.item() else { break };
                if item.is_expired() {
                    continue;
                }
                let name = &item.key()[self.prefix.len()..];
                if matcher.is_match(name) {
                    names.push(name.to_vec());
                }
            }
            let err = it.err().cloned();
            it.close().await?;
            if let Some(e) = err {
                return Err(DbError::Kv(e));
            }
            let result: DbResult = Box::new(names);
            Ok(result)
        })
    }
}

/// Replies an array of bulk strings from a `Vec<Vec<u8>>`.
struct KeysWire;

impl WireOp for KeysWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Vec<u8>>>() {
                Ok(names) => RespValue::Array(Some(
                    names
                        .iter()
                        .map(|n| RespValue::BulkString(Some(Bytes::copy_from_slice(n))))
                        .collect(),
                )),
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error: bad keys result")),
            },
            Err(e) => err_resp(&e),
        }
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
        keys: None,
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
    target_db: i32,
    same_db: bool,
}

impl DbOp for MoveOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let source = self.source.clone();
        let target = self.target.clone();
        let (target_db, same_db) = (self.target_db, self.same_db);
        Box::pin(async move {
            if same_db {
                return Err(DbError::Redis("source and destination objects are the same".into()));
            }
            let item = match tx.get(&source).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            match tx.get(&target).await {
                Ok(_) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(KvError::KeyNotFound) => {}
                Err(e) => return Err(e.into()),
            }
            let corrupt = || DbError::Redis("internal error: malformed element prefix".into());
            let legacy = |public: &[u8]| [b"-".as_slice(), public].concat();
            let from = prefix_of(&item, &legacy(&source))?;
            let to = if from.is_explicit() {
                ElementPrefix::explicit(with_db(&from, target_db).ok_or_else(corrupt)?)
            } else {
                ElementPrefix::legacy(legacy(&target))
            };
            move_elements(tx, &source, base_type(item.metadata()), &from, &to).await?;
            let (value, metadata) = if to.is_explicit() {
                encode_sentinel(namespace::payload(&item)?.to_vec(), base_type(item.metadata()), &to)
            } else {
                (item.value().to_vec(), item.metadata())
            };
            tx.set(Entry::new(target, value).metadata(metadata).keep_ttl_of(&item))?;
            tx.delete(&source)?;
            let result: DbResult = Box::new(1i64);
            Ok(result)
        })
    }
}

/// Moves the key `old` (already read as `item`) to `new` for `RENAME`:
/// the destination's own elements are cleared, the elements follow if they
/// are addressed by the old name, and the public entry moves with its
/// remaining TTL. `RENAME k k` is a no-op.
///
/// Renaming is O(1) whatever the layout, because the elements never move. A
/// value whose sentinel records a namespace just has the sentinel copied. A
/// legacy value's elements are addressed by its old name (`-<db>:<old>...`),
/// so the new sentinel records that prefix as its namespace; nothing else is
/// rewritten, and a key later created under the old name gets a namespace of
/// its own instead of colliding with them.
async fn rename_entry(tx: &dyn Tx, old: &[u8], new: &[u8], item: &Item) -> Result<(), DbError> {
    if old == new {
        return Ok(());
    }
    let legacy = |public: &[u8]| [b"-".as_slice(), public].concat();

    match tx.get(new).await {
        Ok(existing) => {
            let prefix = prefix_of(&existing, &legacy(new))?;
            clear_compound_entries(tx, new, &prefix, existing.metadata()).await?;
        }
        Err(KvError::KeyNotFound) => {}
        Err(e) => return Err(e.into()),
    }

    let source = prefix_of(item, &legacy(old))?;
    let (value, metadata) = if source.is_explicit() || !has_elements(base_type(item.metadata())) {
        (item.value().to_vec(), item.metadata())
    } else {
        encode_sentinel(item.value().to_vec(), item.metadata(), &ElementPrefix::explicit(source.to_vec()))
    };
    tx.set(Entry::new(new.to_vec(), value).metadata(metadata).keep_ttl_of(item))?;
    tx.delete(old)?;
    Ok(())
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
            rename_entry(tx, &old, &new, &item).await?;
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
            rename_entry(tx, &old, &new, &item).await?;
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
            if item.ttl().is_some_and(|ttl| ttl.is_zero()) {
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

enum TimeUnit {
    Seconds,
    Millis,
}

struct TtlOp {
    key: Vec<u8>,
}

impl DbOp for TtlOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let result = ttl_value(tx, &key, TimeUnit::Seconds).await;
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
            let result = ttl_value(tx, &key, TimeUnit::Millis).await;
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

/// Returns the remaining TTL of the key in milliseconds, `None` if missing.
/// A present key without an expiry yields the `-1` sentinel; a key past its
/// expiry yields `-2` (mirroring Redis, where an expired key reports as
/// missing).
async fn ttl_value(tx: &dyn Tx, key: &[u8], time_unit: TimeUnit) -> i64 {
    match tx.get(key).await {
        Ok(item) => {
            if let Some(ttl) = item.ttl() {
                match time_unit {
                    TimeUnit::Seconds => ttl.as_secs() as i64,
                    TimeUnit::Millis => ttl.as_millis() as i64,
                }
            } else {
                -1
            }
        },
        _ => -2,
    }
}

struct TypeOp {
    key: Vec<u8>,
}

impl DbOp for TypeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let name = match tx.get(&key).await {
                Ok(item) => type_name(base_type(item.metadata())).to_string(),
                Err(KvError::KeyNotFound) => "none".to_string(),
                Err(e) => return Err(e.into()),
            };
            let result: DbResult = Box::new(name);
            Ok(result)
        })
    }
}

struct DelOp {
    keys: Vec<(Vec<u8>, Vec<u8>)>,
}

impl DbOp for DelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            let mut count = 0i64;
            for (public_key, legacy_prefix) in &keys {
                let item = match tx.get(public_key).await {
                    Err(KvError::KeyNotFound) => continue,
                    Err(e) => return Err(e.into()),
                    Ok(item) => item,
                };
                let prefix = prefix_of(&item, legacy_prefix)?;
                clear_compound_entries(tx, public_key, &prefix, item.metadata()).await?;
                tx.delete(public_key)?;
                count += 1;
            }
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

/// The prefixes, under a value's element prefix, that its element entries
/// live under, for the types that keep the elements as one family of keys.
/// Lists are not covered: their nodes are found by walking the chain.
fn element_prefixes(base_type: u8, prefix: &[u8]) -> Vec<Vec<u8>> {
    if base_type == ValueType::Set as u8
        || base_type == ValueType::Hash as u8
        || base_type == ValueType::Stream as u8
        || base_type == ValueType::Bloom as u8
    {
        vec![[prefix, b"\0"].concat()]
    } else if base_type == ValueType::SortedSet as u8 {
        vec![[prefix, b":score:"].concat(), [prefix, b":member:"].concat()]
    } else {
        Vec::new()
    }
}

/// Whether values of this type keep their data as separate element entries
/// (strings, HyperLogLogs and JSON documents live in the public entry itself).
fn has_elements(base_type: u8) -> bool {
    [ValueType::List, ValueType::Set, ValueType::SortedSet, ValueType::Hash, ValueType::Stream, ValueType::Bloom]
        .iter()
        .any(|t| *t as u8 == base_type)
}

/// Re-keys every entry under `from` to live under `to` instead (keeping the
/// part of the key after the prefix, the value and the metadata byte).
async fn move_prefix(tx: &dyn Tx, from: &[u8], to: &[u8]) -> Result<(), DbError> {
    let mut it = tx.new_prefix_iterator(from).await?;
    let mut moved = Vec::new();
    while it.next().await {
        if let Some(item) = it.item() {
            moved.push((item.key().to_vec(), item.value().to_vec(), item.metadata()));
        }
    }
    let err = it.err().cloned();
    if let Some(e) = err {
        it.close().await?;
        return Err(DbError::Kv(e));
    }
    it.close().await?;
    for (key, value, metadata) in moved {
        let mut new_key = to.to_vec();
        new_key.extend_from_slice(&key[from.len()..]);
        tx.set(Entry::new(new_key, value).metadata(metadata))?;
        tx.delete(&key)?;
    }
    Ok(())
}

/// Moves the elements of the compound value whose sentinel is at
/// `public_key` from element prefix `from` to element prefix `to`. O(size of
/// the value). Does nothing for types that keep no elements.
pub(crate) async fn move_elements(
    tx: &dyn Tx,
    public_key: &[u8],
    base_type: u8,
    from: &[u8],
    to: &[u8],
) -> Result<(), DbError> {
    if base_type == ValueType::List as u8 {
        return list::move_nodes(tx, public_key, from, to).await;
    }
    for (from, to) in element_prefixes(base_type, from).into_iter().zip(element_prefixes(base_type, to)) {
        move_prefix(tx, &from, &to).await?;
    }
    Ok(())
}

/// Deletes the internal entry keys owned by a compound value so that
/// recreating the public key later starts from an empty container:
///
/// * Set and hash field/member entries, stream entries and bloom filter pages
///   live under `-<db>:<key>\x00...`.
/// * Sorted-set score index entries live under `-<db>:<key>:score:` and
///   member index entries under `-<db>:<key>:member:`.
/// * List nodes live under `-<db>:<key>:<nodeKey>`; they are found by walking
///   the chain from the sentinel at `public_key`.
///
/// Returns without deleting anything for value types that keep no internal
/// entries (strings, HyperLogLogs, JSON documents).
async fn clear_compound_entries(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &[u8],
    metadata: u8,
) -> Result<(), DbError> {
    let metadata = base_type(metadata);
    if metadata == ValueType::List as u8 {
        return list::delete_nodes(tx, public_key, node_prefix).await;
    }
    for prefix in element_prefixes(metadata, node_prefix) {
        let mut it = tx.new_prefix_iterator(&prefix).await?;
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
    }
    Ok(())
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
        let matcher = self.pattern.clone().map(GlobPattern::keyspace);
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
                if matcher.as_ref().is_some_and(|m| !m.is_match(name)) {
                    continue;
                }
                if let Some(want) = type_filter {
                    if base_type(item.metadata()) != want {
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
    use kv::kv::TxIsolation;
    use crate::testutil::test_session;

    /// Runs one op through its own transaction, committing if it mutates, and
    /// renders the reply.
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating, TxIsolation::Snapshot).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Runs a db op expecting success, returning the raw result.
    async fn exec_db(session: &Session, op: QueuedOp) -> DbResult {
        let store = session.store();
        let tx = store.begin(op.is_mutating, TxIsolation::Snapshot).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await.expect("op failed");
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        outcome
    }

    /// Seeds a string value directly under the public key.
    async fn seed_string(session: &Session, key: &[u8], val: &[u8]) {
        let store = session.store();
        let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
        tx.set(Entry::new(session.public_key(key), val.to_vec()).metadata(ValueType::String as u8))
            .expect("seed");
        tx.commit().await.expect("commit");
    }

    /// Reads the stored public key via a fresh session on the given DB.
    async fn stored(session: &Session, db: i32, key: &[u8]) -> Option<Vec<u8>> {
        let store = session.store();
        let tx = store.begin(false, TxIsolation::Snapshot).await.expect("read tx");
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

    /// Number of internal (`-`- or `+`-prefixed) entries currently in the store.
    async fn internal_entry_count(session: &Session) -> usize {
        // Name-derived prefixes start with `-`, namespaces with `+`.
        crate::testutil::entries_under(session, b"-").await + crate::testutil::entries_under(session, b"+").await
    }

    /// Builds a value of each compound type under `key`, so tests can check
    /// that DEL leaves nothing behind for any of them.
    fn build_ops(session: &Session, ty: &str, key: &[u8]) -> Vec<QueuedOp> {
        match ty {
            "list" => vec![crate::list::rpush(session, key, &[bs("a"), bs("b"), bs("c")])],
            "hash" => vec![crate::hash::hset(session, key, &[bs("f"), bs("v"), bs("g"), bs("w")])],
            "set" => vec![crate::set::sadd(session, key, &[bs("x"), bs("y")])],
            "zset" => vec![crate::zset::zadd(session, key, &[bs("1"), bs("a"), bs("2"), bs("b")])],
            "stream" => vec![
                crate::stream::xadd(session, key, false, None, None, None, &[bs("f"), bs("1")]),
                crate::stream::xadd(session, key, false, None, None, None, &[bs("f"), bs("2")]),
            ],
            "bloom" => vec![crate::bloom::add(session, key, b"item")],
            other => panic!("unknown type {other}"),
        }
    }

    #[tokio::test]
    async fn del_removes_every_internal_entry_of_each_compound_type() {
        for ty in ["list", "hash", "set", "zset", "stream", "bloom"] {
          for legacy in [false, true] {
            let session = test_session();
            for op in build_ops(&session, ty, b"k") {
                exec(&session, op).await;
            }
            if legacy {
                crate::testutil::legacy_key(&session, b"k").await;
            }
            assert!(internal_entry_count(&session).await > 0, "{ty}: test data must create internal entries");

            let reply = exec(&session, del(&session, &[bs("k")])).await;
            assert_eq!(expect_int(&reply), 1, "{ty}");
            assert_eq!(internal_entry_count(&session).await, 0, "{ty} (legacy layout: {legacy}): DEL left internal entries behind");
          }
        }
    }

    /// Stale internal entries used to reappear when a key was recreated under
    /// the same name.
    #[tokio::test]
    async fn recreating_a_deleted_stream_does_not_resurrect_old_entries() {
        let session = test_session();
        // Explicit, different IDs: auto-generated ones from the same
        // millisecond would collide on one key and hide the leftover.
        let id = |s: &[u8]| crate::stream::parse_stream_id_arg(s).expect("valid id");
        exec(&session, crate::stream::xadd(&session, b"st", false, None, None, id(b"1-1"), &[bs("f"), bs("old")])).await;
        exec(&session, del(&session, &[bs("st")])).await;
        exec(&session, crate::stream::xadd(&session, b"st", false, None, None, id(b"2-1"), &[bs("f"), bs("new")])).await;

        let reply = exec(&session, crate::stream::xrange(&session, b"st", None, None, 1000)).await;
        match reply {
            RespValue::Array(Some(entries)) => assert_eq!(entries.len(), 1, "old entry resurfaced: {entries:?}"),
            other => panic!("unexpected XRANGE reply: {other:?}"),
        }
    }

    #[tokio::test]
    async fn recreating_a_deleted_bloom_filter_does_not_resurrect_old_items() {
        let session = test_session();
        exec(&session, crate::bloom::add(&session, b"bf", b"old")).await;
        exec(&session, del(&session, &[bs("bf")])).await;
        exec(&session, crate::bloom::add(&session, b"bf", b"other")).await;

        let reply = exec(&session, crate::bloom::exists(&session, b"bf", b"old")).await;
        assert_eq!(reply, RespValue::Integer(0), "item from the deleted filter is still present");
    }

    /// `foo`'s list nodes share a string prefix with `foo:bar`'s; deleting one
    /// must not touch the other.
    #[tokio::test]
    async fn del_of_a_list_leaves_a_similarly_named_list_intact() {
        let session = test_session();
        exec(&session, crate::list::rpush(&session, b"foo", &[bs("1"), bs("2")])).await;
        exec(&session, crate::list::rpush(&session, b"foo:bar", &[bs("3"), bs("4")])).await;
        let neighbour = exec(&session, crate::list::lrange(&session, b"foo:bar", 0, -1)).await;

        exec(&session, del(&session, &[bs("foo")])).await;

        assert_eq!(exec(&session, crate::list::lrange(&session, b"foo:bar", 0, -1)).await, neighbour);
    }

    /// Pushes `values` onto list `l` and returns the session.
    async fn list_of(values: &[&str]) -> Session {
        let session = test_session();
        let values: Vec<Bytes> = values.iter().map(|v| bs(v)).collect();
        exec(&session, crate::list::rpush(&session, b"l", &values)).await;
        session
    }

    /// A list's stored nodes must track its live elements: removing elements
    /// deletes their nodes instead of orphaning them.
    #[tokio::test]
    async fn list_removals_delete_the_removed_nodes() {
        let s = list_of(&["a", "b", "c", "d", "e"]).await;
        assert_eq!(internal_entry_count(&s).await, 5);

        exec(&s, crate::list::lpop(&s, b"l")).await;
        assert_eq!(internal_entry_count(&s).await, 4, "LPOP");
        exec(&s, crate::list::rpop(&s, b"l")).await;
        assert_eq!(internal_entry_count(&s).await, 3, "RPOP");
        exec(&s, crate::list::lrem(&s, b"l", 1, b"c")).await;
        assert_eq!(internal_entry_count(&s).await, 2, "LREM");
        exec(&s, crate::list::rpush(&s, b"l", &[bs("x"), bs("y"), bs("z")])).await;
        assert_eq!(internal_entry_count(&s).await, 5);
        exec(&s, crate::list::ltrim(&s, b"l", 0, 1)).await;
        assert_eq!(internal_entry_count(&s).await, 2, "LTRIM");
    }

    #[tokio::test]
    async fn list_emptied_by_any_command_leaves_no_nodes() {
        // Popped to empty.
        let s = list_of(&["a", "b"]).await;
        exec(&s, crate::list::lpop(&s, b"l")).await;
        exec(&s, crate::list::lpop(&s, b"l")).await;
        assert_eq!(internal_entry_count(&s).await, 0, "LPOP to empty");

        // Removed to empty.
        let s = list_of(&["a", "a"]).await;
        exec(&s, crate::list::lrem(&s, b"l", 0, b"a")).await;
        assert_eq!(internal_entry_count(&s).await, 0, "LREM to empty");

        // Trimmed to an empty range.
        let s = list_of(&["a", "b", "c"]).await;
        exec(&s, crate::list::ltrim(&s, b"l", 5, 10)).await;
        assert_eq!(internal_entry_count(&s).await, 0, "LTRIM out of range");

        // Source of RPOPLPUSH emptied; the element lives on in the destination.
        let s = list_of(&["a"]).await;
        exec(&s, crate::list::rpoplpush(&s, b"l", b"dst")).await;
        assert_eq!(internal_entry_count(&s).await, 1, "RPOPLPUSH source emptied");
    }

    /// The names in a `KEYS` reply, sorted.
    async fn key_names(session: &Session, pattern: &[u8]) -> Vec<String> {
        match exec(session, keys(session, pattern)).await {
            RespValue::Array(Some(items)) => {
                let mut names: Vec<String> = items
                    .into_iter()
                    .map(|v| match v {
                        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
                        other => panic!("unexpected KEYS item {other:?}"),
                    })
                    .collect();
                names.sort();
                names
            }
            other => panic!("unexpected KEYS reply {other:?}"),
        }
    }

    #[tokio::test]
    async fn keys_star_lists_every_key_including_the_empty_name() {
        let session = test_session();
        for name in ["", "a", "b"] {
            seed_string(&session, name.as_bytes(), b"v").await;
        }
        assert_eq!(key_names(&session, b"*").await, ["", "a", "b"]);
        // Any other pattern goes through the glob matcher, which (as in Redis)
        // never matches the empty name.
        assert_eq!(key_names(&session, b"**").await, ["a", "b"]);
    }

    #[tokio::test]
    async fn keys_skips_expired_keys() {
        let session = test_session();
        seed_string(&session, b"live", b"v").await;
        seed_string(&session, b"gone", b"v").await;
        exec(&session, pexpire(&session, b"gone", 5)).await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(key_names(&session, b"*").await, ["live"]);
    }

    #[tokio::test]
    async fn keys_only_sees_the_current_db() {
        let mut session = test_session();
        seed_string(&session, b"zero", b"v").await;
        session.switch_db(1);
        seed_string(&session, b"one", b"v").await;
        assert_eq!(key_names(&session, b"*").await, ["one"]);
        session.switch_db(0);
        assert_eq!(key_names(&session, b"*").await, ["zero"]);
    }

    // --- compound values in the legacy and the namespaced layout (RFC 0001) ---

    const TYPES: [&str; 6] = ["list", "hash", "set", "zset", "stream", "bloom"];

    /// An op that reads back everything of the value of type `ty` under `key`.
    fn read_op(session: &Session, ty: &str, key: &[u8]) -> QueuedOp {
        match ty {
            "list" => crate::list::lrange(session, key, 0, -1),
            "hash" => crate::hash::hgetall(session, key),
            "set" => crate::set::smembers(session, key),
            "zset" => crate::zset::zrange(session, key, 0, -1, true),
            "stream" => crate::stream::xrange(session, key, None, None, 1000),
            "bloom" => crate::bloom::exists(session, key, b"item"),
            other => panic!("unknown type {other}"),
        }
    }

    /// What `read_op` returns, with unordered replies sorted.
    async fn read_back(session: &Session, ty: &str, key: &[u8]) -> RespValue {
        match exec(session, read_op(session, ty, key)).await {
            RespValue::Array(Some(mut items)) if ty == "set" => {
                items.sort_by_key(|i| format!("{i:?}"));
                RespValue::Array(Some(items))
            }
            other => other,
        }
    }

    async fn build(session: &Session, ty: &str, key: &[u8]) {
        for op in build_ops(session, ty, key) {
            exec(session, op).await;
        }
    }

    /// Builds the value, then puts it into the layout earlier releases wrote
    /// (elements addressed by the key's name).
    async fn build_legacy(session: &Session, ty: &str, key: &[u8]) {
        build(session, ty, key).await;
        crate::testutil::legacy_key(session, key).await;
    }

    #[tokio::test]
    async fn del_removes_a_namespaced_value_completely() {
        for ty in TYPES {
            let s = test_session();
            build(&s, ty, b"k").await;
            let ns = crate::testutil::namespace_of(&s, b"k").await;
            assert!(crate::testutil::entries_under(&s, &ns).await > 0, "{ty}");

            assert_eq!(expect_int(&exec(&s, del(&s, &[bs("k")])).await), 1, "{ty}");
            assert_eq!(crate::testutil::entries_under(&s, &ns).await, 0, "{ty}: DEL left elements in the namespace");
            assert_eq!(crate::testutil::stored_type_byte(&s, &s.public_key(b"k")).await, None, "{ty}");
        }
    }

    /// Renaming a namespaced value copies the sentinel and nothing else: every
    /// element entry is byte-for-byte what it was, so the cost does not grow
    /// with the size of the value.
    #[tokio::test]
    async fn renaming_a_namespaced_value_does_not_touch_its_elements() {
        for ty in TYPES {
            let s = test_session();
            build(&s, ty, b"old").await;
            let ns = crate::testutil::namespace_of(&s, b"old").await;
            let before = read_back(&s, ty, b"old").await;
            let elements = crate::testutil::entries_snapshot(&s, &ns).await;
            let empty = read_back(&s, ty, b"never-existed").await;
            assert_ne!(before, empty, "{ty}: test data must be visible");

            let reply = exec(&s, rename(&s, b"old", b"new")).await;
            assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"OK")), "{ty}");

            assert_eq!(read_back(&s, ty, b"new").await, before, "{ty}: data lost by RENAME");
            assert_eq!(read_back(&s, ty, b"old").await, empty, "{ty}: old name still has data");
            assert_eq!(crate::testutil::entries_snapshot(&s, &ns).await, elements, "{ty}: RENAME rewrote elements");
            let meta = crate::testutil::stored_type_byte(&s, &s.public_key(b"new")).await.unwrap();
            assert!(crate::common::namespace::is_namespaced(meta), "{ty}: the namespace must follow the key");
        }
    }

    /// A legacy value's elements are addressed by its old name, so the renamed
    /// value's sentinel records that prefix and nothing is moved: O(1) here too.
    #[tokio::test]
    async fn renaming_a_legacy_value_does_not_touch_its_elements() {
        for ty in TYPES {
            let s = test_session();
            build_legacy(&s, ty, b"old").await;
            let before = read_back(&s, ty, b"old").await;
            let empty = read_back(&s, ty, b"never-existed").await;
            let old_prefix = s.private_key(b"old");
            let elements = crate::testutil::entries_snapshot(&s, &old_prefix).await;
            assert!(!elements.is_empty(), "{ty}");

            exec(&s, rename(&s, b"old", b"new")).await;

            assert_eq!(read_back(&s, ty, b"new").await, before, "{ty}");
            assert_eq!(read_back(&s, ty, b"old").await, empty, "{ty}");
            assert_eq!(crate::testutil::entries_snapshot(&s, &old_prefix).await, elements, "{ty}: RENAME rewrote elements");
            assert_eq!(crate::testutil::namespace_of(&s, b"new").await, old_prefix, "{ty}: the new sentinel records the prefix the elements live under");
        }
    }

    /// Elements left under the old name's prefix by a rename can never be seen
    /// by a key created later under that name.
    #[tokio::test]
    async fn a_name_freed_by_a_rename_starts_empty() {
        for legacy in [false, true] {
            let s = test_session();
            exec(&s, crate::hash::hset(&s, b"old", &[bs("f"), bs("v")])).await;
            if legacy {
                crate::testutil::legacy_key(&s, b"old").await;
            }
            exec(&s, rename(&s, b"old", b"new")).await;

            exec(&s, crate::hash::hset(&s, b"old", &[bs("g"), bs("w")])).await;

            let fresh = exec(&s, crate::hash::hgetall(&s, b"old")).await;
            assert_eq!(fresh, RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("g"))), RespValue::BulkString(Some(bs("w")))])), "legacy={legacy}");
            let renamed = exec(&s, crate::hash::hgetall(&s, b"new")).await;
            assert_eq!(renamed, RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("f"))), RespValue::BulkString(Some(bs("v")))])), "legacy={legacy}");
        }
    }

    /// Strings (and other types with no separate elements) have nothing to
    /// record, so renaming them must leave a plain string behind.
    #[tokio::test]
    async fn renaming_a_string_does_not_make_it_a_namespaced_value() {
        let s = test_session();
        seed_string(&s, b"old", b"value").await;
        exec(&s, rename(&s, b"old", b"new")).await;
        assert_eq!(stored(&s, 0, b"new").await, Some(b"value".to_vec()));
        assert_eq!(
            crate::testutil::stored_type_byte(&s, &s.public_key(b"new")).await,
            Some(ValueType::String as u8)
        );
    }

    #[tokio::test]
    async fn rename_over_a_namespaced_destination_replaces_it() {
        let s = test_session();
        build(&s, "hash", b"dst").await;
        crate::testutil::exec(&s, crate::hash::hset(&s, b"dst", &[bs("stale"), bs("x")])).await;
        let dst_ns = crate::testutil::namespace_of(&s, b"dst").await;
        crate::testutil::exec(&s, crate::hash::hset(&s, b"src", &[bs("f"), bs("v")])).await;

        exec(&s, rename(&s, b"src", b"dst")).await;

        let got = exec(&s, crate::hash::hgetall(&s, b"dst")).await;
        assert_eq!(got, RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("f"))), RespValue::BulkString(Some(bs("v")))])));
        assert_eq!(crate::testutil::entries_under(&s, &dst_ns).await, 0, "the old destination's elements must be cleared");
    }

    #[tokio::test]
    async fn rename_carries_the_ttl_of_a_namespaced_value() {
        let s = test_session();
        build(&s, "list", b"old").await;
        exec(&s, expire(&s, b"old", 100)).await;
        exec(&s, rename(&s, b"old", b"new")).await;
        let remaining = ttl_secs(&s, b"new").await;
        assert!((90..=100).contains(&remaining), "ttl was {remaining}");
    }

    #[tokio::test]
    async fn type_scan_keys_and_expiry_see_through_the_namespace() {
        let s = test_session();
        for ty in TYPES {
            build(&s, ty, ty.as_bytes()).await;
        }
        // TYPE and EXISTS.
        for ty in TYPES {
            let reply = exec(&s, key_type(&s, ty.as_bytes())).await;
            let expected = if ty == "zset" { "zset" } else { ty };
            assert_eq!(reply, RespValue::SimpleString(Bytes::copy_from_slice(expected.as_bytes())), "{ty}");
        }
        assert_eq!(expect_int(&exec(&s, exists(&s, &[bs("hash"), bs("list"), bs("nope")])).await), 2);
        // KEYS lists each namespaced key once, none of its elements.
        assert_eq!(key_names(&s, b"*").await, ["bloom", "hash", "list", "set", "stream", "zset"]);
        // SCAN with a TYPE filter.
        let scan_reply = exec(&s, scan(&s, b"0", 100, None, type_byte(b"hash"))).await;
        let RespValue::Array(Some(parts)) = scan_reply else { panic!("SCAN must reply an array") };
        assert_eq!(parts[1], RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("hash")))])));
        // EXPIRE / TTL / PERSIST keep the value and the namespace.
        exec(&s, expire(&s, b"set", 100)).await;
        assert!((90..=100).contains(&ttl_secs(&s, b"set").await));
        exec(&s, persist(&s, b"set")).await;
        assert_eq!(ttl_secs(&s, b"set").await, -1);
        assert_eq!(read_back(&s, "set", b"set").await, {
            let t = test_session();
            build(&t, "set", b"set").await;
            read_back(&t, "set", b"set").await
        });
        let meta = crate::testutil::stored_type_byte(&s, &s.public_key(b"set")).await.unwrap();
        assert!(crate::common::namespace::is_namespaced(meta));
    }

    // --- MOVE ---

    #[tokio::test]
    async fn move_carries_a_legacy_value_to_another_db() {
        for ty in TYPES {
            let mut s = test_session();
            build_legacy(&s, ty, b"k").await;
            exec(&s, expire(&s, b"k", 100)).await;
            let before = read_back(&s, ty, b"k").await;

            assert_eq!(expect_int(&exec(&s, move_op(&s, b"k", 1)).await), 1, "{ty}");

            assert_eq!(internal_entry_count(&s).await, crate::testutil::entries_under(&s, b"-1:k").await, "{ty}: nothing may stay behind in db 0");
            s.switch_db(1);
            assert_eq!(read_back(&s, ty, b"k").await, before, "{ty}: data lost by MOVE");
            assert!((90..=100).contains(&ttl_secs(&s, b"k").await), "{ty}: TTL");
            s.switch_db(0);
            assert_eq!(stored(&s, 0, b"k").await, None, "{ty}");
        }
    }

    #[tokio::test]
    async fn move_carries_a_namespaced_value_to_another_db() {
        for ty in TYPES {
            let mut s = test_session();
            build(&s, ty, b"k").await;
            let ns = crate::testutil::namespace_of(&s, b"k").await;
            let before = read_back(&s, ty, b"k").await;
            let elements = crate::testutil::entries_snapshot(&s, &ns).await.len();

            assert_eq!(expect_int(&exec(&s, move_op(&s, b"k", 1)).await), 1, "{ty}");

            // The namespace's db number follows the key, so FLUSHDB of db 0 cannot destroy it.
            assert_eq!(crate::testutil::entries_under(&s, &ns).await, 0, "{ty}: elements left in db 0's namespace");
            let moved = crate::common::namespace::with_db(&ns, 1).unwrap();
            assert_eq!(crate::testutil::entries_under(&s, &moved).await, elements, "{ty}");
            s.switch_db(1);
            assert_eq!(read_back(&s, ty, b"k").await, before, "{ty}: data lost by MOVE");
        }
    }

    #[tokio::test]
    async fn move_does_nothing_when_the_target_exists_or_the_db_is_the_same() {
        let s = test_session();
        build(&s, "hash", b"k").await;
        let before = read_back(&s, "hash", b"k").await;
        // Occupy the key in db 1.
        {
            let mut other = Session::new(s.store(), s.registry());
            other.switch_db(1);
            seed_string(&other, b"k", b"already here").await;
        }
        assert_eq!(expect_int(&exec(&s, move_op(&s, b"k", 1)).await), 0);
        assert_eq!(read_back(&s, "hash", b"k").await, before, "a refused MOVE must leave the source alone");
        assert_eq!(stored(&s, 1, b"k").await, Some(b"already here".to_vec()));

        let reply = exec(&s, move_op(&s, b"k", 0)).await;
        assert_eq!(reply, RespValue::Error(Bytes::from_static(b"ERR source and destination objects are the same")));
        assert_eq!(read_back(&s, "hash", b"k").await, before, "MOVE to the same db must not delete the key");

        assert_eq!(expect_int(&exec(&s, move_op(&s, b"missing", 1)).await), 0);
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

    fn bs(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// Remaining TTL of `key` in seconds, as `TTL` reports it (-1 = none).
    async fn ttl_secs(session: &Session, key: &[u8]) -> i64 {
        *exec_db(session, ttl(session, key)).await.downcast::<i64>().unwrap()
    }

    /// Runs `setup`, gives `key` a 100s TTL, runs `mutate` and asserts the
    /// TTL survived: Redis keeps a key's expiry when a command modifies it.
    async fn assert_mutation_keeps_ttl(
        session: &Session,
        key: &[u8],
        setup: QueuedOp,
        mutate: QueuedOp,
    ) {
        exec(session, setup).await;
        exec(session, expire(session, key, 100)).await;
        assert!((90..=100).contains(&ttl_secs(session, key).await), "setup should leave a TTL");

        let reply = exec(session, mutate).await;
        assert!(!matches!(reply, RespValue::Error(_)), "mutation failed: {reply:?}");

        let remaining = ttl_secs(session, key).await;
        assert!((90..=100).contains(&remaining), "TTL was lost or changed: {remaining}");
    }

    #[tokio::test]
    async fn string_updates_keep_the_ttl() {
        use crate::strings;
        let s = test_session();
        seed_string(&s, b"n", b"5").await;
        assert_mutation_keeps_ttl(&s, b"n", strings::increment(&s, b"n", 1), strings::increment(&s, b"n", 2)).await;
        seed_string(&s, b"n2", b"5").await;
        assert_mutation_keeps_ttl(&s, b"n2", strings::increment(&s, b"n2", 0), strings::increment(&s, b"n2", -3)).await;
        seed_string(&s, b"a", b"x").await;
        assert_mutation_keeps_ttl(&s, b"a", strings::append(&s, b"a", b"y"), strings::append(&s, b"a", b"z")).await;
        seed_string(&s, b"r", b"hello").await;
        assert_mutation_keeps_ttl(&s, b"r", strings::set_range(&s, b"r", 0, b"j"), strings::set_range(&s, b"r", 1, b"u")).await;
        seed_string(&s, b"f", b"1.5").await;
        assert_mutation_keeps_ttl(&s, b"f", strings::incr_by_float(&s, b"f", 0.0), strings::incr_by_float(&s, b"f", 1.0)).await;
        seed_string(&s, b"b", b"\x00").await;
        assert_mutation_keeps_ttl(&s, b"b", crate::bitmap::set_bit(&s, b"b", 1, 1), crate::bitmap::set_bit(&s, b"b", 2, 1)).await;
        assert_mutation_keeps_ttl(&s, b"hll", crate::hll::pfadd(&s, b"hll", &[bs("a")]), crate::hll::pfadd(&s, b"hll", &[bs("b"),  bs("c")])).await;
    }

    #[tokio::test]
    async fn compound_updates_keep_the_ttl() {
        let s = test_session();
        assert_mutation_keeps_ttl(&s, b"h", crate::hash::hset(&s, b"h", &[bs("f"),  bs("v")]), crate::hash::hset(&s, b"h", &[bs("g"),  bs("w")])).await;
        assert_mutation_keeps_ttl(&s, b"st", crate::set::sadd(&s, b"st", &[bs("x")]), crate::set::sadd(&s, b"st", &[bs("y")])).await;
        assert_mutation_keeps_ttl(&s, b"z", crate::zset::zadd(&s, b"z", &[bs("1"),  bs("a")]), crate::zset::zadd(&s, b"z", &[bs("2"),  bs("b")])).await;
        assert_mutation_keeps_ttl(&s, b"l", crate::list::rpush(&s, b"l", &[bs("a")]), crate::list::rpush(&s, b"l", &[bs("b")])).await;
        assert_mutation_keeps_ttl(&s, b"l2", crate::list::lpush(&s, b"l2", &[bs("a")]), crate::list::lpush(&s, b"l2", &[bs("b")])).await;
        assert_mutation_keeps_ttl(
            &s,
            b"x",
            crate::stream::xadd(&s, b"x", false, None, None, None, &[bs("f"),  bs("1")]),
            crate::stream::xadd(&s, b"x", false, None, None, None, &[bs("f"),  bs("2")]),
        )
        .await;
        assert_mutation_keeps_ttl(&s, b"bf", crate::bloom::add(&s, b"bf", b"one"), crate::bloom::add(&s, b"bf", b"two")).await;
    }

    /// Commands that replace the value are the ones that clear an expiry.
    #[tokio::test]
    async fn overwriting_commands_still_clear_the_ttl() {
        let s = test_session();
        seed_string(&s, b"k", b"v").await;
        exec(&s, expire(&s, b"k", 100)).await;
        exec(&s, crate::strings::get_set(&s, b"k", b"new")).await;
        assert_eq!(ttl_secs(&s, b"k").await, -1);
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
            let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
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
        let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
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
        let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
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
