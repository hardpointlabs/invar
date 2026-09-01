//! kv: vendor-neutral abstraction over a transactional LSM-tree key/value
//! store, in the spirit of the Go package `kv` (see `kv/kv.go` and
//! `kv/doc.go` in the repository root). Backed by SlateDB (`slate`) or
//! Fjall (`fjall`).
//!
//! # Guarantee model
//!
//! The abstraction makes different promises for different kinds of
//! operations. Don't assume one blanket isolation level applies to
//! everything below — which promise applies depends on which entry point
//! you use.
//!
//! | Entry point                 | Guarantee                                        |
//! |------------------------------|--------------------------------------------------|
//! | Tx (get/set/delete/commit)   | Snapshot isolation, write-write conflict detection (`Error::Conflict`) |
//! | Tx::new_iterator             | Same snapshot as the enclosing Tx; lexicographic key order |
//! | KeyValueStore::merge         | Not yet implemented. |
//!
//! All get/set/delete calls made through a Tx observe a consistent snapshot
//! fixed at the transaction's start, and a Tx's buffered writes apply as a
//! single indivisible unit on a successful commit, or not at all on error.
//! Two concurrent mutating transactions whose write sets intersect cannot
//! both commit, the loser's commit returns `Error::Conflict`.
//!
//! Commit returning `Ok(())` means the write is applied and visible; it does
//! NOT yet mean the write is guaranteed durable against an unclean process
//! restart — that depends on backend-level configuration (e.g. SlateDB's
//! `AwaitDurable`) which is not currently exposed as a per-call choice on
//! this interface.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

/// A pinned, `Send` boxed future. The closure-based entry points on
/// [`KeyValueStore`] use this type so callers can write `async` blocks
/// inside the closures.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Error returned by the kv abstraction. Mirrors the three sentinel errors
/// of the Go package (`ErrKeyNotFound`, `ErrConflict`, `ErrUndefined`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Conflict")]
    Conflict,
    #[error("Undefined")]
    Undefined,
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Key-value pair, with optional TTL and metadata, to write to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
    meta: u8,
    ttl: Duration,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            meta: 0,
            ttl: Duration::ZERO,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn metadata(mut self, data: u8) -> Self {
        self.meta = data;
        self
    }

    pub fn metadata_byte(&self) -> u8 {
        self.meta
    }

    pub fn ttl(mut self, duration: Duration) -> Self {
        self.ttl = duration;
        self
    }

    /// Returns the TTL set via [`Entry::ttl`], or `Duration::ZERO` if none
    /// was set.
    pub fn ttl_value(&self) -> Duration {
        self.ttl
    }
}

/// Immutable key-value pair read from the store.
#[derive(Debug, Clone)]
pub struct Item {
    key: Vec<u8>,
    value: Vec<u8>,
    meta: u8,
    /// Absolute expiry time as Unix milliseconds, or `None` if the entry
    /// never expires.
    expire_ts: Option<i64>,
}

impl Item {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the remaining TTL, or `Duration::ZERO` if the entry has no
    /// TTL or has already expired.
    pub fn ttl(&self) -> Duration {
        match self.expire_ts {
            Some(ts) => {
                let remaining = ts - now_millis();
                if remaining < 0 {
                    Duration::ZERO
                } else {
                    Duration::from_millis(remaining as u64)
                }
            }
            None => Duration::ZERO,
        }
    }

    /// Returns the metadata byte set when the entry was written.
    pub fn metadata(&self) -> u8 {
        self.meta
    }

    /// Returns the absolute expiry time as Unix seconds, or 0 if the entry
    /// never expires.
    pub fn expires_at(&self) -> u64 {
        self.expire_ts.map(|ts| (ts / 1000) as u64).unwrap_or(0)
    }

    /// Returns the absolute expiry time as Unix milliseconds, or 0 if the
    /// entry never expires.
    pub fn expires_at_millis(&self) -> u64 {
        self.expire_ts
            .filter(|&ts| ts > 0)
            .map(|ts| ts as u64)
            .unwrap_or(0)
    }

    /// Returns the value of this entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Constructs an item from its raw parts. Used by backend
    /// implementations to translate a stored key-value pair into the
    /// vendor-neutral [`Item`] view.
    pub(crate) fn new(key: Vec<u8>, value: Vec<u8>, meta: u8, expire_ts: Option<i64>) -> Self {
        Self {
            key,
            value,
            meta,
            expire_ts,
        }
    }
}

/// Handle returned by a completed write. Currently informational only: the
/// store's merge support is not yet implemented.
#[derive(Debug, Clone)]
pub struct WriteHandle {
    inner: slatedb::WriteHandle,
}

impl WriteHandle {
    pub fn seqnum(&self) -> u64 {
        self.inner.seqnum()
    }

    pub fn create_ts(&self) -> i64 {
        self.inner.create_ts()
    }
}

/// Generic ordered iterator of keys in the store (not complete).
#[async_trait]
pub trait KeyValueIterator: Send + Sync + 'static {
    /// Advances the iterator and reports whether an item is available.
    /// Must be called before the first [`KeyValueIterator::item`]. Returns
    /// `false` when the prefix range is exhausted OR on error — call
    /// [`KeyValueIterator::err`] to distinguish the two.
    async fn next(&mut self) -> bool;

    /// Returns the current entry. Valid only after [`KeyValueIterator::next`]
    /// returns `true`.
    fn item(&self) -> Option<&Item>;

    /// Returns any error encountered during iteration. Always check this
    /// after a [`KeyValueIterator::next`] that returns `false`.
    fn err(&self) -> Option<&Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

/// Generic transaction object.
#[async_trait]
pub trait Tx: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Result<Item, Error>;

    fn set(&self, entry: Entry) -> Result<(), Error>;

    fn delete(&self, key: &[u8]) -> Result<(), Error>;

    /// Create a KeyValueIterator with an arbitrary start and end key
    /// An unbounded start key means starting from the beginning of the keyspace
    /// An unbounded end key means this iterator will run through the whole keyspace (assuming
    /// caller keeps iterating).
    async fn new_range_iterator(&self, start: std::ops::Bound<&[u8]>, end: std::ops::Bound<&[u8]>) -> Result<Box<dyn KeyValueIterator>, Error>;

    /// Create a KeyValueIterator with an arbitrary start key prefix
    async fn new_prefix_iterator(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueIterator>, Error>;

    async fn commit(self: Box<Self>) -> Result<(), Error>;

    fn discard(self: Box<Self>);
}

/// Abstraction over a transactional LSM Tree implementation.
#[async_trait]
pub trait KeyValueStore: Send + Sync + 'static {
    fn new_entry(&self, key: Vec<u8>, value: Vec<u8>) -> Entry;

    /// Create a new manually managed transaction. It's critical to call
    /// [`Tx::discard`] after use to ensure any resources are cleaned up.
    async fn begin(&self, mutating: bool) -> Result<Box<dyn Tx>, Error>;

    async fn update<F>(&self, f: F) -> Result<(), Error>
    where
        F: for<'a> FnOnce(&'a dyn Tx) -> BoxFuture<'a, Result<(), Error>> + Send + 'static;

    async fn read<R, F>(&self, f: F) -> Result<R, Error>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a dyn Tx) -> BoxFuture<'a, Result<R, Error>> + Send + 'static;

    /// It's critical to call this to clean up all DB resources and to ensure
    /// all data is persisted to durable storage.
    async fn close(&self) -> Result<(), Error>;

    /// Merge appends a commutative delta to key using the store's globally
    /// registered merge operator. Not yet implemented: returns `Ok(None)`.
    async fn merge(&self, key: &[u8], operand: &[u8]) -> Result<Option<WriteHandle>, Error>;

    /// Forcibly write data to underlying storage.
    async fn sync(&self) -> Result<(), Error>;

    /// Drop all keys in the entire key value store.
    async fn destroy(&self) -> Result<(), Error>;

    /// Drop all keys in the key value store starting with the specified
    /// prefix.
    async fn drop_prefix(&self, prefix: &[u8]) -> Result<(), Error>;
}
