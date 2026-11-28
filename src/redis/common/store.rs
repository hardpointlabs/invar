//! The object-safe face of the key-value store used by the Redis layer.
//!
//! [`kv::kv::KeyValueStore`] is not object-safe (its `update`/`read` entry
//! points take higher-ranked generic closures), so it cannot be used as
//! `dyn`. The Redis layer only needs a small subset — start a transaction,
//! and the whole-store teardown operations — which is extracted here into
//! [`RedisStore`] and blanket-implemented for every concrete backend.

use async_trait::async_trait;
use kv::kv::{AckLevel, Error as KvError, Error, KeyValueStore, Tx, TxIsolation, WriteHandle};
use std::time::Duration;

/// The subset of [`KeyValueStore`] the Redis server needs, usable through a
/// trait object so sessions, commands and the listener stay backend-agnostic.
#[async_trait]
pub trait RedisStore: Send + Sync + 'static {
    /// Opens a manually managed transaction. Callers must [`Tx::commit`] or
    /// [`Tx::discard`] it.
    async fn begin(&self, mutating: bool, isolation: TxIsolation) -> Result<Box<dyn Tx>, KvError>;

    /// Closes the store, flushing all data to durable storage.
    async fn close(&self) -> Result<(), KvError>;

    /// Forcibly flushes buffered writes to durable storage (SAVE/BGSAVE).
    async fn sync(&self) -> Result<(), KvError>;

    /// Destroys the entire store.
    async fn destroy(&self) -> Result<(), KvError>;

    /// Deletes every key starting with `prefix`.
    async fn drop_prefix(&self, prefix: &[u8]) -> Result<(), KvError>;

    async fn await_until(&self, handle: WriteHandle, duration: Duration) -> Result<bool, Error>;

    /// Sets when commits are acknowledged (RFC 0004).
    fn set_ack_level(&self, level: AckLevel);

    /// The current acknowledgement level.
    fn ack_level(&self) -> AckLevel;
}

#[async_trait]
impl<S: KeyValueStore> RedisStore for S {
    async fn begin(&self, mutating: bool, isolation: TxIsolation) -> Result<Box<dyn Tx>, KvError> {
        KeyValueStore::begin(self, mutating, isolation).await
    }

    async fn close(&self) -> Result<(), KvError> {
        KeyValueStore::close(self).await
    }

    async fn sync(&self) -> Result<(), KvError> {
        KeyValueStore::sync(self).await
    }

    async fn destroy(&self) -> Result<(), KvError> {
        KeyValueStore::destroy(self).await
    }

    async fn drop_prefix(&self, prefix: &[u8]) -> Result<(), KvError> {
        KeyValueStore::drop_prefix(self, prefix).await
    }

    async fn await_until(&self, handle: WriteHandle, duration: Duration) -> Result<bool, Error> {
        KeyValueStore::await_until(self, handle, duration).await
    }

    fn set_ack_level(&self, level: AckLevel) {
        KeyValueStore::set_ack_level(self, level)
    }

    fn ack_level(&self) -> AckLevel {
        KeyValueStore::ack_level(self)
    }
}
