//! Housekeeping glue code shared by all Redis command implementations,
//! mirroring the Go `redis/common` package: per-connection [`Session`] state,
//! command batching via the [`DbOp`]/[`WireOp`] abstractions, and the
//! [`WatchRegistry`] that tracks clients blocked on key changes.

pub mod op;
pub mod registry;
pub mod session;
pub mod store;

pub use op::{DbError, DbOp, DbResult, QueuedOp, WireOp};
pub use registry::{BlockResult, Claim, PopResult, StreamResult, WaitKind, WatchRegistry};
pub use session::{BlockingSignal, Session, SessionError};
pub use store::RedisStore;

/// Writes `entry`, carrying over the remaining TTL of whatever is currently
/// stored under the same key.
///
/// Redis keeps a key's expiry when a command modifies its value (`INCR`,
/// `APPEND`, `HSET`, `RPUSH`, ...); only commands that replace the value
/// (`SET`, `GETSET`, `*STORE` destinations) clear it. Writing a plain
/// [`kv::kv::Entry`] always clears the expiry, so commands that update an
/// existing key must write through this instead.
pub(crate) async fn set_keeping_ttl(
    tx: &dyn kv::kv::Tx,
    entry: kv::kv::Entry,
) -> Result<(), kv::kv::Error> {
    // An absent or already expired key has no expiry to keep.
    let entry = match tx.get(entry.key()).await {
        Ok(existing) => entry.keep_ttl_of(&existing),
        Err(kv::kv::Error::KeyNotFound) => entry,
        Err(e) => return Err(e),
    };
    tx.set(entry)
}

/// The public Redis value types stored as the metadata byte on LSM entries.
/// Not private/internal types.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String = 0,
    List,
    Set,
    SortedSet,
    Hash,
    Stream,
    VectorSet,
    Bloom,
    Json,
}
