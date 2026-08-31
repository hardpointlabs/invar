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
pub use session::{Session, SessionError};
pub use store::RedisStore;

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
