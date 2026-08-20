//! The two abstract command halves: the database operations (`DbOp`) and the
//! wire operations (`WireOp`).
//!
//! This mirrors the Go `redis/common` `QueuedOp` split (see
//! `redis/common/README.md`): a command is a lazy pair of closures. The `DbOp`
//! runs inside a transaction owned by the session's dispatcher and returns an
//! opaque, type-erased result; the `WireOp` runs after the transaction commits
//! (or fails) and renders that result into a RESP reply.
//!
//! The two are declared lazily so a session can either execute an op
//! immediately or, inside a `MULTI` block, queue it and run the whole batch
//! together at `EXEC`.

use std::any::Any;

use bytes::Bytes;
use kv::kv::{BoxFuture, Error as KvError, Tx};
use crate::resp;
use crate::resp::RespValue;

/// Opaque result of a [`DbOp`], analogous to Go's `any`. The corresponding
/// [`WireOp`] downcasts it to the concrete type the command produced.
pub type DbResult = Box<dyn Any + Send>;

/// Error space of the database side of a command. Wraps the kv abstraction's
/// errors and adds Redis-specific failures the store cannot express (e.g.
/// `WRONGTYPE` when a command hits a key holding a different value type).
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    Kv(#[from] KvError),
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("key already exists")]
    KeyExists,
    /// A Redis-specific error with a literal message (rendered as `ERR `msg).
    /// Carries no "ERR" prefix itself.
    #[error("{0}")]
    Redis(String),
}

/// RESP error reply for `WRONGTYPE`.
const WRONG_TYPE_REPLY: Bytes =
    Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value");

/// Renders a [`DbError`] into the RESP error a client should see.
pub fn err_resp(err: &DbError) -> RespValue {
    match err {
        DbError::WrongType => RespValue::Error(WRONG_TYPE_REPLY),
        DbError::KeyExists => RespValue::Error(Bytes::from_static(b"ERR key already exists")),
        DbError::Redis(msg) => RespValue::Error(format!("ERR {msg}").into()),
        DbError::Kv(e) => RespValue::Error(format!("ERR {e}").into()),
    }
}

/// The database side of a command: a lazy bundle of KV-store operations that
/// runs inside a transaction managed by [`crate::common::Session`].
///
/// Implementations must be `'static` (they are owned by the session's queue)
/// and must not borrow the session or the connection.
pub trait DbOp: Send + 'static {
    /// Runs the KV operations against `tx`. The returned future may borrow
    /// both `self` and `tx`; the dispatcher awaits it to completion before
    /// running the next op.
    /// The default implementation is a no-op, making no changes to the `KeyValueStore`
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }

    /// Called by the dispatcher when a transaction that ran this op failed to
    /// commit, so any waiter claims embedded in `result` can be returned to
    /// the watch registry. A no-op for commands that carry no claims.
    fn release_claims(&self, _result: &DbResult) {}
}

/// The wire side of a command: renders the outcome of a [`DbOp`] into the
/// RESP reply the client receives. Runs after the transaction commits, or
/// with an error if it failed.
pub trait WireOp: Send + 'static {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => resp::ok_resp(),
            Err(e) => RespValue::Error(Bytes::from(e.to_string())),
        }
    }
}

pub struct DefaultWire;

impl WireOp for DefaultWire {}

/// A command split into its database and wire halves, as returned by command
/// implementations.
pub struct QueuedOp {
    pub db_op: Box<dyn DbOp>,
    pub wire_op: Box<dyn WireOp>,
    /// Whether this op must run inside a writable transaction.
    pub is_mutating: bool,
    /// Whether this op is allowed to be enqueued inside a MULTI transactional block
    pub allowed_in_tx: bool,
    /// When true and inside MULTI, this op is rejected immediately (its wire
    /// reply is returned to the client) and the transaction is marked dirty
    /// — the op is never enqueued. Used by `error_op` so that parse / arity /
    /// unknown-command failures produce the correct error reply and trigger
    /// `EXECABORT`, matching Redis semantics.
    pub abort_in_tx: bool,
}

/// A [`DbOp`] with no database effect, used for wire-only commands such as
/// `PING` and `ECHO`.
pub struct NoOp;

impl DbOp for NoOp {}

pub fn wire_only_op(wire_op: Box<dyn WireOp>, allowed_in_tx: bool) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op,
        is_mutating: false,
        allowed_in_tx,
        abort_in_tx: false,
    }
}
