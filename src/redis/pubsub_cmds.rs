//! QueuedOp wrappers for pub/sub commands that participate in MULTI/EXEC.
//!
//! `PUBLISH` and `SPUBLISH` are the only pub/sub commands that can appear
//! inside a `MULTI` block.  They need no database transaction (pub/sub state
//! is purely in-memory), so the `DbOp` half is a no-op and the `WireOp` half
//! executes the actual publish and replies with the receiver count.

use std::sync::Arc;

use bytes::Bytes;
use kv::kv::{BoxFuture, Tx};

use crate::common::op::{DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::pubsub::PubSubRegistry;
use crate::resp::RespValue;

/// Returns a [`QueuedOp`] that publishes `payload` to `channel` when executed.
///
/// The publish happens inside the `WireOp` (after transaction commit) because
/// pub/sub delivery should not be rolled back if the enclosing transaction
/// fails for unrelated reasons — this matches real Redis semantics where
/// `PUBLISH` inside `MULTI` always fires when `EXEC` runs.
pub(crate) fn publish_op(
    registry: Arc<PubSubRegistry>,
    channel: Bytes,
    payload: Bytes,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PublishDbOp),
        wire_op: Box::new(PublishWireOp {
            registry,
            channel,
            payload,
        }),
        is_mutating: false,
    }
}

// ---------------------------------------------------------------------------

struct PublishDbOp;

impl DbOp for PublishDbOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

struct PublishWireOp {
    registry: Arc<PubSubRegistry>,
    channel: Bytes,
    payload: Bytes,
}

impl WireOp for PublishWireOp {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => {
                let count = self.registry.publish(&self.channel, self.payload.clone());
                RespValue::Integer(count)
            }
            Err(e) => crate::common::op::err_resp(&e),
        }
    }
}
