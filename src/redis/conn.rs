//! Redis connection & server management commands.
//!
//! Port of the Go `redis/conn` package. These are mostly stubbed out for
//! client compatibility (per `COMPATIBILITY.md`) plus a few real operations:
//! `SELECT` for switching the connection's DB, `DBSIZE` for counting keys in
//! the current namespace, and `SAVE`/`BGSAVE` for flushing the store. `PING`
//! and `ECHO` are implemented directly in the dispatcher (as in the Go
//! listener), not here.
//!
//! `SAVE` cannot be expressed as a wire-only op (its reply must await the
//! store flush), so the dispatcher handles it specially; the remaining
//! commands are queued [`QueuedOp`]s whose database half is a no-op.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Tx};

use crate::common::op::{DbError, DbOp, DbResult, NoOp, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::RedisStore;
use crate::resp::RespValue;

/// The Invar version and commit banners reported by `LOLWUT`, matching the
/// Go `config` package defaults.
pub const VERSION: &str = "dev";
pub const COMMIT: &str = "unknown";

/// `SYNC`/`PSYNC` — stubbed, since Invar runs single-writer without
/// replication. Replies `+OK`.
pub fn sync() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(OkWire),
        is_mutating: false,
        allowed_in_tx: false,
    }
}

/// `WAIT` — stubbed. Replies `+OK`.
pub fn wait() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(OkWire),
        is_mutating: false,
        allowed_in_tx: false,
    }
}

/// `LOLWUT` — replies with the Invar version banner.
pub fn lolwut(version: &str, commit: &str) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(LolwutWire {
            version: version.to_string(),
            commit: commit.to_string(),
        }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `TIME` — replies `[unix-seconds, microseconds]` as bulk strings.
pub fn time() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(TimeWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `MODULE LIST` — the only supported subcommand, replying an empty array.
pub fn module(args: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(ModuleWire {
            args: args.to_vec(),
        }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `BGSAVE` — replies `+OK` immediately and flushes the store in the
/// background.
pub fn bgsave(session: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(BgSaveWire {
            store: session.store(),
        }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// `DBSIZE` — counts the public keys in the current DB by iterating the
/// session prefix. O(n) rather than O(1), as in Go.
pub fn dbsize(session: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(DbSizeOp {
            prefix: session.prefix(),
        }),
        wire_op: Box::new(DbSizeWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// A queued op that replies `+OK` with no database effect. Used by `SELECT`
/// inside a `MULTI` block: the dispatcher switches the connection's DB
/// immediately (mirroring Go's run-time key derivation, which is equivalent
/// because SELECTs are dispatched in queue order), but defers the `OK` reply
/// to `EXEC` so the transaction returns one element per command.
pub fn ok_op() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

/// A queued op that replies `PONG` with an optional message, if provided
pub fn ping(msg: Option<Bytes>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(PingOp { msg }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

/// A queued op that echoes a message back to the sender
pub fn echo(msg: Bytes) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(EchoOp { msg }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

// --- WireOp halves ---

/// Replies `+OK` on success.
struct OkWire;

impl WireOp for OkWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    }
}

struct LolwutWire {
    version: String,
    commit: String,
}

impl WireOp for LolwutWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        RespValue::BulkString(Some(Bytes::from(format!(
            "Invar version: {}, commit: {}\n",
            self.version, self.commit
        ))))
    }
}

/// Replies `[seconds, microseconds]` for the current time.
struct TimeWire;

impl WireOp for TimeWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let sec = now.as_secs().to_string();
        let micro = now.subsec_micros().to_string();
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from(sec))),
            RespValue::BulkString(Some(Bytes::from(micro))),
        ]))
    }
}

struct ModuleWire {
    args: Vec<Bytes>,
}

impl WireOp for ModuleWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        if self.args.len() < 2 {
            return RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'module' command",
            ));
        }
        if self.args[1].eq_ignore_ascii_case(b"list") {
            RespValue::Array(Some(Vec::new()))
        } else {
            RespValue::Error(Bytes::from_static(b"ERR unknown subcommand"))
        }
    }
}

/// Replies `+OK` and flushes the store on a background task.
struct BgSaveWire {
    store: Arc<dyn RedisStore>,
}

impl WireOp for BgSaveWire {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        let store = self.store.clone();
        tokio::spawn(async move {
            let _ = store.sync().await;
        });
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    }
}

// --- DbOp half for DBSIZE ---

struct DbSizeOp {
    prefix: Vec<u8>,
}

impl DbOp for DbSizeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let prefix = self.prefix.clone();
        Box::pin(async move {
            let mut it = tx.new_prefix_iterator(&prefix).await?;
            let mut count: i64 = 0;
            while it.next().await {
                count += 1;
            }
            let err = it.err().cloned();
            if let Some(e) = err {
                it.close().await?;
                return Err(DbError::Kv(e));
            }
            it.close().await?;
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

/// Replies the DBSIZE count, using Go's unusual `ERR: ` prefix on failure.
struct DbSizeWire;

impl WireOp for DbSizeWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(count) => RespValue::Integer(*count),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR: internal error: bad int result"))
                }
            },
            Err(e) => RespValue::Error(Bytes::from(format!("ERR: {e}"))),
        }
    }
}

/// `PING [message]` — replies `+PONG`, or the message as a bulk string.
pub struct PingOp {
    msg: Option<Bytes>,
}

impl WireOp for PingOp {
    fn reply(
        &self,
        _result: Result<crate::common::op::DbResult, crate::common::op::DbError>,
    ) -> RespValue {
        match &self.msg {
            Some(msg) => RespValue::BulkString(Some(msg.clone())),
            None => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        }
    }
}

/// `ECHO message` — replies with the message as a bulk string.
pub struct EchoOp {
    msg: Bytes,
}

impl WireOp for EchoOp {
    fn reply(
        &self,
        _result: Result<crate::common::op::DbResult, crate::common::op::DbError>,
    ) -> RespValue {
        RespValue::BulkString(Some(self.msg.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ValueType;
    use crate::testutil::test_session;
    use kv::kv::Entry;

    /// Runs a queued op through its own transaction and renders the reply.
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Seeds a plain string key in the current DB of `session`.
    async fn seed(session: &Session, key: &[u8]) {
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(
            Entry::new(session.public_key(key), b"v".to_vec())
                .metadata(ValueType::String as u8),
        )
        .expect("seed");
        tx.commit().await.expect("commit");
    }

    fn expect_ok(reply: &RespValue) {
        assert_eq!(RespValue::SimpleString(Bytes::from_static(b"OK")), *reply);
    }

    fn expect_err(reply: &RespValue) -> Bytes {
        match reply {
            RespValue::Error(e) => e.clone(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sync_wait_reply_ok() {
        let session = test_session();
        expect_ok(&exec(&session, sync()).await);
        expect_ok(&exec(&session, wait()).await);
    }

    #[tokio::test]
    async fn lolwut_mentions_invar() {
        let session = test_session();
        let reply = exec(&session, lolwut("1.0.0", "abc123")).await;
        match reply {
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(&b).to_string();
                assert_eq!(s, "Invar version: 1.0.0, commit: abc123\n");
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn time_returns_sec_and_micro() {
        let session = test_session();
        let reply = exec(&session, time()).await;
        match reply {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                let get = |i: usize| -> i64 {
                    match &items[i] {
                        RespValue::BulkString(Some(b)) => {
                            String::from_utf8_lossy(b).parse().unwrap()
                        }
                        other => panic!("expected bulk, got {other:?}"),
                    }
                };
                let before = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                let sec = get(0);
                assert!((sec - before).abs() <= 2);
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn module_list_is_empty_array() {
        let session = test_session();
        let reply = exec(&session, module(&[Bytes::from_static(b"module"), Bytes::from_static(b"list")])).await;
        assert_eq!(reply, RespValue::Array(Some(Vec::new())));

        assert_eq!(
            expect_err(&exec(&session, module(&[Bytes::from_static(b"module")])).await),
            Bytes::from_static(b"ERR wrong number of arguments for 'module' command")
        );

        assert_eq!(
            expect_err(&exec(&session, module(&[
                Bytes::from_static(b"module"),
                Bytes::from_static(b"load"),
            ]))
            .await),
            Bytes::from_static(b"ERR unknown subcommand")
        );
    }

    #[tokio::test]
    async fn bgsave_replies_ok() {
        let session = test_session();
        expect_ok(&exec(&session, bgsave(&session)).await);
        // Let the spawned flush complete.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn dbsize_counts_keys_in_current_db() {
        let session = test_session();
        let reply = exec(&session, dbsize(&session)).await;
        assert_eq!(reply, RespValue::Integer(0));

        seed(&session, b"a").await;
        seed(&session, b"b").await;
        seed(&session, b"c").await;
        let reply = exec(&session, dbsize(&session)).await;
        assert_eq!(reply, RespValue::Integer(3));

        // Keys in another DB don't count.
        let mut session2 = Session::new(session.store(), session.registry());
        session2.switch_db(1);
        seed(&session2, b"only-db1").await;

        let reply = exec(&session, dbsize(&session)).await;
        assert_eq!(reply, RespValue::Integer(3));
        let reply = exec(&session2, dbsize(&session2)).await;
        assert_eq!(reply, RespValue::Integer(1));
    }
}