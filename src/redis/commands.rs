//! Redis command dispatch: routes a decoded command array to its handler,
//! enforcing the `MULTI`/`EXEC`/`DISCARD` lifecycle, and flushes any queued
//! operations through the session's transaction dispatcher.

use bytes::Bytes;

use crate::common::op::WireOp;
use crate::common::session::Session;
use crate::resp::RespValue;
use crate::strings;

/// Dispatches a single decoded command. Returns the RESP replies to write
/// back to the client (possibly multiple, e.g. pipelined queue replies).
pub async fn dispatch_command(session: &mut Session, args: &[Bytes]) -> Vec<RespValue> {
    let mut replies = Vec::new();

    let Some(name) = args.first() else {
        error(session, &mut replies, "ERR empty command");
        return replies;
    };
    let name: Vec<u8> = name.iter().map(u8::to_ascii_lowercase).collect();

    match name.as_slice() {
        b"ping" => match &args[1..] {
            [] => queue_wire(session, &mut replies, PingOp::new(None)),
            [msg] => queue_wire(session, &mut replies, PingOp::new(Some(msg.clone()))),
            _ => error(
                session,
                &mut replies,
                "ERR wrong number of arguments for 'ping' command",
            ),
        },
        b"echo" => match &args[1..] {
            [msg] => queue_wire(session, &mut replies, EchoOp { msg: msg.clone() }),
            _ => error(
                session,
                &mut replies,
                "ERR wrong number of arguments for 'echo' command",
            ),
        },
        b"select" => {
            match &args[1..] {
                [db] => match parse_i64(db) {
                    Some(db) if db >= 0 => {
                        session.switch_db(db as i32);
                        replies.push(ok());
                    }
                    _ => error(session, &mut replies, "ERR invalid DB index"),
                },
                _ => error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'select' command",
                ),
            }
            return replies;
        }
        b"multi" => {
            if session.in_multi() {
                // Nested MULTI is an error but does NOT abort the outer
                // transaction, so it bypasses dirty tracking.
                replies.push(RespValue::Error(Bytes::from_static(
                    b"MULTI calls can not be nested",
                )));
            } else {
                session.enter_multi();
                replies.push(ok());
            }
            return replies;
        }
        b"exec" => match session.exit_multi(false) {
            Ok(()) => replies.extend(session.dispatch_pending_ops(true).await),
            Err(_) => error(session, &mut replies, "ERR EXEC without MULTI"),
        },
        b"discard" => match session.exit_multi(true) {
            Ok(()) => replies.push(ok()),
            Err(_) => error(session, &mut replies, "DISCARD without MULTI"),
        },
        b"set" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'set' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::set(session, key, value)) {
                replies.push(queued);
            }
        }
        b"get" => {
            let Some(key) = args.get(1) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'get' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::get(session, key)) {
                replies.push(queued);
            }
        }
        _ => {
            error(
                session,
                &mut replies,
                format!("ERR unknown command '{}'", String::from_utf8_lossy(&name)),
            );
            return replies;
        }
    }

    replies.extend(session.dispatch_pending_ops(false).await);
    replies
}

/// `+OK`.
fn ok() -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

/// Pushes an error reply, flagging the current MULTI transaction as dirty
/// (matching Redis's CLIENT_DIRTY_EXEC) whenever one is in progress.
fn error(session: &mut Session, replies: &mut Vec<RespValue>, msg: impl Into<Bytes>) {
    if session.in_multi() {
        session.mark_dirty();
    }
    replies.push(RespValue::Error(msg.into()));
}

/// Enqueues a wire-only op, recording the `+QUEUED` reply if in MULTI.
fn queue_wire(session: &mut Session, replies: &mut Vec<RespValue>, wire_op: impl WireOp + 'static) {
    if let Some(queued) = session.enqueue_wire_op(Box::new(wire_op)) {
        replies.push(queued);
    }
}

/// Parses a command's arguments as exactly one `(key, value)` pair.
fn parse_pair(args: &[Bytes]) -> Option<(&Bytes, &Bytes)> {
    match args {
        [key, value] => Some((key, value)),
        _ => None,
    }
}

/// Parses a base-10 signed 64-bit integer, rejecting trailing garbage.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// `PING [message]` — replies `+PONG`, or the message as a bulk string.
pub struct PingOp {
    msg: Option<Bytes>,
}

impl PingOp {
    fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }
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
    use crate::testutil::test_session;

    /// Calls `dispatch_command` with string args; panics on protocol errors.
    async fn dispatch(session: &mut Session, args: &[&str]) -> Vec<RespValue> {
        let args: Vec<Bytes> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        dispatch_command(session, &args).await
    }

    #[tokio::test]
    async fn ping_and_echo() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["ping"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"PONG"))]
        );
        assert_eq!(
            dispatch(&mut session, &["ping", "hi"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"hi")))]
        );
        assert_eq!(
            dispatch(&mut session, &["echo", "hello"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"hello")))]
        );
        assert!(matches!(
            dispatch(&mut session, &["echo"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn set_then_get_roundtrip() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"bar")))]
        );
        // Overwrite with a binary-ish value.
        dispatch(&mut session, &["set", "foo", "baz!"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"baz!")))]
        );
    }

    #[tokio::test]
    async fn get_missing_key_is_null() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["get", "nope"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn wrong_arity_is_an_error() {
        let mut session = test_session();
        assert!(matches!(
            dispatch(&mut session, &["get"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["set", "a"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn multi_exec_batches_commands() {
        let mut session = test_session();
        assert_eq!(dispatch(&mut session, &["multi"]).await, vec![ok()]);
        assert_eq!(
            dispatch(&mut session, &["set", "foo", "bar"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![
                ok(),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))]
        );
        // The writes must actually be persisted.
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"bar")))]
        );
    }

    #[tokio::test]
    async fn discard_drops_the_queue() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        assert_eq!(dispatch(&mut session, &["discard"]).await, vec![ok()]);
        assert!(matches!(
            dispatch(&mut session, &["discard"]).await[0],
            RespValue::Error(_)
        ));
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn exec_without_multi_is_an_error() {
        let mut session = test_session();
        assert!(matches!(
            dispatch(&mut session, &["exec"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn unknown_command_aborts_exec() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        let err = dispatch(&mut session, &["notacommand"]).await;
        assert!(matches!(err[0], RespValue::Error(_)));
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors."
            ))]
        );
    }

    #[tokio::test]
    async fn nested_multi_errors_do_not_abort() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert!(matches!(
            dispatch(&mut session, &["multi"]).await[0],
            RespValue::Error(_)
        ));
        assert_eq!(
            dispatch(&mut session, &["set", "foo", "bar"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![ok()]))]
        );
    }

    #[tokio::test]
    async fn select_switches_database() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "foo", "one"]).await;
        assert_eq!(dispatch(&mut session, &["select", "1"]).await, vec![ok()]);
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(None)]
        );
        dispatch(&mut session, &["select", "0"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"one")))]
        );
    }

    #[tokio::test]
    async fn select_rejects_bad_indices() {
        let mut session = test_session();
        for bad in ["abc", "-1", "1.5"] {
            assert!(matches!(
                dispatch(&mut session, &["select", bad]).await[0],
                RespValue::Error(_)
            ));
        }
    }
}
