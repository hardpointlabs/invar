//! Tokio TCP server speaking RESP, the Rust counterpart of the Go
//! `redis.RedisListener`.
//!
//! Each accepted connection builds a per-connection [`Session`] (holding the
//! current DB, `MULTI` state and the op queue) and frames incoming bytes with
//! [`RespDecoder`]. Commands are routed through [`dispatch_command`], which
//! enqueues ops on the session and flushes them through a transaction against
//! the shared store.
//!
//! ## Pub/Sub loop
//!
//! When a connection has active pub/sub subscriptions it enters a special
//! `select!` loop that multiplexes two sources onto the same socket:
//!
//! 1. Incoming commands from the client (`framed.next()`).
//! 2. Broadcast messages pushed by `PUBLISH` on any subscribed channel or
//!    pattern (`subs.channel_streams.next()` / `subs.pattern_streams.next()`).
//!
//! ### Cancellation safety
//!
//! We use `biased` select! to give message-push branches priority and avoid
//! the scenario where a partially-polled `StreamMap::next()` future is
//! dropped on the floor when the command branch fires first.  In practice,
//! the `biased` annotation means that if a pushed message is already in the
//! `StreamMap` buffer it is sent before we read the next command — this is the
//! correct observable ordering for a well-behaved pub/sub client anyway.
//!
//! References:
//! - <https://smallcultfollowing.com/babysteps/blog/2022/06/13/async-cancellation-a-case-study-of-pub-sub-in-mini-redis/>

use std::sync::Arc;

use bytes::Bytes;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt as TokioStreamExt;
use tokio_util::codec::Framed;

use crate::commands::dispatch_command;
use crate::common::{RedisStore, Session, WatchRegistry};
use crate::pubsub::{
    message_resp, pmessage_resp, psubscribe_resp, punsubscribe_resp, ssubscribe_resp,
    subscribe_resp, sunsubscribe_resp, unsubscribe_resp, ConnectionSubs, PubSubRegistry,
};
use crate::resp::{RespDecoder, RespError, RespValue};

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

/// Accepts RESP connections on a TCP address.
pub struct RedisListener {
    addr: std::net::SocketAddr,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    pubsub: Arc<PubSubRegistry>,
}

impl RedisListener {
    pub fn new(addr: std::net::SocketAddr, store: Arc<dyn RedisStore>) -> Self {
        Self {
            addr,
            store,
            registry: Arc::new(WatchRegistry::new()),
            pubsub: Arc::new(PubSubRegistry::new()),
        }
    }

    /// Binds and accepts connections forever. Each connection is handled on
    /// its own Tokio task.
    pub async fn serve(self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        println!("invar: redis listener on {}", self.addr);
        loop {
            let (socket, _peer) = listener.accept().await?;
            let store = self.store.clone();
            let registry = self.registry.clone();
            let pubsub = self.pubsub.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store, registry, pubsub).await {
                    eprintln!("invar: connection error: {e}");
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Serves a single connection until it closes. A protocol violation is
/// answered with an error and the connection is dropped, as a real Redis
/// server would.
async fn handle_connection(
    socket: TcpStream,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    pubsub: Arc<PubSubRegistry>,
) -> Result<(), RespError> {
    let mut session = Session::new_with_pubsub(store, registry, pubsub.clone());
    let mut framed = Framed::new(socket, RespDecoder::new());
    let mut subs = ConnectionSubs::new(pubsub.clone());

    loop {
        if subs.is_subscribed() {
            // ----------------------------------------------------------------
            // Subscribe mode: biased select! gives push-messages priority.
            // Using `biased` prevents the StreamMap future from being cancelled
            // mid-poll when a command arrives at the same time.
            // ----------------------------------------------------------------
            tokio::select! {
                biased;

                // Branch 1: pushed message from an exact-channel subscription.
                Some((key, result)) = TokioStreamExt::next(&mut subs.channel_streams) => {
                    match result {
                        Ok(msg) => {
                            framed.send(message_resp(msg.channel, msg.payload)).await?;
                        }
                        Err(_lagged) => {
                            // Client fell behind; we skip the gap silently
                            // (matching Redis's behaviour for slow subscribers).
                        }
                    }
                    let _ = key; // consumed by StreamMap
                }

                // Branch 2: pushed message from a pattern subscription.
                Some((pattern_key, result)) = TokioStreamExt::next(&mut subs.pattern_streams) => {
                    match result {
                        Ok(msg) => {
                            framed.send(pmessage_resp(pattern_key, msg.channel, msg.payload)).await?;
                        }
                        Err(_lagged) => {}
                    }
                }

                // Branch 3: incoming command from the client.
                frame = TokioStreamExt::next(&mut framed) => {
                    match frame {
                        None => {
                            // Connection closed — clean up all subscriptions.
                            subs.unsubscribe_all_channels();
                            subs.unsubscribe_all_patterns();
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            let _ = framed.send(RespValue::Error(Bytes::from_static(b"ERR protocol error"))).await;
                            subs.unsubscribe_all_channels();
                            subs.unsubscribe_all_patterns();
                            return Err(e);
                        }
                        Some(Ok(RespValue::Array(Some(elements)))) => {
                            let args = match extract_args(elements) {
                                Some(a) => a,
                                None => {
                                    framed.send(RespValue::Error(Bytes::from_static(
                                        b"ERR command arguments must be bulk strings",
                                    ))).await?;
                                    continue;
                                }
                            };
                            let should_close = handle_subscribe_mode_command(
                                &mut framed, &mut session, &mut subs, &args,
                            ).await?;
                            if should_close {
                                subs.unsubscribe_all_channels();
                                subs.unsubscribe_all_patterns();
                                return Ok(());
                            }
                        }
                        Some(Ok(_other)) => {
                            framed.send(RespValue::Error(Bytes::from_static(
                                b"ERR expected a request array",
                            ))).await?;
                        }
                    }
                }
            }
        } else {
            // ----------------------------------------------------------------
            // Normal (non-subscribed) mode: just process commands.
            // ----------------------------------------------------------------
            match TokioStreamExt::next(&mut framed).await {
                None => return Ok(()),
                Some(Err(e)) => {
                    let _ = framed
                        .send(RespValue::Error(Bytes::from_static(b"ERR protocol error")))
                        .await;
                    return Err(e);
                }
                Some(Ok(RespValue::Array(Some(elements)))) => {
                    let args = match extract_args(elements) {
                        Some(a) => a,
                        None => {
                            framed
                                .send(RespValue::Error(Bytes::from_static(
                                    b"ERR command arguments must be bulk strings",
                                )))
                                .await?;
                            continue;
                        }
                    };

                    // Handle subscribe-family commands that initiate subscribe mode.
                    // If we are inside a MULTI block, all subscribe-family commands
                    // must be rejected immediately (dirtying the transaction), so
                    // we route them through dispatch_command which handles that case.
                    let name_lower: Vec<u8> = args
                        .first()
                        .map(|b| b.iter().map(u8::to_ascii_lowercase).collect())
                        .unwrap_or_default();

                    let is_sub_cmd = matches!(
                        name_lower.as_slice(),
                        b"subscribe"
                            | b"psubscribe"
                            | b"ssubscribe"
                            | b"unsubscribe"
                            | b"punsubscribe"
                            | b"sunsubscribe"
                    );

                    if is_sub_cmd && session.in_multi() {
                        // Inside MULTI: route through dispatch_command which will
                        // dirty the transaction and return an error.
                        let replies = dispatch_command(&mut session, &args).await;
                        for reply in replies {
                            framed.send(reply).await?;
                        }
                    } else {
                        match name_lower.as_slice() {
                            b"subscribe" => {
                                handle_subscribe(&mut framed, &mut subs, &args[1..], false, false).await?;
                            }
                            b"psubscribe" => {
                                handle_psubscribe(&mut framed, &mut subs, &args[1..]).await?;
                            }
                            b"ssubscribe" => {
                                // SSUBSCRIBE is treated the same as SUBSCRIBE in
                                // single-node mode.
                                handle_subscribe(&mut framed, &mut subs, &args[1..], false, true).await?;
                            }
                            b"unsubscribe" | b"punsubscribe" | b"sunsubscribe" => {
                                // Called outside subscribe mode with no subscriptions;
                                // real Redis replies with a null-unsubscribe frame.
                                let kind_byte = if name_lower.as_slice() == b"punsubscribe" {
                                    b"punsubscribe".as_ref()
                                } else if name_lower.as_slice() == b"sunsubscribe" {
                                    b"sunsubscribe".as_ref()
                                } else {
                                    b"unsubscribe".as_ref()
                                };
                                framed.send(RespValue::Array(Some(vec![
                                    RespValue::BulkString(Some(Bytes::copy_from_slice(kind_byte))),
                                    RespValue::BulkString(None),
                                    RespValue::Integer(0),
                                ]))).await?;
                            }
                            _ => {
                                // Normal command dispatch.
                                let replies = dispatch_command(&mut session, &args).await;
                                for reply in replies {
                                    framed.send(reply).await?;
                                }
                                if session.should_close() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                Some(Ok(_other)) => {
                    framed
                        .send(RespValue::Error(Bytes::from_static(
                            b"ERR expected a request array",
                        )))
                        .await?;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Subscribe-mode command handler
// ---------------------------------------------------------------------------

/// Handles commands received while the connection is in subscribe mode.
///
/// Only `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`,
/// `PUNSUBSCRIBE`, `SUNSUBSCRIBE`, `PING`, and `QUIT` are allowed.
///
/// Returns `true` if the connection should be closed.
async fn handle_subscribe_mode_command(
    framed: &mut Framed<TcpStream, RespDecoder>,
    session: &mut Session,
    subs: &mut ConnectionSubs,
    args: &[Bytes],
) -> Result<bool, RespError> {
    let name_lower: Vec<u8> = args
        .first()
        .map(|b| b.iter().map(u8::to_ascii_lowercase).collect())
        .unwrap_or_default();

    match name_lower.as_slice() {
        b"subscribe" => {
            handle_subscribe(framed, subs, &args[1..], false, false).await?;
        }
        b"ssubscribe" => {
            handle_subscribe(framed, subs, &args[1..], false, true).await?;
        }
        b"psubscribe" => {
            handle_psubscribe(framed, subs, &args[1..]).await?;
        }
        b"unsubscribe" => {
            handle_unsubscribe(framed, subs, &args[1..], false).await?;
        }
        b"sunsubscribe" => {
            handle_unsubscribe(framed, subs, &args[1..], true).await?;
        }
        b"punsubscribe" => {
            handle_punsubscribe(framed, subs, &args[1..]).await?;
        }
        b"ping" => {
            // PING in subscribe mode replies with a push-style array, not the
            // plain +PONG, when no message is given; with a message it echoes
            // the message. This matches real Redis RESP2 subscribe-mode PING.
            let payload = args.get(1).cloned().unwrap_or_else(|| Bytes::from_static(b""));
            framed
                .send(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"pong"))),
                    RespValue::BulkString(Some(payload)),
                ])))
                .await?;
        }
        b"quit" => {
            framed
                .send(RespValue::SimpleString(Bytes::from_static(b"OK")))
                .await?;
            return Ok(true);
        }
        b"reset" => {
            // TODO: full RESET implementation (out of scope for this pass).
            // For now, unsubscribe everything and reply +RESET.
            subs.unsubscribe_all_channels();
            subs.unsubscribe_all_patterns();
            session.request_close();
            framed
                .send(RespValue::SimpleString(Bytes::from_static(b"RESET")))
                .await?;
        }
        _ => {
            framed
                .send(RespValue::Error(Bytes::from_static(
                    b"ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                )))
                .await?;
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / PSUBSCRIBE helpers
// ---------------------------------------------------------------------------

/// Processes a `SUBSCRIBE` or `SSUBSCRIBE` command.  Emits one reply frame
/// per channel, as Redis specifies.
async fn handle_subscribe(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subs: &mut ConnectionSubs,
    channels: &[Bytes],
    _is_pattern: bool,
    use_ssubscribe_frame: bool,
) -> Result<(), RespError> {
    for ch in channels {
        subs.subscribe_channel(ch.clone());
        let total = subs.total();
        let frame = if use_ssubscribe_frame {
            ssubscribe_resp(ch.clone(), total)
        } else {
            subscribe_resp(ch.clone(), total)
        };
        framed.send(frame).await?;
    }
    Ok(())
}

/// Processes a `PSUBSCRIBE` command.  Emits one reply frame per pattern.
async fn handle_psubscribe(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subs: &mut ConnectionSubs,
    patterns: &[Bytes],
) -> Result<(), RespError> {
    for pat in patterns {
        subs.subscribe_pattern(pat.clone());
        let total = subs.total();
        framed.send(psubscribe_resp(pat.clone(), total)).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// UNSUBSCRIBE / PUNSUBSCRIBE helpers
// ---------------------------------------------------------------------------

/// Processes an `UNSUBSCRIBE` or `SUNSUBSCRIBE` command.
///
/// With explicit channel names: emits one frame per channel.
/// With no arguments: unsubscribes from all, emits one frame per channel, or
/// a single null-channel frame if there were none.
async fn handle_unsubscribe(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subs: &mut ConnectionSubs,
    channels: &[Bytes],
    use_sunsubscribe_frame: bool,
) -> Result<(), RespError> {
    if channels.is_empty() {
        // Unsubscribe-all.
        let all: Vec<Bytes> = subs.channel_streams.keys().cloned().collect();
        if all.is_empty() {
            // Real Redis sends a null-channel frame with count 0.
            let frame = if use_sunsubscribe_frame {
                sunsubscribe_resp(Bytes::from_static(b""), 0)
            } else {
                unsubscribe_resp(Bytes::from_static(b""), 0)
            };
            // Match real Redis: BulkString(None) for the channel field.
            framed
                .send(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(if use_sunsubscribe_frame {
                        Bytes::from_static(b"sunsubscribe")
                    } else {
                        Bytes::from_static(b"unsubscribe")
                    })),
                    RespValue::BulkString(None),
                    RespValue::Integer(0),
                ])))
                .await?;
            let _ = frame; // silence unused warning
        } else {
            for ch in &all {
                subs.unsubscribe_channel(ch);
                let total = subs.total();
                let frame = if use_sunsubscribe_frame {
                    sunsubscribe_resp(ch.clone(), total)
                } else {
                    unsubscribe_resp(ch.clone(), total)
                };
                framed.send(frame).await?;
            }
        }
    } else {
        // Explicit channels.
        for ch in channels {
            subs.unsubscribe_channel(ch);
            let total = subs.total();
            let frame = if use_sunsubscribe_frame {
                sunsubscribe_resp(ch.clone(), total)
            } else {
                unsubscribe_resp(ch.clone(), total)
            };
            framed.send(frame).await?;
        }
    }
    Ok(())
}

/// Processes a `PUNSUBSCRIBE` command.
///
/// With explicit patterns: emits one frame per pattern.
/// With no arguments: unsubscribes from all patterns.
async fn handle_punsubscribe(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subs: &mut ConnectionSubs,
    patterns: &[Bytes],
) -> Result<(), RespError> {
    if patterns.is_empty() {
        let all: Vec<Bytes> = subs.pattern_streams.keys().cloned().collect();
        if all.is_empty() {
            framed
                .send(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
                    RespValue::BulkString(None),
                    RespValue::Integer(0),
                ])))
                .await?;
        } else {
            for pat in &all {
                subs.unsubscribe_pattern(pat);
                let total = subs.total();
                framed.send(punsubscribe_resp(pat.clone(), total)).await?;
            }
        }
    } else {
        for pat in patterns {
            subs.unsubscribe_pattern(pat);
            let total = subs.total();
            framed.send(punsubscribe_resp(pat.clone(), total)).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts a `Vec<Bytes>` of bulk strings from a RESP array's elements.
/// Returns `None` if any element is not a bulk/simple string.
fn extract_args(elements: Vec<RespValue>) -> Option<Vec<Bytes>> {
    let mut args = Vec::with_capacity(elements.len());
    for element in elements {
        match element {
            RespValue::BulkString(Some(bytes)) | RespValue::SimpleString(bytes) => {
                args.push(bytes);
            }
            _ => return None,
        }
    }
    Some(args)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt as FuturesStreamExt};
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::Framed;

    use super::*;

    /// Connects to `addr`, retrying until the listener accepts.
    async fn connect_retry(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
        for _ in 0..100 {
            if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("listener never came up on {addr}");
    }

    fn test_store() -> Arc<dyn RedisStore> {
        crate::testutil::test_store()
    }

    /// Sends a raw RESP array command.
    async fn send_cmd(w: &mut tokio::net::TcpStream, parts: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            buf.extend_from_slice(p);
            buf.extend_from_slice(b"\r\n");
        }
        w.write_all(&buf).await.unwrap();
    }

    /// Reads the next decoded RESP value from a framed stream.
    async fn next_reply(framed: &mut Framed<tokio::net::TcpStream, RespDecoder>) -> RespValue {
        tokio::time::timeout(Duration::from_secs(5), FuturesStreamExt::next(framed))
            .await
            .expect("timeout waiting for reply")
            .expect("stream ended")
            .expect("decode error")
    }

    #[tokio::test]
    async fn serves_ping_set_get_and_multi() {
        // Reserve a free port, then hand it to the listener.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let handle = tokio::spawn(RedisListener::new(addr, test_store()).serve());

        let mut client = connect_retry(addr).await;
        client
            .write_all(
                b"*1\r\n$4\r\nPING\r\n\
                  *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n\
                  *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n\
                  *1\r\n$5\r\nMULTI\r\n\
                  *3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n\
                  *1\r\n$4\r\nEXEC\r\n",
            )
            .await
            .unwrap();

        let mut framed = Framed::new(client, RespDecoder::new());
        let mut replies = Vec::new();
        for _ in 0..6 {
            replies.push(FuturesStreamExt::next(&mut framed).await.unwrap().unwrap());
        }

        assert_eq!(
            replies[0],
            RespValue::SimpleString(Bytes::from_static(b"PONG"))
        );
        assert_eq!(
            replies[1],
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            replies[2],
            RespValue::BulkString(Some(Bytes::from_static(b"bar")))
        );
        assert_eq!(
            replies[3],
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            replies[4],
            RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
        );
        assert_eq!(
            replies[5],
            RespValue::Array(Some(vec![RespValue::SimpleString(Bytes::from_static(
                b"OK"
            ))]))
        );

        handle.abort();
    }

    // ----------------------------------------------------------------
    // Pub/Sub integration tests
    // ----------------------------------------------------------------

    async fn spawn_listener() -> (std::net::SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>) {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = tokio::spawn(RedisListener::new(addr, test_store()).serve());
        // Brief pause to let the listener start accepting.
        tokio::time::sleep(Duration::from_millis(5)).await;
        (addr, handle)
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_returns_zero() {
        let (addr, handle) = spawn_listener().await;
        let mut pub_conn = connect_retry(addr).await;
        send_cmd(&mut pub_conn, &[b"PUBLISH", b"ch", b"msg"]).await;
        let mut framed = Framed::new(pub_conn, RespDecoder::new());
        let reply = next_reply(&mut framed).await;
        assert_eq!(reply, RespValue::Integer(0));
        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_and_receive_message() {
        let (addr, handle) = spawn_listener().await;

        // Subscriber.
        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());

        // Publisher (separate connection).
        let mut pub_conn = connect_retry(addr).await;

        // Subscribe to "news".
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"news"))),
        ])))
        .await
        .unwrap();

        // Expect subscribe confirmation.
        let confirm = next_reply(&mut sub).await;
        match &confirm {
            RespValue::Array(Some(v)) => {
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"subscribe"))));
                assert_eq!(v[1], RespValue::BulkString(Some(Bytes::from_static(b"news"))));
                assert_eq!(v[2], RespValue::Integer(1));
            }
            _ => panic!("unexpected reply: {confirm:?}"),
        }

        // Publish.
        send_cmd(&mut pub_conn, &[b"PUBLISH", b"news", b"breaking"]).await;
        let mut pub_framed = Framed::new(pub_conn, RespDecoder::new());
        let pub_reply = next_reply(&mut pub_framed).await;
        assert_eq!(pub_reply, RespValue::Integer(1));

        // Subscriber receives the message.
        let msg = next_reply(&mut sub).await;
        match &msg {
            RespValue::Array(Some(v)) => {
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"message"))));
                assert_eq!(v[1], RespValue::BulkString(Some(Bytes::from_static(b"news"))));
                assert_eq!(v[2], RespValue::BulkString(Some(Bytes::from_static(b"breaking"))));
            }
            _ => panic!("unexpected message push: {msg:?}"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn psubscribe_receives_matching_message() {
        let (addr, handle) = spawn_listener().await;

        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());
        let mut pub_conn = connect_retry(addr).await;

        // PSUBSCRIBE to "news.*".
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"PSUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"news.*"))),
        ])))
        .await
        .unwrap();

        let confirm = next_reply(&mut sub).await;
        match &confirm {
            RespValue::Array(Some(v)) => {
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"psubscribe"))));
                assert_eq!(v[1], RespValue::BulkString(Some(Bytes::from_static(b"news.*"))));
                assert_eq!(v[2], RespValue::Integer(1));
            }
            _ => panic!("unexpected: {confirm:?}"),
        }

        // Publish to "news.sports".
        send_cmd(&mut pub_conn, &[b"PUBLISH", b"news.sports", b"goal"]).await;
        let mut pf = Framed::new(pub_conn, RespDecoder::new());
        let pub_reply = next_reply(&mut pf).await;
        assert_eq!(pub_reply, RespValue::Integer(1));

        // Subscriber receives pmessage.
        let msg = next_reply(&mut sub).await;
        match &msg {
            RespValue::Array(Some(v)) => {
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"pmessage"))));
                assert_eq!(v[1], RespValue::BulkString(Some(Bytes::from_static(b"news.*"))));
                assert_eq!(v[2], RespValue::BulkString(Some(Bytes::from_static(b"news.sports"))));
                assert_eq!(v[3], RespValue::BulkString(Some(Bytes::from_static(b"goal"))));
            }
            _ => panic!("unexpected: {msg:?}"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn non_allowed_command_in_subscribe_mode_is_rejected() {
        let (addr, handle) = spawn_listener().await;
        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());

        // Subscribe first.
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ch"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub).await; // consume subscribe confirmation

        // Try a disallowed command.
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ])))
        .await
        .unwrap();

        let reply = next_reply(&mut sub).await;
        match &reply {
            RespValue::Error(msg) => {
                assert!(
                    msg.as_ref().starts_with(b"ERR only"),
                    "got: {}",
                    String::from_utf8_lossy(msg)
                );
            }
            _ => panic!("expected error, got: {reply:?}"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn unsubscribe_no_args_removes_all_and_sends_frames() {
        let (addr, handle) = spawn_listener().await;
        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());

        // Subscribe to two channels.
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ch1"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ch2"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub).await; // ch1 confirm
        next_reply(&mut sub).await; // ch2 confirm

        // Unsubscribe with no args.
        sub.send(RespValue::Array(Some(vec![RespValue::BulkString(Some(
            Bytes::from_static(b"UNSUBSCRIBE"),
        ))])))
        .await
        .unwrap();

        // Should get two unsubscribe frames (order may vary, but counts must
        // be 1 then 0, or similar decrement pattern).
        let r1 = next_reply(&mut sub).await;
        let r2 = next_reply(&mut sub).await;

        let counts: Vec<i64> = [&r1, &r2]
            .iter()
            .map(|r| match r {
                RespValue::Array(Some(v)) => match &v[2] {
                    RespValue::Integer(n) => *n,
                    _ => panic!("expected integer"),
                },
                _ => panic!("expected array"),
            })
            .collect();
        // One frame with count 1, one with count 0.
        let mut sorted = counts.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1]);

        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_mode_ping_works() {
        let (addr, handle) = spawn_listener().await;
        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());

        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ch"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub).await;

        // PING in subscribe mode.
        sub.send(RespValue::Array(Some(vec![RespValue::BulkString(Some(
            Bytes::from_static(b"PING"),
        ))])))
        .await
        .unwrap();

        let pong = next_reply(&mut sub).await;
        match &pong {
            RespValue::Array(Some(v)) => {
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"pong"))));
                // payload is empty
                assert_eq!(v[1], RespValue::BulkString(Some(Bytes::from_static(b""))));
            }
            _ => panic!("expected pong array, got {pong:?}"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn pubsub_channels_numsub_numpat_via_commands() {
        let (addr, handle) = spawn_listener().await;

        // Two subscribers.
        let sub1_sock = connect_retry(addr).await;
        let mut sub1 = Framed::new(sub1_sock, RespDecoder::new());
        let sub2_sock = connect_retry(addr).await;
        let mut sub2 = Framed::new(sub2_sock, RespDecoder::new());

        // A publisher/introspection connection.
        let mut ctrl = connect_retry(addr).await;

        // sub1 subscribes to "sports" and pattern "n*".
        sub1.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"sports"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub1).await;

        sub1.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"PSUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"n*"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub1).await;

        // sub2 subscribes to "sports".
        sub2.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"sports"))),
        ])))
        .await
        .unwrap();
        next_reply(&mut sub2).await;

        // Give the subscriptions a moment to propagate.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // PUBSUB CHANNELS
        send_cmd(&mut ctrl, &[b"PUBSUB", b"CHANNELS"]).await;
        let mut cf = Framed::new(ctrl, RespDecoder::new());
        let ch_reply = next_reply(&mut cf).await;
        let ch_names: Vec<Bytes> = match &ch_reply {
            RespValue::Array(Some(v)) => v
                .iter()
                .filter_map(|r| match r {
                    RespValue::BulkString(Some(b)) => Some(b.clone()),
                    _ => None,
                })
                .collect(),
            _ => panic!("expected array, got {ch_reply:?}"),
        };
        assert!(ch_names.contains(&Bytes::from_static(b"sports")));

        // PUBSUB NUMSUB sports
        send_cmd(cf.get_mut(), &[b"PUBSUB", b"NUMSUB", b"sports"]).await;
        let ns_reply = next_reply(&mut cf).await;
        match &ns_reply {
            RespValue::Array(Some(v)) => {
                // [channel, count]
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], RespValue::BulkString(Some(Bytes::from_static(b"sports"))));
                assert_eq!(v[1], RespValue::Integer(2));
            }
            _ => panic!("expected array, got {ns_reply:?}"),
        }

        // PUBSUB NUMPAT
        send_cmd(cf.get_mut(), &[b"PUBSUB", b"NUMPAT"]).await;
        let np_reply = next_reply(&mut cf).await;
        assert_eq!(np_reply, RespValue::Integer(1));

        handle.abort();
    }

    #[tokio::test]
    async fn multi_with_subscribe_command_aborts_transaction() {
        let (addr, handle) = spawn_listener().await;
        let conn_sock = connect_retry(addr).await;
        let mut conn = Framed::new(conn_sock, RespDecoder::new());

        // MULTI
        conn.send(RespValue::Array(Some(vec![RespValue::BulkString(Some(
            Bytes::from_static(b"MULTI"),
        ))])))
        .await
        .unwrap();
        let r = next_reply(&mut conn).await;
        assert_eq!(r, RespValue::SimpleString(Bytes::from_static(b"OK")));

        // Queue a SET (observable side-effect).
        conn.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"txkey"))),
            RespValue::BulkString(Some(Bytes::from_static(b"val"))),
        ])))
        .await
        .unwrap();
        let queued = next_reply(&mut conn).await;
        assert_eq!(
            queued,
            RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
        );

        // Queue a SUBSCRIBE — must be rejected and dirty the tx.
        conn.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ch"))),
        ])))
        .await
        .unwrap();
        let sub_err = next_reply(&mut conn).await;
        assert!(
            matches!(&sub_err, RespValue::Error(_)),
            "expected error, got {sub_err:?}"
        );

        // EXEC must return EXECABORT.
        conn.send(RespValue::Array(Some(vec![RespValue::BulkString(Some(
            Bytes::from_static(b"EXEC"),
        ))])))
        .await
        .unwrap();
        let exec_reply = next_reply(&mut conn).await;
        assert!(
            matches!(&exec_reply, RespValue::Error(msg) if msg.starts_with(b"EXECABORT")),
            "got {exec_reply:?}"
        );

        // Confirm SET did NOT execute.
        conn.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"txkey"))),
        ])))
        .await
        .unwrap();
        let get = next_reply(&mut conn).await;
        assert_eq!(get, RespValue::BulkString(None));

        handle.abort();
    }

    #[tokio::test]
    async fn publish_counts_multiple_subscribers() {
        let (addr, handle) = spawn_listener().await;

        let sub1_sock = connect_retry(addr).await;
        let mut sub1 = Framed::new(sub1_sock, RespDecoder::new());
        let sub2_sock = connect_retry(addr).await;
        let mut sub2 = Framed::new(sub2_sock, RespDecoder::new());
        let mut pub_conn = connect_retry(addr).await;

        for sub in [&mut sub1, &mut sub2] {
            sub.send(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
                RespValue::BulkString(Some(Bytes::from_static(b"shared"))),
            ])))
            .await
            .unwrap();
            next_reply(sub).await; // consume confirmation
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        send_cmd(&mut pub_conn, &[b"PUBLISH", b"shared", b"hi"]).await;
        let mut pf = Framed::new(pub_conn, RespDecoder::new());
        let pub_reply = next_reply(&mut pf).await;
        assert_eq!(pub_reply, RespValue::Integer(2));

        // Both subscribers receive the message.
        for sub in [&mut sub1, &mut sub2] {
            let msg = tokio::time::timeout(Duration::from_secs(2), FuturesStreamExt::next(sub))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match &msg {
                RespValue::Array(Some(v)) => {
                    assert_eq!(
                        v[0],
                        RespValue::BulkString(Some(Bytes::from_static(b"message")))
                    );
                }
                _ => panic!("expected message push, got {msg:?}"),
            }
        }

        handle.abort();
    }

    #[tokio::test]
    async fn ssubscribe_and_spublish() {
        let (addr, handle) = spawn_listener().await;

        let sub_sock = connect_retry(addr).await;
        let mut sub = Framed::new(sub_sock, RespDecoder::new());
        let mut pub_conn = connect_retry(addr).await;

        // SSUBSCRIBE — treated as SUBSCRIBE for single-node invar.
        sub.send(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SSUBSCRIBE"))),
            RespValue::BulkString(Some(Bytes::from_static(b"shardch"))),
        ])))
        .await
        .unwrap();

        let confirm = next_reply(&mut sub).await;
        match &confirm {
            RespValue::Array(Some(v)) => {
                assert_eq!(
                    v[0],
                    RespValue::BulkString(Some(Bytes::from_static(b"ssubscribe")))
                );
            }
            _ => panic!("expected ssubscribe frame, got {confirm:?}"),
        }

        // SPUBLISH — same path as PUBLISH for single-node.
        send_cmd(&mut pub_conn, &[b"SPUBLISH", b"shardch", b"payload"]).await;
        let mut pf = Framed::new(pub_conn, RespDecoder::new());
        let pub_reply = next_reply(&mut pf).await;
        assert_eq!(pub_reply, RespValue::Integer(1));

        let msg = next_reply(&mut sub).await;
        match &msg {
            RespValue::Array(Some(v)) => {
                assert_eq!(
                    v[0],
                    RespValue::BulkString(Some(Bytes::from_static(b"message")))
                );
                assert_eq!(
                    v[1],
                    RespValue::BulkString(Some(Bytes::from_static(b"shardch")))
                );
            }
            _ => panic!("expected message, got {msg:?}"),
        }

        handle.abort();
    }
}
