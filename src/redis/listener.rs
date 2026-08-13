//! Tokio TCP server speaking RESP, the Rust counterpart of the Go
//! `redis.RedisListener`.
//!
//! Each accepted connection builds a per-connection [`Session`] (holding the
//! current DB, `MULTI` state and the op queue) and frames incoming bytes with
//! [`RespDecoder`]. Commands are routed through [`dispatch_command`], which
//! enqueues ops on the session and flushes them through a transaction against
//! the shared store.

use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::commands::dispatch_command;
use crate::common::{RedisStore, Session, WatchRegistry};
use crate::resp::{RespDecoder, RespError, RespValue};

/// Accepts RESP connections on a TCP address.
pub struct RedisListener {
    addr: std::net::SocketAddr,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
}

impl RedisListener {
    pub fn new(addr: std::net::SocketAddr, store: Arc<dyn RedisStore>) -> Self {
        Self {
            addr,
            store,
            registry: Arc::new(WatchRegistry::new()),
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
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store, registry).await {
                    eprintln!("invar: connection error: {e}");
                }
            });
        }
    }
}

/// Serves a single connection until it closes. A protocol violation is
/// answered with an error and the connection is dropped, as a real Redis
/// server would.
async fn handle_connection(
    socket: TcpStream,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
) -> Result<(), RespError> {
    let mut session = Session::new(store, registry);
    let mut framed = Framed::new(socket, RespDecoder::new());
    while let Some(frame) = framed.next().await {
        match frame {
            Ok(RespValue::Array(Some(elements))) => {
                // A command is an array of bulk strings; coerce each element.
                let mut args = Vec::with_capacity(elements.len());
                let mut well_formed = true;
                for element in elements {
                    match element {
                        RespValue::BulkString(Some(bytes)) | RespValue::SimpleString(bytes) => {
                            args.push(bytes);
                        }
                        _ => {
                            well_formed = false;
                            break;
                        }
                    }
                }
                if !well_formed {
                    framed
                        .send(RespValue::Error(Bytes::from_static(
                            b"ERR command arguments must be bulk strings",
                        )))
                        .await?;
                    continue;
                }
                let replies = dispatch_command(&mut session, &args).await;
                for reply in replies {
                    framed.send(reply).await?;
                }
            }
            Ok(_other) => {
                framed
                    .send(RespValue::Error(Bytes::from_static(
                        b"ERR expected a request array",
                    )))
                    .await?;
            }
            Err(e) => {
                let _ = framed
                    .send(RespValue::Error(Bytes::from_static(b"ERR protocol error")))
                    .await;
                return Err(e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::Framed;

    use super::*;

    /// Connects to `addr`, retrying until the listener accepts.
    async fn connect_retry(addr: std::net::SocketAddr) -> TcpStream {
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("listener never came up on {addr}");
    }

    fn test_store() -> Arc<dyn RedisStore> {
        crate::testutil::test_store()
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
            replies.push(framed.next().await.unwrap().unwrap());
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
}
