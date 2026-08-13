//! Tokio TCP server speaking RESP, the Rust counterpart of the Go
//! `redis.RedisListener`.
//!
//! For now each connection frames incoming bytes with [`RespDecoder`] and
//! replies `+OK` to every command; command dispatch against the `kv` store
//! lands here next.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::resp::{RespDecoder, RespError, RespValue};

/// Accepts RESP connections on a TCP address.
pub struct RedisListener {
    addr: std::net::SocketAddr,
}

impl RedisListener {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self { addr }
    }

    /// Binds and accepts connections forever. Each connection is handled on
    /// its own Tokio task.
    pub async fn serve(self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        println!("invar: redis listener on {}", self.addr);
        loop {
            let (socket, _peer) = listener.accept().await?;
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket).await {
                    eprintln!("invar: connection error: {e}");
                }
            });
        }
    }
}

/// Serves a single connection until it closes. Every command is answered
/// with `+OK`; a protocol violation is answered with an error and the
/// connection is dropped, as a real Redis server would.
async fn handle_connection(socket: TcpStream) -> Result<(), RespError> {
    let mut framed = Framed::new(socket, RespDecoder::new());
    while let Some(frame) = framed.next().await {
        match frame {
            Ok(_command) => {
                framed
                    .send(RespValue::SimpleString(Bytes::from_static(b"OK")))
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

    #[tokio::test]
    async fn replies_ok_to_every_command() {
        // Reserve a free port, then hand it to the listener.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let handle = tokio::spawn(RedisListener::new(addr).serve());

        let mut client = connect_retry(addr).await;
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();

        let mut framed = Framed::new(client, RespDecoder::new());
        let first = framed.next().await.unwrap().unwrap();
        assert_eq!(first, RespValue::SimpleString(Bytes::from_static(b"OK")));
        let second = framed.next().await.unwrap().unwrap();
        assert_eq!(second, RespValue::SimpleString(Bytes::from_static(b"OK")));

        handle.abort();
    }
}
