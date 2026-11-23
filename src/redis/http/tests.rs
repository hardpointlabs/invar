//! End-to-end tests: a real listener on loopback, driven with raw HTTP/1.1.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::testutil::test_store;

struct Server {
    addr: SocketAddr,
    shutdown: CancellationToken,
    registry: Arc<WatchRegistry>,
    pubsub: Arc<PubSubRegistry>,
    store: Arc<dyn RedisStore>,
}

async fn start() -> Server {
    let store = test_store();
    let registry = Arc::new(WatchRegistry::new());
    let pubsub = Arc::new(PubSubRegistry::new());
    let http = HttpListener::new(
        "127.0.0.1:0".parse().unwrap(),
        store.clone(),
        registry.clone(),
        pubsub.clone(),
    )
    .with_max_body(1024);
    let shutdown = http.shutdown_token();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(http.serve_on(listener));
    Server {
        addr,
        shutdown,
        registry,
        pubsub,
        store,
    }
}

/// Sends one request on a fresh connection and returns (status, body).
async fn req(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let mut head = format!(
        "{method} {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    sock.write_all(format!("{head}\r\n{body}").as_bytes())
        .await
        .unwrap();
    let mut out = Vec::new();
    sock.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text[9..12].parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|x| x.1)
        .unwrap_or("")
        .to_string();
    (status, body)
}

async fn post(addr: SocketAddr, target: &str, body: &str) -> (u16, Value) {
    let (s, b) = req(addr, "POST", target, &[], body).await;
    (s, serde_json::from_str(&b).unwrap_or(Value::String(b)))
}

#[tokio::test]
async fn single_command_json_and_path_forms() {
    let s = start().await;
    assert_eq!(
        post(s.addr, "/", r#"["SET","k","v"]"#).await,
        (200, json!({"result":"OK"}))
    );
    assert_eq!(
        post(s.addr, "/", r#"["GET","k"]"#).await,
        (200, json!({"result":"v"}))
    );
    assert_eq!(
        post(s.addr, "/", r#"["GET","nope"]"#).await,
        (200, json!({"result":null}))
    );
    let (st, b) = req(
        s.addr,
        "GET",
        "/get/k",
        &[("Authorization", "Bearer x")],
        "",
    )
    .await;
    assert_eq!((st, b.as_str()), (200, r#"{"result":"v"}"#));
    // Body as the last argument, query parameters as trailing arguments.
    let (st, _) = req(s.addr, "POST", "/set/p?EX=100&_token=t", &[], "binary val").await;
    assert_eq!(st, 200);
    assert_eq!(
        post(s.addr, "/", r#"["GET","p"]"#).await.1,
        json!({"result":"binary val"})
    );
    let ttl = post(s.addr, "/", r#"["TTL","p"]"#).await.1["result"]
        .as_i64()
        .unwrap();
    assert!((99..=100).contains(&ttl), "ttl {ttl}");
    // Empty path segments are tolerated.
    assert_eq!(post(s.addr, "//pipeline", r#"[["GET","k"]]"#).await.0, 200);
    s.shutdown.cancel();
}

#[tokio::test]
async fn errors_and_statuses() {
    let s = start().await;
    let (st, v) = post(s.addr, "/", r#"["GET"]"#).await;
    assert_eq!(st, 400);
    assert!(v["error"]
        .as_str()
        .unwrap()
        .starts_with("ERR wrong number of arguments"));
    for body in [
        "not json",
        r#"["SET","k",null]"#,
        r#"["SET","k",true]"#,
        "[]",
    ] {
        assert_eq!(post(s.addr, "/", body).await.0, 400, "{body}");
    }
    // Rejected commands never run.
    for body in [r#"["SELECT",1]"#, r#"["BLPOP","l",0]"#, r#"["MULTI"]"#] {
        let (st, v) = post(s.addr, "/", body).await;
        assert_eq!(st, 400);
        assert!(v["error"]
            .as_str()
            .unwrap()
            .starts_with("ERR unsupported command"));
    }
    assert_eq!(req(s.addr, "DELETE", "/get/a", &[], "").await.0, 405);
    assert_eq!(req(s.addr, "HEAD", "/get/a", &[], "").await.0, 405);
    assert_eq!(req(s.addr, "GET", "/pipeline", &[], "").await.0, 405);
    let (st, b) = req(s.addr, "POST", "/", &[], &"x".repeat(2000)).await;
    assert_eq!(st, 413);
    assert!(b.contains("max request size exceeded"));
    s.shutdown.cancel();
}

#[tokio::test]
async fn pipeline_is_not_atomic_and_keeps_going() {
    let s = start().await;
    let (st, v) = post(
        s.addr,
        "/pipeline",
        r#"[["SET","a","1"],["INCR","a"],["LPUSH","a","x"],["GET","missing"]]"#,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(v[0], json!({"result":"OK"}));
    assert_eq!(v[1], json!({"result":2}));
    assert!(v[2]["error"].as_str().unwrap().starts_with("WRONGTYPE"));
    assert_eq!(v[3], json!({"result":null}));
    assert_eq!(post(s.addr, "/pipeline", "[[\"GET\"],").await.0, 400);
    s.shutdown.cancel();
}

#[tokio::test]
async fn multi_exec_is_atomic_and_discards_on_queue_error() {
    let s = start().await;
    let (st, v) = post(
        s.addr,
        "/multi-exec",
        r#"[["SET","a","1"],["INCR","a"],["LPUSH","a","x"]]"#,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(v[0], json!({"result":"OK"}));
    assert_eq!(v[1], json!({"result":2}));
    assert!(v[2]["error"].is_string());
    // An unsupported or malformed command discards everything.
    let (st, v) = post(s.addr, "/multi-exec", r#"[["SET","b","1"],["NOPE"]]"#).await;
    assert_eq!(st, 400);
    assert!(v["error"].is_string());
    assert_eq!(
        post(s.addr, "/", r#"["GET","b"]"#).await.1,
        json!({"result":null})
    );
    let (st, _) = post(s.addr, "/multi-exec", r#"[["SET","c","1"],["WATCH","c"]]"#).await;
    assert_eq!(st, 400);
    assert_eq!(
        post(s.addr, "/", r#"["GET","c"]"#).await.1,
        json!({"result":null})
    );
    s.shutdown.cancel();
}

#[tokio::test]
async fn base64_encoding() {
    let s = start().await;
    let h = [("Upstash-Encoding", "base64")];
    let (_, b) = req(s.addr, "POST", "/", &h, r#"["SET","k","hi"]"#).await;
    assert_eq!(b, r#"{"result":"OK"}"#);
    let (_, b) = req(s.addr, "POST", "/", &h, r#"["GET","k"]"#).await;
    assert_eq!(b, r#"{"result":"aGk="}"#);
    let (_, b) = req(s.addr, "POST", "/", &h, r#"["PING"]"#).await;
    assert_eq!(b, r#"{"result":"UE9ORw=="}"#);
    let (_, b) = req(
        s.addr,
        "POST",
        "/pipeline",
        &h,
        r#"[["MGET","k","nope"],["NOPE"]]"#,
    )
    .await;
    let v: Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v[0], json!({"result":["aGk=", null]}));
    assert!(v[1]["error"].as_str().unwrap().starts_with("ERR"));
    s.shutdown.cancel();
}

#[tokio::test]
async fn resp2_response_format() {
    let s = start().await;
    let h = [("Upstash-Response-Format", "resp2")];
    let (st, b) = req(s.addr, "POST", "/", &h, r#"["GET","nope"]"#).await;
    assert_eq!((st, b.as_str()), (200, "$-1\r\n"));
    let (_, b) = req(
        s.addr,
        "POST",
        "/pipeline",
        &h,
        r#"[["SET","a",1],["INCR","a"]]"#,
    )
    .await;
    assert_eq!(b, "+OK\r\n:2\r\n");
    let both = [
        ("Upstash-Response-Format", "resp2"),
        ("Upstash-Encoding", "base64"),
    ];
    assert_eq!(req(s.addr, "POST", "/", &both, r#"["PING"]"#).await.0, 400);
    let bad = [("Upstash-Response-Format", "xml")];
    assert_eq!(req(s.addr, "POST", "/", &bad, r#"["PING"]"#).await.0, 400);
    s.shutdown.cancel();
}

/// RFC 0003's RESP2-shape guard: RESP3 reply types must never reach JSON.
#[tokio::test]
async fn replies_keep_their_resp2_shape() {
    let s = start().await;
    let batch = r#"[["HSET","h","f","v"],["ZADD","z",1.5,"m"],["SADD","s","a"]]"#;
    assert_eq!(post(s.addr, "/pipeline", batch).await.0, 200);
    let (_, v) = post(
        s.addr,
        "/pipeline",
        r#"[["HGETALL","h"],["ZRANGE","z",0,-1,"WITHSCORES"],["ZSCORE","z","m"],["SMEMBERS","s"]]"#,
    )
    .await;
    assert_eq!(v[0], json!({"result":["f","v"]}));
    assert_eq!(v[1], json!({"result":["m","1.5"]}));
    assert_eq!(v[2], json!({"result":"1.5"}));
    assert_eq!(v[3], json!({"result":["a"]}));
    s.shutdown.cancel();
}

#[tokio::test]
async fn shares_state_with_other_listeners() {
    let s = start().await;
    // Written over HTTP, visible to a session built like the RESP listener's.
    post(s.addr, "/", r#"["SET","shared","1"]"#).await;
    let mut other = Session::new_with_pubsub(s.store.clone(), s.registry.clone(), s.pubsub.clone());
    let reply = enqueue_command(&mut other, &[Bytes::from("GET"), Bytes::from("shared")]).await;
    assert!(matches!(&reply[0], RespValue::BulkString(Some(b)) if b.as_ref() == b"1"));
    // PUBLISH over HTTP reaches registry subscribers.
    let mut rx = s.pubsub.subscribe_channel(Bytes::from("ch"));
    assert_eq!(
        post(s.addr, "/", r#"["PUBLISH","ch","hi"]"#).await.1,
        json!({"result":1})
    );
    assert_eq!(rx.recv().await.unwrap().payload.as_ref(), b"hi");
    s.shutdown.cancel();
}

async fn read_until(sock: &mut TcpStream, buf: &mut Vec<u8>, needle: &str) {
    while !String::from_utf8_lossy(buf).contains(needle) {
        let mut chunk = [0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut chunk))
            .await
            .expect("timed out waiting for SSE data")
            .unwrap();
        assert!(
            n > 0,
            "stream closed early: {}",
            String::from_utf8_lossy(buf)
        );
        buf.extend_from_slice(&chunk[..n]);
    }
}

#[tokio::test]
async fn subscribe_streams_events_and_releases_on_disconnect() {
    let s = start().await;
    let mut sock = TcpStream::connect(s.addr).await.unwrap();
    sock.write_all(
        b"POST /subscribe/news HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    read_until(&mut sock, &mut buf, "data: subscribe,news,1\n\n").await;
    assert!(String::from_utf8_lossy(&buf)
        .to_lowercase()
        .contains("text/event-stream"));
    post(s.addr, "/", r#"["PUBLISH","news","a,b\nc"]"#).await;
    read_until(&mut sock, &mut buf, "data: message,news,a,b\ndata: c\n\n").await;

    let mut psock = TcpStream::connect(s.addr).await.unwrap();
    psock
        .write_all(b"POST /psubscribe/ne* HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut pbuf = Vec::new();
    read_until(&mut psock, &mut pbuf, "data: psubscribe,ne*,1\n\n").await;
    post(s.addr, "/", r#"["PUBLISH","news","x"]"#).await;
    read_until(&mut psock, &mut pbuf, "data: pmessage,ne*,news,x\n\n").await;

    // Disconnecting releases both subscriptions.
    drop(sock);
    drop(psock);
    for _ in 0..50 {
        if post(s.addr, "/", r#"["PUBLISH","news","y"]"#).await.1 == json!({"result":0}) {
            s.shutdown.cancel();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("subscriptions were not released after disconnect");
}

#[tokio::test]
async fn subscribe_requires_exactly_one_channel() {
    let s = start().await;
    assert_eq!(post(s.addr, "/subscribe", "").await.0, 400);
    assert_eq!(post(s.addr, "/subscribe/a/b", "").await.0, 400);
    s.shutdown.cancel();
}
