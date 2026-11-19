//! Redis-over-HTTP listener implementing the Upstash REST protocol (RFC 0003).
//!
//! One HTTP request is one short-lived Redis session on database 0: it is
//! built the way the RESP listener builds a connection's session, runs its
//! command(s) through [`enqueue_command`], and is dropped. Replies are
//! downgraded to their RESP2 shapes before being mapped to JSON, so RESP3
//! types never reach a client. The listener is unauthenticated and the
//! `Authorization` header and `_token` parameter are ignored.
//!
//! Left out on purpose because nothing settles Upstash's behaviour (RFC 0003,
//! Appendix A): `HEAD` requests (answered `405`), boolean/`null` JSON
//! arguments, valueless or repeated query parameters, and multi-channel
//! `/subscribe` paths are all rejected rather than guessed at.

pub mod request;
pub mod response;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderValue, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::commands::enqueue_command;
use crate::common::session::DEFAULT_CONFLICT_RETRY_COUNT;
use crate::common::{RedisStore, Session, WatchRegistry};
use crate::pubsub::{PubSubMsg, PubSubRegistry};
use crate::resp::RespValue;

/// Default `--http-max-body`: 10 MiB.
pub const DEFAULT_MAX_BODY: usize = 10 * 1024 * 1024;

const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const BODY_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

type Body = BoxBody<Bytes, Infallible>;

/// The HTTP front end. Build it from the same store, watch registry and
/// pub/sub registry as the RESP listener (see [`crate::RedisListener::http_listener`])
/// so both operate on shared state.
pub struct HttpListener {
    addr: SocketAddr,
    ctx: Arc<Ctx>,
}

struct Ctx {
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    pubsub: Arc<PubSubRegistry>,
    conflict_retry_count: usize,
    max_body: usize,
    shutdown: CancellationToken,
}

impl Ctx {
    fn session(&self) -> Session {
        let mut session = Session::new_with_pubsub(
            self.store.clone(),
            self.registry.clone(),
            self.pubsub.clone(),
        );
        session.set_conflict_retry_count(self.conflict_retry_count);
        session
    }
}

impl HttpListener {
    pub fn new(
        addr: SocketAddr,
        store: Arc<dyn RedisStore>,
        registry: Arc<WatchRegistry>,
        pubsub: Arc<PubSubRegistry>,
    ) -> Self {
        Self {
            addr,
            ctx: Arc::new(Ctx {
                store,
                registry,
                pubsub,
                conflict_retry_count: DEFAULT_CONFLICT_RETRY_COUNT,
                max_body: DEFAULT_MAX_BODY,
                shutdown: CancellationToken::new(),
            }),
        }
    }

    fn ctx_mut(&mut self) -> &mut Ctx {
        Arc::get_mut(&mut self.ctx).expect("listener configured before serving")
    }

    pub fn with_conflict_retry_count(mut self, count: usize) -> Self {
        self.ctx_mut().conflict_retry_count = count;
        self
    }

    /// Maximum request body size in bytes.
    pub fn with_max_body(mut self, bytes: usize) -> Self {
        self.ctx_mut().max_body = bytes;
        self
    }

    /// A token that stops the listener: it stops accepting, cancels SSE
    /// streams and lets in-flight requests finish (bounded).
    pub fn shutdown_token(&self) -> CancellationToken {
        self.ctx.shutdown.clone()
    }

    /// Binds the configured address and serves until the shutdown token fires.
    pub async fn serve(self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Redis-over-HTTP listener on {}", listener.local_addr()?);
        self.serve_on(listener).await
    }

    /// Serves on an already-bound listener until the shutdown token fires.
    pub async fn serve_on(self, listener: TcpListener) -> std::io::Result<()> {
        let ctx = self.ctx;
        let mut conns = JoinSet::new();
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((sock, _)) => {
                        conns.spawn(serve_connection(sock, ctx.clone()));
                    }
                    Err(e) => tracing::warn!(error = %e, "http accept failed"),
                },
                Some(_) = conns.join_next(), if !conns.is_empty() => {}
            }
        }
        drop(listener);
        let drain = async { while conns.join_next().await.is_some() {} };
        if tokio::time::timeout(DRAIN_TIMEOUT, drain).await.is_err() {
            tracing::warn!("http connections did not drain in time, dropping them");
        }
        tracing::info!("Redis-over-HTTP listener stopped");
        Ok(())
    }
}

async fn serve_connection(sock: tokio::net::TcpStream, ctx: Arc<Ctx>) {
    let shutdown = ctx.shutdown.clone();
    let svc = service_fn(move |req| {
        let ctx = ctx.clone();
        async move { Ok::<_, Infallible>(handle(req, ctx).await) }
    });
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .serve_connection(TokioIo::new(sock), svc);
    tokio::pin!(conn);
    let result = tokio::select! {
        r = conn.as_mut() => r,
        _ = shutdown.cancelled() => {
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    };
    if let Err(e) = result {
        tracing::debug!(error = %e, "http connection error");
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).boxed()
}

fn json_response(status: StatusCode, value: &Value) -> Response<Body> {
    let mut resp = Response::new(full(serde_json::to_vec(value).unwrap_or_default()));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn json_error(status: StatusCode, message: &str) -> Response<Body> {
    json_response(status, &response::error_object(message))
}

#[derive(Clone, Copy)]
enum Endpoint {
    Command,
    Pipeline,
    MultiExec,
    Subscribe,
    Psubscribe,
    Other,
}

impl Endpoint {
    fn label(self) -> &'static str {
        match self {
            Endpoint::Command => "command",
            Endpoint::Pipeline => "pipeline",
            Endpoint::MultiExec => "multi_exec",
            Endpoint::Subscribe => "subscribe",
            Endpoint::Psubscribe => "psubscribe",
            Endpoint::Other => "other",
        }
    }
}

async fn handle(req: Request<Incoming>, ctx: Arc<Ctx>) -> Response<Body> {
    let (endpoint, resp) = route(req, &ctx).await;
    metrics::counter!(
        "invar_http_requests_total",
        "endpoint" => endpoint.label(),
        "status" => resp.status().as_u16().to_string()
    )
    .increment(1);
    resp
}

async fn route(req: Request<Incoming>, ctx: &Arc<Ctx>) -> (Endpoint, Response<Body>) {
    let method = req.method().clone();
    if !matches!(method, Method::GET | Method::POST | Method::PUT) {
        // `HEAD` is documented as allowed, but whether it executes the
        // command is unknown (RFC 0003, Appendix A); refuse rather than guess.
        return (
            Endpoint::Other,
            json_error(StatusCode::METHOD_NOT_ALLOWED, "ERR method not allowed"),
        );
    }
    let segments = request::path_segments(req.uri().path());
    let query = req.uri().query().map(str::to_owned);
    let first = segments.first().map(|s| s.to_ascii_lowercase());
    let endpoint = match first.as_deref() {
        None => Endpoint::Command,
        Some(b"pipeline") => Endpoint::Pipeline,
        Some(b"multi-exec") => Endpoint::MultiExec,
        Some(b"subscribe") => Endpoint::Subscribe,
        Some(b"psubscribe") => Endpoint::Psubscribe,
        Some(_) => Endpoint::Command,
    };
    let headers = req.headers().clone();
    let base64 = headers
        .get("upstash-encoding")
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"base64"));
    let format = headers
        .get("upstash-response-format")
        .map(|v| v.as_bytes().to_ascii_lowercase());

    if matches!(first.as_deref(), Some(b"monitor")) {
        return (
            Endpoint::Other,
            json_error(StatusCode::BAD_REQUEST, "ERR unsupported command 'monitor'"),
        );
    }
    if matches!(
        endpoint,
        Endpoint::Pipeline | Endpoint::MultiExec | Endpoint::Subscribe | Endpoint::Psubscribe
    ) && method != Method::POST
    {
        return (
            endpoint,
            json_error(StatusCode::METHOD_NOT_ALLOWED, "ERR method not allowed"),
        );
    }

    // SSE needs no body.
    if matches!(endpoint, Endpoint::Subscribe | Endpoint::Psubscribe) {
        let pattern = matches!(endpoint, Endpoint::Psubscribe);
        if segments.len() != 2 {
            return (
                endpoint,
                json_error(
                    StatusCode::BAD_REQUEST,
                    "ERR exactly one channel or pattern is supported",
                ),
            );
        }
        return (endpoint, sse(ctx, segments[1].clone(), pattern));
    }

    // Response format: only `json` (default) and `resp2`; `resp2` can't be
    // combined with base64 and does not apply to /multi-exec.
    let raw_resp2 = match format.as_deref() {
        None | Some(b"json") => false,
        Some(b"resp2") if matches!(endpoint, Endpoint::MultiExec) => false,
        Some(b"resp2") if base64 => {
            return (
                endpoint,
                json_error(
                    StatusCode::BAD_REQUEST,
                    "ERR resp2 response format cannot be combined with base64 encoding",
                ),
            )
        }
        Some(b"resp2") => true,
        Some(_) => {
            return (
                endpoint,
                json_error(StatusCode::BAD_REQUEST, "ERR unsupported response format"),
            )
        }
    };

    let body = match read_body(req, ctx.max_body).await {
        Ok(b) => b,
        Err(resp) => return (endpoint, resp),
    };

    let resp = match endpoint {
        Endpoint::Command => {
            let args = if segments.is_empty() {
                if method != Method::POST {
                    Err("ERR missing command".to_string())
                } else {
                    request::parse_command(&body)
                }
            } else {
                let body: &[u8] = if method == Method::GET { b"" } else { &body };
                request::path_form_args(segments, query.as_deref(), body)
            };
            match args {
                Ok(args) => single(ctx, args, base64, raw_resp2).await,
                Err(e) => json_error(StatusCode::BAD_REQUEST, &e),
            }
        }
        Endpoint::Pipeline => match request::parse_batch(&body) {
            Ok(rows) => pipeline(ctx, rows, base64, raw_resp2).await,
            Err(e) => json_error(StatusCode::BAD_REQUEST, &e),
        },
        Endpoint::MultiExec => match request::parse_batch(&body) {
            Ok(rows) => multi_exec(ctx, rows, base64).await,
            Err(e) => json_error(StatusCode::BAD_REQUEST, &e),
        },
        _ => unreachable!("handled above"),
    };
    (endpoint, resp)
}

/// Reads the request body, enforcing the size limit before buffering it.
async fn read_body(req: Request<Incoming>, max: usize) -> Result<Bytes, Response<Body>> {
    let too_large = || {
        json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "ERR max request size exceeded",
        )
    };
    if let Some(len) = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if len > max {
            return Err(too_large());
        }
    }
    let collect = Limited::new(req.into_body(), max).collect();
    match tokio::time::timeout(BODY_TIMEOUT, collect).await {
        Ok(Ok(collected)) => Ok(collected.to_bytes()),
        Ok(Err(e))
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some() =>
        {
            Err(too_large())
        }
        Ok(Err(_)) => Err(json_error(
            StatusCode::BAD_REQUEST,
            "ERR could not read request body",
        )),
        Err(_) => Err(json_error(
            StatusCode::REQUEST_TIMEOUT,
            "ERR request body timed out",
        )),
    }
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Runs one command and returns its reply (never queued: no `MULTI` is open).
async fn run(session: &mut Session, args: &[Bytes]) -> RespValue {
    if let Err(e) = request::check_supported(args) {
        return RespValue::Error(Bytes::from(e));
    }
    enqueue_command(session, args)
        .await
        .into_iter()
        .next()
        .unwrap_or(RespValue::Null)
}

async fn single(ctx: &Ctx, args: Vec<Bytes>, base64: bool, raw_resp2: bool) -> Response<Body> {
    let mut session = ctx.session();
    let reply = run(&mut session, &args).await;
    let is_error = matches!(reply, RespValue::Error(_));
    let status = if is_error {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::OK
    };
    if raw_resp2 {
        return raw_response(status, response::resp2_bytes(reply));
    }
    let (value, _) = response::item(reply, base64);
    json_response(status, &value)
}

fn raw_response(status: StatusCode, bytes: Bytes) -> Response<Body> {
    let mut resp = Response::new(full(bytes));
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp
}

async fn pipeline(
    ctx: &Ctx,
    rows: Vec<Vec<Bytes>>,
    base64: bool,
    raw_resp2: bool,
) -> Response<Body> {
    let mut session = ctx.session();
    let mut items = Vec::with_capacity(rows.len());
    let mut raw = Vec::new();
    for args in rows {
        let reply = run(&mut session, &args).await;
        if raw_resp2 {
            raw.extend_from_slice(&response::resp2_bytes(reply));
        } else {
            items.push(response::item(reply, base64).0);
        }
    }
    if raw_resp2 {
        return raw_response(StatusCode::OK, Bytes::from(raw));
    }
    json_response(StatusCode::OK, &Value::Array(items))
}

/// `MULTI`, the commands, `EXEC`. A queue-time error discards the whole
/// transaction (nothing is executed) and is answered with a single error.
async fn multi_exec(ctx: &Ctx, rows: Vec<Vec<Bytes>>, base64: bool) -> Response<Body> {
    let mut session = ctx.session();
    let begin = enqueue_command(&mut session, &[Bytes::from_static(b"MULTI")]).await;
    if let Some(RespValue::Error(e)) = begin.first() {
        return json_error(StatusCode::BAD_REQUEST, &String::from_utf8_lossy(e));
    }
    for args in rows {
        let reply = run(&mut session, &args).await;
        if let RespValue::Error(e) = reply {
            // Dropping the session discards the queued transaction.
            return json_error(StatusCode::BAD_REQUEST, &String::from_utf8_lossy(&e));
        }
    }
    let exec = enqueue_command(&mut session, &[Bytes::from_static(b"EXEC")]).await;
    match exec.into_iter().next() {
        Some(RespValue::Error(e)) => {
            json_error(StatusCode::BAD_REQUEST, &String::from_utf8_lossy(&e))
        }
        Some(RespValue::Array(Some(replies))) => {
            let items = replies
                .into_iter()
                .map(|r| response::item(r, base64).0)
                .collect();
            json_response(StatusCode::OK, &Value::Array(items))
        }
        _ => json_error(StatusCode::BAD_REQUEST, "ERR transaction was not executed"),
    }
}

// ---------------------------------------------------------------------------
// Server-sent events
// ---------------------------------------------------------------------------

/// Frames one SSE event. Payload lines are split on `\r\n`, `\r` and `\n`,
/// each becoming its own `data:` line, as the SSE format requires.
fn sse_event(fields: &[&[u8]], payload: &[u8]) -> Bytes {
    let mut line = Vec::new();
    for field in fields {
        line.extend_from_slice(field);
        line.push(b',');
    }
    line.extend_from_slice(payload);
    let text = String::from_utf8_lossy(&line)
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let mut out = String::new();
    for l in text.split('\n') {
        out.push_str("data: ");
        out.push_str(l);
        out.push('\n');
    }
    out.push('\n');
    Bytes::from(out)
}

fn sse(ctx: &Arc<Ctx>, name: Bytes, pattern: bool) -> Response<Body> {
    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);
    let rx_msgs = if pattern {
        ctx.pubsub.subscribe_pattern(name.clone())
    } else {
        ctx.pubsub.subscribe_channel(name.clone())
    };
    let ctx = ctx.clone();
    metrics::gauge!("invar_http_sse_streams").increment(1.0);
    tokio::spawn(async move {
        sse_task(&ctx, name.clone(), pattern, rx_msgs, tx).await;
        if pattern {
            ctx.pubsub.cleanup_pattern(&name);
        } else {
            ctx.pubsub.cleanup_channel(&name);
        }
        metrics::gauge!("invar_http_sse_streams").decrement(1.0);
    });
    let body = StreamBody::new(ReceiverStream::new(rx)).boxed();
    let mut resp = Response::new(body);
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    resp.headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    resp
}

async fn sse_task(
    ctx: &Ctx,
    name: Bytes,
    pattern: bool,
    mut rx_msgs: broadcast::Receiver<PubSubMsg>,
    tx: mpsc::Sender<Result<Frame<Bytes>, Infallible>>,
) {
    let kind: &[u8] = if pattern { b"psubscribe" } else { b"subscribe" };
    let ack = sse_event(&[kind, &name], b"1");
    if tx.send(Ok(Frame::data(ack))).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            // The client went away (or hyper dropped the body).
            _ = tx.closed() => return,
            _ = ctx.shutdown.cancelled() => return,
            msg = rx_msgs.recv() => match msg {
                Ok(m) => {
                    let event = if pattern {
                        sse_event(&[b"pmessage", &name, &m.channel], &m.payload)
                    } else {
                        sse_event(&[b"message", &m.channel], &m.payload)
                    };
                    if tx.send(Ok(Frame::data(event))).await.is_err() {
                        return;
                    }
                }
                // A slow client misses messages, as with a slow RESP subscriber.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}

#[cfg(test)]
mod tests;
