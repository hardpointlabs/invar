use clap::{Parser, ValueEnum};
use kv::{
    fjall::FjallDb,
    kv::AckLevel,
    slate::{SlateDb, SlateDbOpts, SlateFollower},
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use redis::{ListenAddr, RedisListener, RedisStore, DEFAULT_CONFLICT_RETRY_COUNT};
use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{timeout, Duration};

/// What this process is doing (RFC 0004), reported by `GET /role`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Role {
    /// Opening the store, as writer or follower, or being promoted.
    Starting = 0,
    /// Holding the store open read-only, serving no clients, until promoted.
    Follower = 1,
    /// Serving clients.
    Writer = 2,
    /// Shutting down.
    Stopping = 3,
    /// Another process opened the store as writer; this one stopped serving.
    Fenced = 4,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Starting => "starting",
            Role::Follower => "follower",
            Role::Writer => "writer",
            Role::Stopping => "stopping",
            Role::Fenced => "fenced",
        }
    }

    fn from_u8(v: u8) -> Role {
        match v {
            1 => Role::Follower,
            2 => Role::Writer,
            3 => Role::Stopping,
            4 => Role::Fenced,
            _ => Role::Starting,
        }
    }
}

/// The process's current [`Role`], shared with the probe and admin servers.
struct NodeState(AtomicU8);

impl NodeState {
    fn new(role: Role) -> Self {
        Self(AtomicU8::new(role as u8))
    }

    fn get(&self) -> Role {
        Role::from_u8(self.0.load(Ordering::Acquire))
    }

    fn set(&self, role: Role) {
        self.0.store(role as u8, Ordering::Release);
    }

    /// Moves from `from` to `to` only if the current role is `from`.
    fn transition(&self, from: Role, to: Role) -> bool {
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Ready means serving clients in the role this process was started for.
    /// Every process is writer-eligible today, so that means being the writer.
    fn ready(&self) -> bool {
        self.get() == Role::Writer
    }
}

/// Routes a request line (`<METHOD> <PATH> HTTP/1.x`) to a status line and body.
type Router = Arc<dyn Fn(&str) -> (&'static str, String) + Send + Sync>;

/// Splits a request line into its method and path (without query string).
fn method_and_path(request_line: &str) -> (&str, &str) {
    let mut parts = request_line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    (method, target.split('?').next().unwrap_or(""))
}

/// The probe/metrics listener's routes. `GET`/`HEAD` only.
fn route(
    request_line: &str,
    state: &NodeState,
    metrics: &PrometheusHandle,
) -> (&'static str, String) {
    let (method, path) = method_and_path(request_line);
    if method != "GET" && method != "HEAD" {
        return ("405 Method Not Allowed", String::new());
    }
    match path {
        "/livez" => ("200 OK", "ok\n".into()),
        "/readyz" if state.ready() => ("200 OK", "ok\n".into()),
        "/readyz" => ("503 Service Unavailable", "not ready\n".into()),
        "/role" => ("200 OK", format!("{}\n", state.get().as_str())),
        "/metrics" => ("200 OK", metrics.render()),
        _ => ("404 Not Found", String::new()),
    }
}

/// The admin listener's routes: `POST /promote` only (RFC 0004).
fn admin_route(
    request_line: &str,
    state: &NodeState,
    promote: &mpsc::Sender<()>,
) -> (&'static str, String) {
    let (method, path) = method_and_path(request_line);
    if path != "/promote" {
        return ("404 Not Found", String::new());
    }
    if method != "POST" {
        return ("405 Method Not Allowed", String::new());
    }
    // Claim the promotion here, so concurrent requests can't start two.
    if !state.transition(Role::Follower, Role::Starting) {
        let role = state.get().as_str();
        return ("409 Conflict", format!("not a follower (role: {role})\n"));
    }
    match promote.try_send(()) {
        Ok(()) => ("202 Accepted", "promoting\n".into()),
        Err(_) => {
            state.set(Role::Follower);
            ("503 Service Unavailable", "promotion unavailable\n".into())
        }
    }
}

async fn handle_conn(mut sock: TcpStream, router: Router) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    let mut len = 0;
    // Only the request line is needed; read until it is complete.
    while !buf[..len].contains(&b'\n') && len < buf.len() {
        let n = sock.read(&mut buf[len..]).await?;
        if n == 0 {
            break;
        }
        len += n;
    }
    let head = String::from_utf8_lossy(&buf[..len]);
    let request_line = head.lines().next().unwrap_or("");
    let (status, body) = router(request_line);
    let head_only = request_line.starts_with("HEAD ");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        if head_only { "" } else { &body },
    );
    sock.write_all(response.as_bytes()).await?;
    sock.shutdown().await
}

/// Handle to a running probe/metrics or admin server, used to stop it during shutdown.
struct MetricsServer {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl MetricsServer {
    /// Stops accepting connections, lets in-flight requests finish (bounded), and waits for the server to exit.
    async fn stop(self) {
        let _ = self.shutdown.send(());
        if let Err(e) = self.task.await {
            tracing::warn!(error = %e, "metrics server task failed");
        }
    }
}

/// Serves `/livez`, `/readyz`, `/role` and `/metrics`. `/readyz` reports 200
/// only while this process is serving clients in the role it was started for.
async fn start_metrics_server(addr: SocketAddr, state: Arc<NodeState>) -> MetricsServer {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder");

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind metrics listener");
    tracing::info!(%addr, "metrics/probe server started");

    let metrics = handle.clone();
    let router: Router = Arc::new(move |line: &str| route(line, &state, &metrics));
    serve_http("metrics/probe", listener, router, Some(handle))
}

/// Serves `POST /promote` (RFC 0004). Like the RESP and HTTP listeners it is
/// unauthenticated, so it should be bound to loopback.
async fn start_admin_server(
    addr: SocketAddr,
    state: Arc<NodeState>,
    promote: mpsc::Sender<()>,
) -> MetricsServer {
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "admin listener is unauthenticated and not bound to loopback; \
             anything that can reach it can make this process open the store as writer"
        );
    }
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind admin listener");
    tracing::info!(%addr, "admin server started");
    let router: Router = Arc::new(move |line: &str| admin_route(line, &state, &promote));
    serve_http("admin", listener, router, None)
}

/// Accepts connections on `listener` and answers each request with `router`,
/// until stopped. Runs Prometheus upkeep when given a handle.
fn serve_http(
    name: &'static str,
    listener: TcpListener,
    router: Router,
    upkeep_handle: Option<PrometheusHandle>,
) -> MetricsServer {
    let (shutdown, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut conns = JoinSet::new();
        let mut upkeep = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = upkeep.tick() => {
                    if let Some(handle) = &upkeep_handle {
                        handle.run_upkeep();
                    }
                }
                accepted = listener.accept() => match accepted {
                    Ok((sock, _)) => {
                        let router = router.clone();
                        conns.spawn(async move {
                            let res = timeout(Duration::from_secs(5), handle_conn(sock, router)).await;
                            if let Ok(Err(e)) = res {
                                tracing::debug!(error = %e, server = name, "connection error");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, server = name, "accept failed"),
                },
                Some(_) = conns.join_next(), if !conns.is_empty() => {}
            }
        }
        drop(listener);
        let drain = async { while conns.join_next().await.is_some() {} };
        if timeout(Duration::from_secs(2), drain).await.is_err() {
            tracing::warn!(
                server = name,
                "connections did not drain in time, dropping them"
            );
        }
        tracing::info!(server = name, "server stopped");
    });

    MetricsServer { shutdown, task }
}

/// Installs `SIGINT` and `SIGTERM` handlers immediately, and returns a future
/// that resolves on the first of either. Process supervisors and container
/// runtimes stop processes with `SIGTERM`. Not an `async fn`: the handlers
/// must be in place before the future is first polled, or a signal arriving
/// while the store is still opening would kill the process.
fn shutdown_signal() -> impl Future<Output = ()> + Send {
    use tokio::signal::unix::{signal, SignalKind};
    let mut int = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
    let mut term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    async move {
        tokio::select! {
            _ = int.recv() => {}
            _ = term.recv() => {}
        }
    }
}

/// Resolves a `host:port` listen address, logging why when it can't.
fn resolve(name: &str, addr: &str) -> Option<SocketAddr> {
    match addr.to_socket_addrs().map(|mut a| a.next()) {
        Ok(Some(resolved)) => Some(resolved),
        Ok(None) => {
            tracing::warn!("{name} disabled: '{addr}' resolved to no addresses");
            None
        }
        Err(e) => {
            tracing::warn!("{name} disabled: couldn't resolve '{addr}': {e}");
            None
        }
    }
}

#[derive(ValueEnum, Clone, Debug)]
enum Backend {
    Slate,
    Memory,
    Fjall,
}

#[derive(Parser)]
#[command(name = "invar", version, about = "Invar: the diskless document store")]
struct Cli {
    /// Specify which storage backend to use
    #[arg(long, env = "INVAR_BACKEND", value_enum)]
    backend: Backend,

    /// Bucket name to use with the slate backend
    #[arg(long, env = "INVAR_S3_BUCKET", required_if_eq("backend", "slate"))]
    bucket: Option<String>,

    /// Where to persist data when using the fjall backend
    #[arg(long, env = "INVAR_DATA_PATH", default_value = "/tmp/invar")]
    path: Option<PathBuf>,

    /// Address the Redis listener binds to: `host:port` (IP or hostname), or a
    /// Unix socket path (any value containing `/`, e.g. `/tmp/invar.sock`)
    #[arg(long, env = "INVAR_REDIS_ADDR", default_value = "0.0.0.0:6379")]
    redis_addr: ListenAddr,

    /// Optional listen address for the Prometheus metrics exporter
    #[arg(long, env = "INVAR_METRICS_ADDR")]
    metrics_addr: Option<String>,

    /// Optional listen address for the Redis-over-HTTP listener (`host:port`).
    /// The listener is unauthenticated: bind it to loopback or put it behind a proxy
    #[arg(long, env = "INVAR_HTTP_ADDR")]
    http_addr: Option<String>,

    /// Maximum HTTP request body size in bytes
    #[arg(long, env = "INVAR_HTTP_MAX_BODY", default_value_t = redis::http::DEFAULT_MAX_BODY)]
    http_max_body: usize,

    /// Bucket prefix to use with the slate backend
    #[arg(long, env = "INVAR_BUCKET_PREFIX", default_value = "/invar")]
    prefix: String,

    /// Path to Foyer on-disk block cache. Only applicable when using the slate backend
    #[arg(long, env = "INVAR_CACHE_PATH")]
    cache_path: Option<PathBuf>,

    /// Maximum amount of memory (in MB) that the slate block cache's in-memory tier can use
    #[arg(long, env = "INVAR_CACHE_MEM_LIMIT", default_value = "16")]
    cache_mem_limit: Option<usize>,

    /// How many times a write is retried after it loses a conflict with a
    /// concurrent writer before the client gets an error (`0` disables retries)
    #[arg(long, env = "INVAR_CONFLICT_RETRY_COUNT", default_value_t = DEFAULT_CONFLICT_RETRY_COUNT)]
    conflict_retry_count: usize,

    /// Grace period for closing the store on shutdown (e.g. "10s", "500ms", "1m")
    /// Invar will shut down earlier if graceful shutdown completes sooner
    #[arg(
        long,
        env = "INVAR_SHUTDOWN_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    shutdown_timeout: Duration,

    /// Start as a read-only follower instead of a writer (RFC 0004): open the
    /// store read-only, serve no clients and report not ready until promoted
    /// with `POST /promote` on the admin listener. Slate backend only
    #[arg(long, env = "INVAR_FOLLOWER")]
    follower: bool,

    /// Optional listen address for the admin listener (`host:port`), which
    /// serves `POST /promote`. Unauthenticated: bind it to loopback
    #[arg(long, env = "INVAR_ADMIN_ADDR")]
    admin_addr: Option<String>,
}

type Fenced = Pin<Box<dyn Future<Output = ()> + Send>>;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt().with_env_filter(filter).init();

    if cli.follower && !matches!(cli.backend, Backend::Slate) {
        tracing::error!("--follower requires the slate backend");
        std::process::exit(2);
    }

    // Installed before anything slow happens, so an early SIGTERM is handled
    // gracefully rather than killing the process.
    let mut shutdown = Box::pin(shutdown_signal());

    let state = Arc::new(NodeState::new(Role::Starting));

    let mut metrics_server = None;
    if let Some(addr) = cli
        .metrics_addr
        .as_deref()
        .and_then(|a| resolve("metrics", a))
    {
        metrics_server = Some(start_metrics_server(addr, state.clone()).await);
    }

    let (promote_tx, mut promote_rx) = mpsc::channel(1);
    let mut admin_server = None;
    if let Some(addr) = cli
        .admin_addr
        .as_deref()
        .and_then(|a| resolve("admin listener", a))
    {
        admin_server = Some(start_admin_server(addr, state.clone(), promote_tx).await);
    } else {
        drop(promote_tx);
        if cli.follower {
            tracing::warn!("follower started without --admin-addr: it can't be promoted");
        }
    }

    let (store, fenced): (Arc<dyn RedisStore>, Fenced) = match cli.backend {
        Backend::Slate => {
            let opts = SlateDbOpts {
                path: cli.prefix.clone(),
                bucket_name: cli
                    .bucket
                    .clone()
                    .expect("bucket is required for the SlateDB backend"),
                cache_path: cli.cache_path.clone(),
                cache_mem_limit: cli.cache_mem_limit.unwrap_or(16),
            };
            let db = if cli.follower {
                let mut follower = SlateFollower::open(opts)
                    .await
                    .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                    .expect("failed to open SlateDB store as follower");
                state.set(Role::Follower);
                tracing::info!("store opened read-only; waiting to be promoted");
                loop {
                    tokio::select! {
                        _ = &mut shutdown => {
                            tracing::info!("shutdown signal received, closing follower");
                            state.set(Role::Stopping);
                            if let Err(e) = follower.close().await {
                                tracing::error!(error = %e, "error closing follower");
                            }
                            stop_servers(admin_server, metrics_server).await;
                            println!("done");
                            return;
                        }
                        Some(()) = promote_rx.recv() => {
                            tracing::info!("promoting: opening the store as writer");
                            match follower.promote().await {
                                Ok(db) => break db,
                                Err(e) => {
                                    tracing::error!(error = %e, "promotion failed; still a follower");
                                    state.set(Role::Follower);
                                }
                            }
                        }
                    }
                }
            } else {
                SlateDb::open(opts)
                    .await
                    .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                    .expect("failed to open SlateDB store")
            };
            let watched = db.clone();
            (
                Arc::new(db),
                Box::pin(async move { watched.fenced().await }),
            )
        }
        Backend::Memory => (
            Arc::new(
                SlateDb::in_memory()
                    .await
                    .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                    .expect("failed to open in-memory store"),
            ),
            Box::pin(std::future::pending()),
        ),
        Backend::Fjall => {
            let path = cli
                .path
                .clone()
                .expect("path is required for the Fjall backend");
            (
                Arc::new(
                    FjallDb::open(path)
                        .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                        .expect("failed to open Fjall store"),
                ),
                Box::pin(std::future::pending()),
            )
        }
    };

    state.set(Role::Writer);

    let listener = RedisListener::new(cli.redis_addr.clone(), store.clone())
        .with_conflict_retry_count(cli.conflict_retry_count);

    let mut http_server = None;
    if let Some(addr) = cli
        .http_addr
        .as_deref()
        .and_then(|a| resolve("http listener", a))
    {
        let http = listener
            .http_listener(addr)
            .with_max_body(cli.http_max_body);
        let token = http.shutdown_token();
        http_server = Some((token, tokio::spawn(http.serve())));
    }

    let was_fenced = tokio::select! {
        result = listener.serve() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "redis listener failed");
            }
            false
        },
        _ = &mut shutdown => {
            tracing::info!("shutdown signal received, closing store");
            false
        }
        _ = fenced => true,
    };

    if was_fenced {
        // Another process opened the store as writer. Stop serving, but stay
        // up: a supervisor would restart an exited process, which would open
        // the store as writer and fence its replacement.
        state.set(Role::Fenced);
        tracing::error!(
            "another process has opened the store as writer; this process has stopped serving \
             clients and is waiting to be stopped"
        );
    } else {
        // Acknowledge every commit from here on only once it is durable, and
        // fail readiness so traffic stops being routed here (RFC 0004).
        store.set_ack_level(AckLevel::Durable);
        state.set(Role::Stopping);
    }

    if let Some((token, task)) = http_server {
        token.cancel();
        if let Ok(Err(e)) = task.await {
            tracing::error!(error = %e, "http listener failed");
        }
    }

    if was_fenced {
        shutdown.await;
        tracing::info!("shutdown signal received, closing store");
    }

    match timeout(cli.shutdown_timeout, store.close()).await {
        Ok(Ok(())) => tracing::info!("store closed cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "error closing store"),
        Err(_) => tracing::error!(
            timeout = ?cli.shutdown_timeout,
            "store close timed out, exiting anyway; writes acknowledged before shutdown began \
             may not be durable"
        ),
    }

    if let ListenAddr::Unix(path) = &cli.redis_addr {
        let _ = std::fs::remove_file(path);
    }

    // The probe server outlives the store so probes stay answerable while it closes.
    stop_servers(admin_server, metrics_server).await;

    println!("done")
}

async fn stop_servers(admin: Option<MetricsServer>, metrics: Option<MetricsServer>) {
    if let Some(server) = admin {
        server.stop().await;
    }
    if let Some(server) = metrics {
        server.stop().await;
    }
}
