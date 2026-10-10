use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use clap::{Parser, ValueEnum};
use kv::{
    fjall::FjallDb,
    slate::{SlateDb, SlateDbOpts},
};
use redis::{ListenAddr, RedisListener, RedisStore};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, timeout};

/// Maps a request line (`<METHOD> <PATH> HTTP/1.x`) to a status line and body.
fn route(request_line: &str, ready: &AtomicBool, metrics: &PrometheusHandle) -> (&'static str, String) {
    let mut parts = request_line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    if method != "GET" && method != "HEAD" {
        return ("405 Method Not Allowed", String::new());
    }
    let path = target.split('?').next().unwrap_or("");
    match path {
        "/livez" => ("200 OK", "ok\n".into()),
        "/readyz" if ready.load(Ordering::Acquire) => ("200 OK", "ok\n".into()),
        "/readyz" => ("503 Service Unavailable", "not ready\n".into()),
        "/metrics" => ("200 OK", metrics.render()),
        _ => ("404 Not Found", String::new()),
    }
}

async fn handle_conn(mut sock: TcpStream, ready: Arc<AtomicBool>, metrics: PrometheusHandle) -> std::io::Result<()> {
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
    let (status, body) = route(request_line, &ready, &metrics);
    let head_only = request_line.starts_with("HEAD ");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        if head_only { "" } else { &body },
    );
    sock.write_all(response.as_bytes()).await?;
    sock.shutdown().await
}

/// Handle to the running probe/metrics server, used to stop it during shutdown.
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

/// Serves `/livez`, `/readyz` and `/metrics`. `/readyz` reports 200 only while `ready` is set.
async fn start_metrics_server(addr: SocketAddr, ready: Arc<AtomicBool>) -> MetricsServer {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder");

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind metrics listener");
    tracing::info!(%addr, "metrics/probe server started");

    let (shutdown, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut conns = JoinSet::new();
        let mut upkeep = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = upkeep.tick() => handle.run_upkeep(),
                accepted = listener.accept() => match accepted {
                    Ok((sock, _)) => {
                        let (ready, handle) = (ready.clone(), handle.clone());
                        conns.spawn(async move {
                            let res = timeout(Duration::from_secs(5), handle_conn(sock, ready, handle)).await;
                            if let Ok(Err(e)) = res {
                                tracing::debug!(error = %e, "metrics connection error");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "metrics accept failed"),
                },
                Some(_) = conns.join_next(), if !conns.is_empty() => {}
            }
        }
        drop(listener);
        let drain = async { while conns.join_next().await.is_some() {} };
        if timeout(Duration::from_secs(2), drain).await.is_err() {
            tracing::warn!("metrics connections did not drain in time, dropping them");
        }
        tracing::info!("metrics/probe server stopped");
    });

    MetricsServer { shutdown, task }
}

#[derive(ValueEnum, Clone, Debug)]
enum Backend {
    Slate,
    Memory,
    Fjall,
}

#[derive(Parser)]
#[command(
    name = "invar",
    version,
    about = "Invar: the diskless document store"
)]
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

    /// Bucket prefix to use with the slate backend
    #[arg(long, env = "INVAR_BUCKET_PREFIX", default_value = "/invar")]
    prefix: String,

    /// Path to Foyer on-disk block cache. Only applicable when using the slate backend
    #[arg(long, env = "INVAR_CACHE_PATH")]
    cache_path: Option<PathBuf>,

    /// Maximum amount of memory (in MB) that the slate block cache's in-memory tier can use
    #[arg(long, env = "INVAR_CACHE_MEM_LIMIT", default_value = "16")]
    cache_mem_limit: Option<usize>,

    /// Grace period for closing the store on shutdown (e.g. "10s", "500ms", "1m")
    /// Invar will shut down earlier if graceful shutdown completes sooner
    #[arg(
        long,
        env = "INVAR_SHUTDOWN_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    shutdown_timeout: Duration,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    let ready = Arc::new(AtomicBool::new(false));

    let mut metrics_server = None;
    if let Some(addr) = &cli.metrics_addr {
        match addr.to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(resolved) => {
                    metrics_server = Some(start_metrics_server(resolved, ready.clone()).await);
                }
                None => tracing::warn!("metrics disabled: '{addr}' resolved to no addresses"),
            },
            Err(e) => tracing::warn!("metrics disabled: couldn't resolve '{addr}': {e}"),
        }
    }

    let store: Arc<dyn RedisStore> = match cli.backend {
        Backend::Slate => {
            let bucket = cli
                .bucket
                .expect("bucket is required for the SlateDB backend");
            Arc::new(
                SlateDb::open(SlateDbOpts {
                    path: cli.prefix.clone(),
                    bucket_name: bucket,
                    cache_path: cli.cache_path,
                    cache_mem_limit: cli.cache_mem_limit.unwrap_or(16),
                })
                .await.inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                .expect("failed to open SlateDB store"),
            )
        }
        Backend::Memory => {
            Arc::new(SlateDb::in_memory()
                .await.inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                .expect("failed to open in-memory store"))
        }
        Backend::Fjall => {
            let path = cli.path.expect("path is required for the Fjall backend");
            Arc::new(FjallDb::open(path)
                .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                .expect("failed to open Fjall store"))
        }
    };

    ready.store(true, Ordering::Release);

    let listener = RedisListener::new(cli.redis_addr.clone(), store.clone());

    tokio::select! {
            result = listener.serve() => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "redis listener failed");
                }
            },
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal received, closing store");
            }
        }

    // Fail readiness first so orchestrators stop routing traffic before the store goes away.
    ready.store(false, Ordering::Release);

    match timeout(cli.shutdown_timeout, store.close()).await {
        Ok(Ok(())) => tracing::info!("store closed cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "error closing store"),
        Err(_) => tracing::warn!(timeout = ?cli.shutdown_timeout, "store close timed out, exiting anyway"),
    }

    if let ListenAddr::Unix(path) = &cli.redis_addr {
        let _ = std::fs::remove_file(path);
    }

    // The metrics server outlives the store so probes stay answerable while it closes.
    if let Some(server) = metrics_server {
        server.stop().await;
    }

    println!("done")
}
