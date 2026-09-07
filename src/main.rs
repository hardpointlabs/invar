use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;

async fn start_metrics_server(addr: SocketAddr) {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .expect("failed to install Prometheus metrics exporter");
    tracing::info!(%addr, "Prometheus metrics server started");
}

use clap::{Parser, ValueEnum};
use kv::{
    fjall::FjallDb,
    slate::{SlateDb, SlateDbOpts},
};
use redis::{RedisListener, RedisStore};
use tokio::time::{Duration, timeout};

#[derive(ValueEnum, Clone, Debug)]
enum Backend {
    Slate,
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

    /// Optional listen address for the Prometheus metrics exporter
    #[arg(long, env = "INVAR_METRICS_ADDR")]
    metrics_addr: Option<String>,

    /// Bucket prefix to use with the slate backend
    #[arg(long, env = "INVAR_BUCKET_PREFIX", default_value = "/invar")]
    prefix: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    let store: Arc<dyn RedisStore> = match cli.backend {
        Backend::Slate => {
            let bucket = cli
                .bucket
                .expect("bucket is required for the SlateDB backend");
            Arc::new(
                SlateDb::open(SlateDbOpts {
                    path: cli.prefix.clone(),
                    bucket_name: bucket,
                    settings: None,
                })
                .await.inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                .expect("failed to open SlateDB store"),
            )
        }
        Backend::Fjall => {
            let path = cli.path.expect("path is required for the Fjall backend");
            Arc::new(FjallDb::open(path)
                .inspect_err(|e| tracing::error!(error = %e, "operation failed"))
                .expect("failed to open Fjall store"))
        }
    };

    if let Some(addr) = &cli.metrics_addr {
        match addr.to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(resolved) => { start_metrics_server(resolved).await; }
                None => tracing::warn!("metrics disabled: '{addr}' resolved to no addresses"),
            },
            Err(e) => tracing::warn!("metrics disabled: couldn't resolve '{addr}': {e}"),
        }
    }

    let addr: SocketAddr = "0.0.0.0:6379".parse().expect("valid listen address");
    let listener = RedisListener::new(addr, store.clone());

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

    match timeout(Duration::from_secs(10), store.close()).await {
        Ok(Ok(())) => tracing::info!("store closed cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "error closing store"),
        Err(_) => tracing::warn!("store close timed out after grace period, exiting anyway"),
    }

    println!("done")
}
