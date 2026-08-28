use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

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
    about = "Invar - a lightweight, durable document store"
)]
struct Cli {
    /// Serve the Redis wire protocol (RESP) on :6379.
    #[arg(long)]
    redis: bool,

    #[arg(long, env = "INVAR_BACKEND", value_enum)]
    backend: Backend,

    // only required when --backend=slate
    #[arg(long, env = "INVAR_S3_BUCKET", required_if_eq("backend", "slate"))]
    bucket: Option<String>,

    #[arg(long, env = "INVAR_DATA_PATH", default_value = "/tmp/invar")]
    path: Option<PathBuf>,

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

    if cli.redis {
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
    }

    match timeout(Duration::from_secs(10), store.close()).await {
        Ok(Ok(())) => tracing::info!("store closed cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "error closing store"),
        Err(_) => tracing::warn!("store close timed out after grace period, exiting anyway"),
    }

    println!("done")
}
