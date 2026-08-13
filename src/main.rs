use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use kv::{
    fjall::FjallDb,
    slate::{SlateDb, SlateDbOpts},
};
use redis::{RedisListener, RedisStore};

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

    // only required when --backend=fjall
    #[arg(long, env = "INVAR_DATA_PATH", required_if_eq("backend", "fjall"))]
    path: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let store: Arc<dyn RedisStore> = match cli.backend {
        Backend::Slate => {
            let bucket = cli
                .bucket
                .expect("bucket is required for the slate backend");
            Arc::new(
                SlateDb::open(SlateDbOpts {
                    path: "/tmp/invar-slatedb".to_string(),
                    object_store_url: format!("s3://{bucket}/"),
                    settings: None,
                })
                .await
                .expect("failed to open SlateDB store"),
            )
        }
        Backend::Fjall => {
            let path = cli.path.expect("path is required for the fjall backend");
            Arc::new(FjallDb::open(path).expect("failed to open Fjall store"))
        }
    };

    if cli.redis {
        let addr: SocketAddr = "0.0.0.0:6379".parse().expect("valid listen address");
        RedisListener::new(addr, store.clone())
            .serve()
            .await
            .expect("redis listener failed");
    }

    store.close().await.expect("failed to close store");
    println!("done!")
}
