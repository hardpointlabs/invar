//! Object-store footprint reporting for the SlateDB backend.
//!
//! SlateDB has no public API for "bytes used on the object store", so we
//! measure it directly: LIST every object under the database's prefix and sum
//! the reported sizes. That is exactly what the bucket holds for this tenant
//! (live SSTs plus WAL, manifests and any garbage the compactor's GC hasn't
//! collected yet), and unlike an incrementally-maintained counter it can't
//! drift.
//!
//! The result is exported as a gauge and refreshed at every wall-clock UTC
//! hour boundary (not "one hour after startup"), so downstream billing that
//! aligns on the hour always reads a value sampled on that boundary. One
//! sample is also taken at startup so the gauge is populated straight away.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

/// Gauge: total bytes of all objects under this instance's prefix, as of the
/// most recent successful sample.
pub const OBJECT_STORE_BYTES: &str = "invar_object_store_bytes";
/// Gauge: Unix time (seconds) at which [`OBJECT_STORE_BYTES`] was last
/// successfully sampled. Lets consumers detect a stale value.
pub const OBJECT_STORE_SAMPLED_AT: &str = "invar_object_store_bytes_sampled_at_seconds";

const HOUR: Duration = Duration::from_secs(3600);
/// How long to wait before retrying a failed LIST (bounded by the next hour boundary).
const RETRY_DELAY: Duration = Duration::from_secs(30);

/// Time remaining until the next UTC hour boundary strictly after `now`.
fn until_next_hour(now: SystemTime) -> Duration {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let into_hour = Duration::from_nanos((since_epoch.as_nanos() % HOUR.as_nanos()) as u64);
    HOUR - into_hour
}

/// Sums the sizes of every object under `prefix`.
///
/// `prefix` is matched on whole path segments, so `invar` does not match
/// objects under `invar-2/` or `invar2/`.
pub async fn prefix_bytes(store: &dyn ObjectStore, prefix: &Path) -> object_store::Result<u64> {
    let mut objects = store.list(Some(prefix));
    let mut total: u64 = 0;
    while let Some(meta) = objects.try_next().await? {
        total = total.saturating_add(meta.size);
    }
    Ok(total)
}

/// Background task that keeps the [`OBJECT_STORE_BYTES`] gauge current.
/// The task is aborted when the reporter is dropped or [`stop`](Self::stop)ped.
pub struct UsageReporter(JoinHandle<()>);

impl UsageReporter {
    pub fn spawn(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        metrics::describe_gauge!(
            OBJECT_STORE_BYTES,
            metrics::Unit::Bytes,
            "Bytes stored in the object store under this instance's prefix"
        );
        metrics::describe_gauge!(
            OBJECT_STORE_SAMPLED_AT,
            metrics::Unit::Seconds,
            "Unix time of the last successful object store size sample"
        );
        UsageReporter(tokio::spawn(async move {
            loop {
                let wait = match prefix_bytes(store.as_ref(), &prefix).await {
                    Ok(bytes) => {
                        let now = SystemTime::now();
                        metrics::gauge!(OBJECT_STORE_BYTES).set(bytes as f64);
                        metrics::gauge!(OBJECT_STORE_SAMPLED_AT).set(
                            now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as f64,
                        );
                        until_next_hour(now)
                    }
                    Err(e) => {
                        // Keep the previous value rather than publishing a wrong one;
                        // the sampled-at gauge shows how stale it is.
                        tracing::warn!(error = %e, "failed to measure object store usage");
                        RETRY_DELAY.min(until_next_hour(SystemTime::now()))
                    }
                };
                tokio::time::sleep(wait).await;
            }
        }))
    }

    pub fn stop(&self) {
        self.0.abort();
    }
}

impl Drop for UsageReporter {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    fn at(secs: u64, millis: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis)
    }

    #[test]
    fn next_hour_boundary() {
        assert_eq!(until_next_hour(at(0, 0)), HOUR);
        assert_eq!(
            until_next_hour(at(3600 * 5 + 1, 0)),
            Duration::from_secs(3599)
        );
        assert_eq!(
            until_next_hour(at(3600 * 5 + 3599, 500)),
            Duration::from_millis(500)
        );
        // Exactly on the boundary waits a full hour rather than firing twice.
        assert_eq!(until_next_hour(at(3600 * 7, 0)), HOUR);
    }

    #[tokio::test]
    async fn sums_only_matching_prefix() {
        let store = InMemory::new();
        for (path, len) in [
            ("invar/a", 10),
            ("invar/sub/b", 5),
            ("invar-2/c", 100),
            ("invar2/d", 1000),
            ("other/e", 10_000),
        ] {
            store
                .put(&Path::from(path), vec![0u8; len].into())
                .await
                .unwrap();
        }
        assert_eq!(
            prefix_bytes(&store, &Path::from("invar")).await.unwrap(),
            15
        );
        assert_eq!(
            prefix_bytes(&store, &Path::from("invar-2")).await.unwrap(),
            100
        );
        assert_eq!(prefix_bytes(&store, &Path::from("nope")).await.unwrap(), 0);
    }
}
