//! kv: vendor-neutral abstraction over a transactional LSM-tree key/value
//! store, backed by [SlateDB](https://github.com/slatedb/slatedb).
//!
//! This is the Rust counterpart of the Go `kv` package's SlateDB backend
//! (`kv/slate.go`). SlateDB stores the metadata byte natively and supports
//! per-key TTLs, so values are encoded simply as `[meta][value]` (see
//! `encode_value`/`decode_value`), with the expiry managed by SlateDB
//! itself rather than an embedded timestamp (contrast the Fjall backend,
//! `src/fjall.rs`).
//!
//! Transactions use SlateDB's snapshot isolation; write-write conflicts
//! surface as [`Error::Conflict`].

use async_trait::async_trait;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, HybridCachePolicy,
    PsyncIoEngineConfig,
};
use slatedb::config::{PutOptions, Settings, Ttl};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{CachedEntry, DbCache};
use slatedb::object_store::aws;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{
    CloseReason, Db, DbIterator, DbReader, DbStatus, DbTransaction, Error as SlateError, ErrorKind,
    IsolationLevel,
};
use slatedb_common::metrics::MetricsRecorder;
use std::collections::{Bound, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::kv::{
    write_handle, AckLevel, AckState, BoxFuture, Entry, Error, Item, KeyValueIterator,
    KeyValueStore, Tx, TxIsolation, WriteHandle,
};
use crate::metrics::MetricsRsRecorder;
use crate::time::now_millis;
use crate::usage::UsageReporter;

/// How long a commit at [`AckLevel::Durable`] waits to be confirmed durable
/// before it fails with [`Error::Timeout`] (RFC 0004).
const DURABLE_COMMIT_TIMEOUT: Duration = Duration::from_secs(1);

/// How often the store flushes its WAL while the acknowledgement level is
/// [`AckLevel::Durable`] and commits are waiting, so a durable commit waits
/// tens of milliseconds rather than a whole `SLATEDB_FLUSH_INTERVAL`.
const DURABLE_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

/// Maps a SlateDB error to the kv abstraction's error space, mirroring the
/// Go backend: transaction conflicts surface as [`Error::Conflict`], fencing
/// as [`Error::Fenced`], everything else as [`Error::Undefined`].
fn map_slate_error(err: SlateError) -> Error {
    match err.kind() {
        ErrorKind::Transaction => Error::Conflict,
        ErrorKind::Closed(CloseReason::Fenced) => Error::Fenced,
        _ => Error::Undefined,
    }
}

/// Keep ~20% of the cache disk free for external FS operations;
/// for example, ZFS' metaslab allocator perf can start to degrade
/// beyond this threshold
const FREE_SPACE_HIGH_WATERMARK: f64 = 0.8;

/// Returns 80% of the free bytes available on the filesystem containing `path`.
/// Calls `statvfs(2)` on the path itself, or its parent if the path doesn't exist yet.
fn disk_capacity_bytes(path: &Path) -> Result<usize, Error> {
    let stat_path = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("/"))
    };
    let size = rustix::fs::statvfs(&stat_path)
        .map(|stat| stat.f_bavail.saturating_mul(stat.f_frsize))
        .expect("unable to call statvfs");
    Ok((size as f64 * FREE_SPACE_HIGH_WATERMARK) as usize)
}

async fn build_foyer_cache(
    cache_path: &Path,
    mem_limit_mb: usize,
) -> Result<Arc<dyn DbCache>, Error> {
    let disk_capacity = disk_capacity_bytes(cache_path).expect("couldn't probe disk capacity");
    let mem_capacity = if mem_limit_mb == 0 { 16 } else { mem_limit_mb } * 1024 * 1024;
    let device = FsDeviceBuilder::new(cache_path)
        .with_capacity(disk_capacity)
        .build()
        .expect("couldn't build FsDevice");
    let cache = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_name("slatedb_block_cache")
        .memory(mem_capacity)
        .with_weighter(|_, v: &CachedEntry| v.size())
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await
        .inspect_err(|e| tracing::error!(error=?e, "error building Foyer block cache"))
        .expect("cache build failed");
    Ok(Arc::new(FoyerHybridCache::new_with_cache(cache)) as Arc<dyn DbCache>)
}

/// Cache namespace for a database, derived from its path. It must be stable
/// across restarts so the persisted Foyer cache is recovered on reopen, so this
/// uses FNV-1a rather than `DefaultHasher`, whose output isn't guaranteed stable.
fn db_cache_id(path: &str) -> u64 {
    path.bytes().fold(0xcbf29ce484222325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100000001b3)
    })
}

/// Options for opening a SlateDB-backed [`KeyValueStore`].
#[derive(Debug, Clone, Default)]
pub struct SlateDbOpts {
    pub path: String,
    pub bucket_name: String,
    /// Path for the Foyer on-disk block cache tier. Cache is disabled when `None`.
    pub cache_path: Option<PathBuf>,
    /// Memory limit for the Foyer in-memory cache tier, in MB. Defaults to 16 when 0.
    pub cache_mem_limit: usize,
}

fn s3_store(bucket_name: String) -> Arc<dyn ObjectStore> {
    Arc::new(
        aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .expect("couldn't create s3 client"),
    )
}

async fn optional_cache(
    cache_path: Option<&Path>,
    cache_mem_limit: usize,
) -> Option<Arc<dyn DbCache>> {
    match cache_path {
        Some(cp) => Some(
            build_foyer_cache(cp, cache_mem_limit)
                .await
                .expect("couldn't build foyer cache"),
        ),
        None => None,
    }
}

/// Acknowledgement state shared between a store and its transactions.
#[derive(Default)]
struct Acks {
    level: AckState,
    /// Highest sequence number committed through this handle, so the durable
    /// flusher can tell whether any commit is still waiting to be durable.
    last_seq: AtomicU64,
}

/// Flushes the WAL every [`DURABLE_FLUSH_INTERVAL`] while the level is
/// [`AckLevel::Durable`] and a committed write is not yet durable. Stopped on
/// close/drop.
struct DurableFlusher(tokio::task::JoinHandle<()>);

impl DurableFlusher {
    fn spawn(db: Db, acks: Arc<Acks>) -> Self {
        let rx = db.subscribe();
        Self(tokio::spawn(async move {
            let mut tick = tokio::time::interval(DURABLE_FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if acks.level.get() != AckLevel::Durable {
                    continue;
                }
                let (durable, closed) = {
                    let s = rx.borrow();
                    (s.durable_seq, s.close_reason.is_some())
                };
                if closed {
                    return;
                }
                if acks.last_seq.load(Ordering::Acquire) > durable {
                    if let Err(e) = db.flush().await {
                        tracing::warn!(error = %e, "durable acknowledgement flush failed");
                    }
                }
            }
        }))
    }

    fn stop(&self) {
        self.0.abort();
    }
}

impl Drop for DurableFlusher {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// SlateDB-backed [`KeyValueStore`].
#[derive(Clone)]
pub struct SlateDb {
    db: Db,
    cache: Option<Arc<dyn DbCache>>,
    /// Periodically samples the object store footprint; stopped on close/drop.
    usage: Arc<UsageReporter>,
    acks: Arc<Acks>,
    flusher: Arc<DurableFlusher>,
}

impl SlateDb {
    pub async fn open(opts: SlateDbOpts) -> Result<SlateDb, Error> {
        let store = s3_store(opts.bucket_name);
        let cache = optional_cache(opts.cache_path.as_deref(), opts.cache_mem_limit).await;
        Self::build(opts.path, store, cache).await
    }

    /// Open a store backed by an in-memory object store (for tests).
    pub async fn in_memory() -> Result<SlateDb, Error> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::build("test-db".to_string(), store, None).await
    }

    async fn build(
        path: String,
        store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn DbCache>>,
    ) -> Result<SlateDb, Error> {
        let settings = Settings::from_env_with_default("SLATEDB_", Settings::default())
            .expect("couldn't read settings");
        let mut builder = Db::builder(path.clone(), store.clone());
        builder = builder.with_settings(settings);
        builder =
            builder.with_metrics_recorder(Arc::new(MetricsRsRecorder) as Arc<dyn MetricsRecorder>);
        if let Some(c) = &cache {
            builder = builder.with_db_cache(c.clone(), db_cache_id(&path));
        }

        let db = builder.build().await.map_err(map_slate_error)?;
        // Must match how SlateDB resolves its own `path`, so we count exactly its keys.
        let usage = Arc::new(UsageReporter::spawn(
            store,
            object_store::path::Path::from(path),
        ));
        let acks = Arc::new(Acks::default());
        let flusher = Arc::new(DurableFlusher::spawn(db.clone(), acks.clone()));
        Ok(SlateDb {
            db,
            cache,
            usage,
            acks,
            flusher,
        })
    }

    /// Resolves once another process has opened the store as writer and
    /// fenced this one (RFC 0004). Never resolves for a store closed any
    /// other way.
    pub async fn fenced(&self) {
        let mut rx = self.db.subscribe();
        let fenced = rx
            .wait_for(|s| matches!(s.close_reason, Some(CloseReason::Fenced)))
            .await
            .is_ok();
        if !fenced {
            std::future::pending::<()>().await;
        }
    }

    async fn delete_scanned(&self, prefix: Option<&[u8]>) -> Result<(), Error> {
        let mut iter = match prefix {
            Some(p) => self.db.scan_prefix(p, ..).await,
            None => self.db.scan(..).await,
        }
        .map_err(map_slate_error)?;

        let mut keys = Vec::new();
        while let Some(kv) = iter.next().await.map_err(map_slate_error)? {
            keys.push(kv.key.to_vec());
        }
        if keys.is_empty() {
            return Ok(());
        }

        let tx = self
            .db
            .begin(IsolationLevel::Snapshot)
            .map_err(map_slate_error)?;
        for key in &keys {
            tx.delete(key).map_err(map_slate_error)?;
        }
        let handle = tx.commit().await.map_err(map_slate_error)?;
        acknowledge(&self.db, &self.acks, handle.map(|h| h.seqnum())).await?;
        Ok(())
    }
}

/// A read-only handle on a SlateDB store that can later become its writer
/// (RFC 0004). Opening it proves the store can be opened and read; it serves
/// no clients. [`SlateFollower::promote`] opens the store as writer in the
/// same process, reusing the same block cache.
pub struct SlateFollower {
    path: String,
    store: Arc<dyn ObjectStore>,
    cache: Option<Arc<dyn DbCache>>,
    /// `None` only while a promotion is in progress, or after a failed
    /// promotion could not reopen the reader.
    reader: Option<DbReader>,
}

impl SlateFollower {
    pub async fn open(opts: SlateDbOpts) -> Result<SlateFollower, Error> {
        let store = s3_store(opts.bucket_name);
        let cache = optional_cache(opts.cache_path.as_deref(), opts.cache_mem_limit).await;
        Self::build(opts.path, store, cache).await
    }

    async fn build(
        path: String,
        store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn DbCache>>,
    ) -> Result<SlateFollower, Error> {
        let reader = Self::open_reader(&path, &store, &cache).await?;
        Ok(SlateFollower {
            path,
            store,
            cache,
            reader: Some(reader),
        })
    }

    async fn open_reader(
        path: &str,
        store: &Arc<dyn ObjectStore>,
        cache: &Option<Arc<dyn DbCache>>,
    ) -> Result<DbReader, Error> {
        let mut builder = DbReader::builder(path.to_string(), store.clone());
        if let Some(c) = cache {
            builder = builder.with_db_cache(c.clone(), db_cache_id(path));
        }
        builder.build().await.map_err(map_slate_error)
    }

    /// Closes the reader and opens the store as writer, which fences any
    /// process still writing it. On failure the reader is reopened, so the
    /// follower can be promoted again.
    pub async fn promote(&mut self) -> Result<SlateDb, Error> {
        if let Some(reader) = self.reader.take() {
            if let Err(e) = reader.close().await {
                tracing::warn!(error = %e, "error closing follower reader");
            }
        }
        match SlateDb::build(self.path.clone(), self.store.clone(), self.cache.clone()).await {
            Ok(db) => Ok(db),
            Err(e) => {
                match Self::open_reader(&self.path, &self.store, &self.cache).await {
                    Ok(reader) => self.reader = Some(reader),
                    Err(e) => tracing::error!(error = %e, "couldn't reopen follower reader"),
                }
                Err(e)
            }
        }
    }

    /// Closes the reader and the block cache, for a follower that is shutting
    /// down without being promoted.
    pub async fn close(&mut self) -> Result<(), Error> {
        if let Some(reader) = self.reader.take() {
            reader.close().await.map_err(map_slate_error)?;
        }
        if let Some(cache) = &self.cache {
            cache.close().await.map_err(map_slate_error)?;
        }
        Ok(())
    }
}

/// Waits until `sn` is durable. Returns `Ok(true)` once it is, `Ok(false)` if
/// `limit` elapses first (`None` waits indefinitely), and an error if the
/// store closes before then: [`Error::Fenced`] if another writer fenced it,
/// [`Error::DbClosed`] otherwise. Shared by `WAIT` and durable commits.
async fn wait_durable(db: &Db, sn: u64, limit: Option<Duration>) -> Result<bool, Error> {
    let mut rx = db.subscribe();
    let wait = async {
        let status = rx
            .wait_for(|s| s.durable_seq >= sn || s.close_reason.is_some())
            .await
            .map_err(|_| Error::DbClosed)?;
        settled(&status, sn)
    };
    match limit {
        None => wait.await,
        Some(limit) => tokio::time::timeout(limit, wait).await.unwrap_or(Ok(false)),
    }
}

/// Classifies a status in which `sn` is durable or the store has closed.
fn settled(status: &DbStatus, sn: u64) -> Result<bool, Error> {
    if status.durable_seq >= sn {
        return Ok(true);
    }
    match status.close_reason {
        Some(CloseReason::Fenced) => Err(Error::Fenced),
        _ => Err(Error::DbClosed),
    }
}

/// Completes a successful commit according to the acknowledgement level: at
/// [`AckLevel::Durable`], waits for the write to be durable, failing with
/// [`Error::Timeout`] after [`DURABLE_COMMIT_TIMEOUT`]. `seq` is `None` when
/// the transaction wrote nothing.
async fn acknowledge(db: &Db, acks: &Acks, seq: Option<u64>) -> Result<(), Error> {
    let Some(sn) = seq else {
        return Ok(());
    };
    acks.last_seq.fetch_max(sn, Ordering::AcqRel);
    // Read after the commit has been applied, so a switch to Durable covers
    // every commit that returns after it.
    if acks.level.get() != AckLevel::Durable {
        return Ok(());
    }
    match wait_durable(db, sn, Some(DURABLE_COMMIT_TIMEOUT)).await? {
        true => Ok(()),
        false => Err(Error::Timeout),
    }
}

#[async_trait]
impl KeyValueStore for SlateDb {
    fn new_entry(&self, key: Vec<u8>, value: Vec<u8>) -> Entry {
        Entry::new(key, value)
    }

    async fn begin(&self, _mutating: bool, isolation: TxIsolation) -> Result<Box<dyn Tx>, Error> {
        let level = match isolation {
            TxIsolation::Snapshot => IsolationLevel::Snapshot,
            TxIsolation::SerializableSnapshot => IsolationLevel::SerializableSnapshot,
        };
        let tx = self.db.begin(level).map_err(map_slate_error)?;
        Ok(Box::new(SlateTx {
            tx,
            pending_expiry: Mutex::new(HashMap::new()),
            db: self.db.clone(),
            acks: self.acks.clone(),
        }))
    }

    async fn update<F>(&self, f: F) -> Result<Option<WriteHandle>, Error>
    where
        F: for<'a> FnOnce(&'a dyn Tx) -> BoxFuture<'a, Result<(), Error>> + Send + 'static,
    {
        let tx = self.begin(true, TxIsolation::Snapshot).await?;
        match f(&*tx).await {
            Ok(()) => tx.commit().await,
            Err(e) => {
                tx.discard();
                Err(e)
            }
        }
    }

    async fn read<R, F>(&self, f: F) -> Result<R, Error>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a dyn Tx) -> BoxFuture<'a, Result<R, Error>> + Send + 'static,
    {
        let tx = self.begin(false, TxIsolation::Snapshot).await?;
        match f(&*tx).await {
            Ok(v) => {
                tx.discard();
                Ok(v)
            }
            Err(e) => {
                tx.discard();
                Err(e)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.usage.stop();
        self.flusher.stop();
        self.db.close().await.map_err(map_slate_error)?;
        if let Some(cache) = &self.cache {
            cache.close().await.map_err(map_slate_error)?;
        }
        Ok(())
    }

    async fn merge(&self, _key: &[u8], _operand: &[u8]) -> Result<Option<WriteHandle>, Error> {
        Ok(None)
    }

    async fn sync(&self) -> Result<(), Error> {
        self.db.flush().await.map_err(map_slate_error)
    }

    async fn destroy(&self) -> Result<(), Error> {
        self.delete_scanned(None).await
    }

    async fn drop_prefix(&self, prefix: &[u8]) -> Result<(), Error> {
        self.delete_scanned(Some(prefix)).await
    }

    async fn await_until(&self, handle: WriteHandle, duration: Duration) -> Result<bool, Error> {
        // Real WAIT/WAITAOF semantics: timeout == 0 means block forever, not
        // "return immediately". Either way, a store that closes or is fenced
        // first returns an error instead of waiting forever.
        let limit = (!duration.is_zero()).then_some(duration);
        wait_durable(&self.db, handle.sequence_number(), limit).await
    }

    fn set_ack_level(&self, level: AckLevel) {
        self.acks.level.set(level);
    }

    fn ack_level(&self) -> AckLevel {
        self.acks.level.get()
    }
}

struct SlateTx {
    tx: DbTransaction,
    /// Expiry (absolute Unix ms, `None` = never) of every key written in this
    /// transaction. SlateDB stamps `expire_ts` when a write lands, so an
    /// uncommitted write reads back without one; without this, updating a key
    /// twice in one transaction (MULTI, Lua scripts) would drop its TTL.
    pending_expiry: Mutex<HashMap<Vec<u8>, Option<i64>>>,
    /// The store's handle, for waiting on durability at commit.
    db: Db,
    acks: Arc<Acks>,
}

impl SlateTx {
    /// Locks the pending-expiry map. A panic while it was held cannot leave
    /// the map half-updated (each critical section is a single insert, remove
    /// or lookup), so a poisoned lock is safe to keep using.
    fn pending_expiry(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Option<i64>>> {
        self.pending_expiry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl Tx for SlateTx {
    async fn get(&self, key: &[u8]) -> Result<Item, Error> {
        let kv = self.tx.get_key_value(key).await.map_err(map_slate_error)?;
        match kv {
            Some(kv) => {
                let (value, meta) = decode_value(&kv.value);
                let expire_ts = match self.pending_expiry().get(key) {
                    Some(pending) => *pending,
                    None => kv.expire_ts,
                };
                let item = Item::new(kv.key.to_vec(), value.to_vec(), meta, expire_ts);
                if item.is_expired() {
                    Err(Error::KeyNotFound)
                } else {
                    Ok(item)
                }
            }
            None => Err(Error::KeyNotFound),
        }
    }

    fn set(&self, entry: Entry) -> Result<(), Error> {
        let stored = encode_value(entry.metadata_byte(), entry.value());
        let ttl = entry.ttl_value();
        self.pending_expiry().insert(
            entry.key().to_vec(),
            (!ttl.is_zero()).then(|| now_millis().saturating_add(ttl.as_millis() as i64)),
        );
        if !ttl.is_zero() {
            let ms = ttl.as_millis() as u64;
            self.tx
                .put_with_options(
                    entry.key(),
                    stored,
                    &PutOptions {
                        ttl: Ttl::ExpireAfterMillis(ms),
                    },
                )
                .map_err(map_slate_error)
        } else {
            self.tx.put(entry.key(), stored).map_err(map_slate_error)
        }
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.pending_expiry().remove(key);
        self.tx.delete(key).map_err(map_slate_error)
    }

    async fn new_range_iterator(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<Box<dyn KeyValueIterator>, Error> {
        let range = (start, end);
        let iter = self.tx.scan(range).await.map_err(map_slate_error)?;
        Ok(Box::new(SlateIterator {
            iter: Some(iter),
            err: None,
            cur: None,
        }))
    }

    async fn new_prefix_iterator(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueIterator>, Error> {
        let iter = self
            .tx
            .scan_prefix(prefix, ..)
            .await
            .map_err(map_slate_error)?;
        Ok(Box::new(SlateIterator {
            iter: Some(iter),
            err: None,
            cur: None,
        }))
    }

    async fn commit(self: Box<Self>) -> Result<Option<WriteHandle>, Error> {
        let seq = self
            .tx
            .commit()
            .await
            .map_err(map_slate_error)?
            .map(|h| h.seqnum());
        acknowledge(&self.db, &self.acks, seq).await?;
        Ok(seq.map(write_handle))
    }

    fn discard(self: Box<Self>) {
        self.tx.rollback();
    }
}

struct SlateIterator {
    iter: Option<DbIterator>,
    err: Option<Error>,
    cur: Option<Item>,
}

#[async_trait]
impl KeyValueIterator for SlateIterator {
    async fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        let Some(iter) = self.iter.as_mut() else {
            return false;
        };
        match iter.next().await {
            Ok(Some(kv)) => {
                let (value, meta) = decode_value(&kv.value);
                self.cur = Some(Item::new(
                    kv.key.to_vec(),
                    value.to_vec(),
                    meta,
                    kv.expire_ts,
                ));
                true
            }
            Ok(None) => false,
            Err(e) => {
                self.err = Some(map_slate_error(e));
                false
            }
        }
    }

    fn item(&self) -> Option<&Item> {
        self.cur.as_ref()
    }

    fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.iter = None;
        Ok(())
    }
}

fn encode_value(meta: u8, val: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + val.len());
    out.push(meta);
    out.extend_from_slice(val);
    out
}

fn decode_value(stored: &[u8]) -> (&[u8], u8) {
    if stored.is_empty() {
        return (&[], 0);
    }
    (&stored[1..], stored[0])
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn in_memory() -> SlateDb {
        SlateDb::in_memory().await.expect("open in-memory store")
    }

    async fn write(store: &SlateDb, key: &[u8], value: &[u8]) {
        let key = key.to_vec();
        let value = value.to_vec();
        store
            .update(move |tx: &dyn Tx| {
                Box::pin(async move {
                    tx.set(Entry::new(key, value))?;
                    Ok(())
                })
            })
            .await
            .expect("update failed");
    }

    async fn read_value(store: &SlateDb, key: &[u8]) -> Result<Vec<u8>, Error> {
        let key = key.to_vec();
        store
            .read(move |tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(&key).await?;
                    Ok(item.value().to_vec())
                })
            })
            .await
    }

    // --- KeyValueStore tests (SlateDB backend) ---

    #[tokio::test]
    async fn new_entry() {
        let store = in_memory().await;
        let entry = store.new_entry(b"mykey".to_vec(), b"myval".to_vec());
        assert_eq!(entry.key(), b"mykey");
        assert_eq!(entry.value(), b"myval");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_entry_isolation() {
        let store = in_memory().await;
        let e1 = store.new_entry(b"a".to_vec(), b"1".to_vec());
        let e2 = store.new_entry(b"b".to_vec(), b"2".to_vec());
        assert_eq!(e1.key(), b"a");
        assert_eq!(e2.key(), b"b");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn entry_metadata() {
        let store = in_memory().await;
        let entry = store.new_entry(b"k".to_vec(), b"v".to_vec());
        let chained = entry.clone().metadata(0x42);
        assert_eq!(chained.metadata_byte(), 0x42);
        assert_eq!(entry.metadata_byte(), 0);

        write(&store, b"k", b"v").await;
        store
            .update(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"k").await?;
                    assert_eq!(item.value(), b"v");
                    assert_eq!(item.metadata(), 0);
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn entry_ttl() {
        let store = in_memory().await;
        let entry = store
            .new_entry(b"k".to_vec(), b"v".to_vec())
            .ttl(Duration::from_secs(5));
        assert_eq!(entry.ttl_value(), Duration::from_secs(5));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn entry_chaining() {
        let store = in_memory().await;
        let entry = store
            .new_entry(b"k".to_vec(), b"v".to_vec())
            .metadata(0x01)
            .ttl(Duration::from_secs(10));
        assert_eq!(entry.metadata_byte(), 0x01);
        assert_eq!(entry.ttl_value(), Duration::from_secs(10));

        store
            .update(move |tx: &dyn Tx| {
                let entry = entry.clone();
                Box::pin(async move {
                    tx.set(entry)?;
                    Ok(())
                })
            })
            .await
            .expect("set with chained entry failed");

        assert_eq!(read_value(&store, b"k").await.expect("read"), b"v");
        store.close().await.expect("close");
    }

    // --- Update / Read ---

    #[tokio::test]
    async fn update_and_read() {
        let store = in_memory().await;
        write(&store, b"hello", b"world").await;
        assert_eq!(read_value(&store, b"hello").await.expect("read"), b"world");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn update_rollback() {
        let store = in_memory().await;
        write(&store, b"k", b"v1").await;

        let boom = Error::Conflict;
        let err = store
            .update(move |tx: &dyn Tx| {
                let boom = boom.clone();
                Box::pin(async move {
                    tx.set(Entry::new(b"k".to_vec(), b"v2".to_vec()))?;
                    Err(boom)
                })
            })
            .await;
        assert!(err.is_err(), "expected error from Update, got none");

        assert_eq!(read_value(&store, b"k").await.expect("read"), b"v1");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn read_return_value() {
        let store = in_memory().await;
        write(&store, b"x", b"42").await;
        let result: String = store
            .read(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"x").await?;
                    Ok(String::from_utf8(item.value().to_vec()).unwrap())
                })
            })
            .await
            .expect("read failed");
        assert_eq!(result, "42");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn read_error_propagation() {
        let store = in_memory().await;
        let boom = Error::Conflict;
        let err: Result<(), Error> = store
            .read(move |_tx: &dyn Tx| {
                let boom = boom.clone();
                Box::pin(async move { Err(boom) })
            })
            .await;
        assert_eq!(err.unwrap_err(), Error::Conflict);
        store.close().await.expect("close");
    }

    // --- Begin ---

    #[tokio::test]
    async fn begin_readonly() {
        let store = in_memory().await;
        write(&store, b"rkey", b"rval").await;

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let item = tx.get(b"rkey").await.expect("get failed");
        assert_eq!(item.value(), b"rval");
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_mutating() {
        let store = in_memory().await;
        let tx = store
            .begin(true, TxIsolation::Snapshot)
            .await
            .expect("begin");
        tx.set(Entry::new(b"mkey".to_vec(), b"mval".to_vec()))
            .expect("set failed");
        tx.commit().await.expect("commit failed");

        assert_eq!(read_value(&store, b"mkey").await.expect("read"), b"mval");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_discard() {
        let store = in_memory().await;
        let tx = store
            .begin(true, TxIsolation::Snapshot)
            .await
            .expect("begin");
        tx.set(Entry::new(b"dkey".to_vec(), b"dval".to_vec()))
            .expect("set failed");
        tx.discard();

        let err = read_value(&store, b"dkey").await;
        assert_eq!(err.unwrap_err(), Error::KeyNotFound);
        store.close().await.expect("close");
    }

    // --- Tx Get/Set/Delete ---

    #[tokio::test]
    async fn tx_set_and_get() {
        let store = in_memory().await;
        write(&store, b"key1", b"val1").await;
        assert_eq!(read_value(&store, b"key1").await.expect("get"), b"val1");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn tx_get_not_found() {
        let store = in_memory().await;
        let err = read_value(&store, b"nonexistent").await;
        assert_eq!(err.unwrap_err(), Error::KeyNotFound);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn tx_delete() {
        let store = in_memory().await;
        write(&store, b"delme", b"gone").await;

        store
            .update(|tx: &dyn Tx| {
                Box::pin(async move {
                    tx.delete(b"delme")?;
                    Ok(())
                })
            })
            .await
            .expect("delete failed");

        let err = read_value(&store, b"delme").await;
        assert_eq!(err.unwrap_err(), Error::KeyNotFound);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn tx_overwrite() {
        let store = in_memory().await;
        write(&store, b"ow", b"v1").await;
        write(&store, b"ow", b"v2").await;
        assert_eq!(read_value(&store, b"ow").await.expect("read"), b"v2");
        store.close().await.expect("close");
    }

    // --- Item ---

    #[tokio::test]
    async fn item_key() {
        let store = in_memory().await;
        write(&store, b"itemkey", b"val").await;
        store
            .read(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"itemkey").await?;
                    assert_eq!(item.key(), b"itemkey");
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    /// SlateDB stamps `expire_ts` when a write lands, so an uncommitted write
    /// reads back without one. `get` must still report the TTL the
    /// transaction itself set, or a second update to the same key within the
    /// transaction (MULTI, Lua scripts) would lose it.
    #[tokio::test]
    async fn uncommitted_ttl_is_visible_to_reads_in_the_same_tx() {
        let store = in_memory().await;
        let tx = store
            .begin(true, TxIsolation::Snapshot)
            .await
            .expect("begin");

        tx.set(Entry::new(b"k".to_vec(), b"v".to_vec()).ttl(Duration::from_secs(60)))
            .expect("set");
        let remaining = tx
            .get(b"k")
            .await
            .expect("get")
            .ttl()
            .expect("pending write keeps its TTL");
        assert!(
            remaining > Duration::from_secs(55) && remaining <= Duration::from_secs(60),
            "remaining TTL was {remaining:?}"
        );

        // Rewriting without a TTL clears it, as a plain overwrite should.
        tx.set(Entry::new(b"k".to_vec(), b"v2".to_vec()))
            .expect("set");
        assert_eq!(tx.get(b"k").await.expect("get").ttl(), None);
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_ttl() {
        let store = in_memory().await;
        write(&store, b"notls", b"val").await;
        store
            .read(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"notls").await?;
                    assert_eq!(item.ttl(), None);
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_metadata_and_expiry() {
        let store = in_memory().await;
        store
            .update(|tx: &dyn Tx| {
                Box::pin(async move {
                    let entry = Entry::new(b"meta".to_vec(), b"val".to_vec())
                        .metadata(0x2A)
                        .ttl(Duration::from_secs(10));
                    tx.set(entry)?;
                    Ok(())
                })
            })
            .await
            .expect("set failed");

        store
            .read(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"meta").await?;
                    assert_eq!(item.metadata(), 0x2A);
                    assert_ne!(item.ttl(), None, "expiry set by TTL(10s)");
                    assert!(item.ttl().unwrap() > Duration::ZERO);
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_value_copy() {
        let store = in_memory().await;
        write(&store, b"copytest", b"original").await;
        let v1 = read_value(&store, b"copytest").await.expect("read");
        let v2 = read_value(&store, b"copytest").await.expect("read");
        assert_eq!(v1, b"original");
        assert_eq!(v2, b"original");
        store.close().await.expect("close");
    }

    // --- Multiple keys ---

    #[tokio::test]
    async fn multiple_keys() {
        let store = in_memory().await;
        let pairs = [("a", "1"), ("b", "2"), ("c", "3")];

        store
            .update(move |tx: &dyn Tx| {
                Box::pin(async move {
                    for (k, v) in pairs {
                        tx.set(Entry::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))?;
                    }
                    Ok(())
                })
            })
            .await
            .expect("batch set failed");

        for (k, want) in pairs {
            assert_eq!(
                read_value(&store, k.as_bytes()).await.expect("get"),
                want.as_bytes()
            );
        }
        store.close().await.expect("close");
    }

    // --- Close ---

    #[tokio::test]
    async fn close() {
        let store = in_memory().await;
        write(&store, b"closeme", b"val").await;
        store.close().await.expect("close returned error");
    }

    // --- Destroy / DropPrefix ---

    #[tokio::test]
    async fn destroy() {
        let store = in_memory().await;
        for k in ["a", "b", "c"] {
            write(&store, k.as_bytes(), b"v").await;
        }

        store.destroy().await.expect("destroy returned error");

        for k in ["a", "b", "c"] {
            assert_eq!(
                read_value(&store, k.as_bytes()).await.unwrap_err(),
                Error::KeyNotFound,
                "key {k} should be gone after Destroy"
            );
        }
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn destroy_leaves_store_usable() {
        let store = in_memory().await;
        write(&store, b"old", b"v").await;
        store.destroy().await.expect("destroy returned error");

        write(&store, b"new", b"v2").await;
        assert_eq!(read_value(&store, b"new").await.expect("read"), b"v2");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn drop_prefix_drops_matching_keys() {
        let store = in_memory().await;
        for k in ["user:1", "user:2", "user:1:extra"] {
            write(&store, k.as_bytes(), b"v").await;
        }

        store
            .drop_prefix(b"user:")
            .await
            .expect("drop_prefix failed");

        for k in ["user:1", "user:2", "user:1:extra"] {
            assert_eq!(
                read_value(&store, k.as_bytes()).await.unwrap_err(),
                Error::KeyNotFound,
                "key {k} should be gone after DropPrefix"
            );
        }
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn drop_prefix_preserves_non_matching_keys() {
        let store = in_memory().await;
        let dropped = ["user:1", "user:2"];
        let preserved = ["other:1", "prefuser:1", "user"];
        for k in dropped.iter().chain(preserved.iter()) {
            write(&store, k.as_bytes(), b"v").await;
        }

        store
            .drop_prefix(b"user:")
            .await
            .expect("drop_prefix failed");

        for k in dropped {
            assert_eq!(
                read_value(&store, k.as_bytes()).await.unwrap_err(),
                Error::KeyNotFound,
                "key {k} should be dropped"
            );
        }
        for k in preserved {
            read_value(&store, k.as_bytes())
                .await
                .expect("preserved key should still exist");
        }
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn drop_prefix_no_match_is_noop() {
        let store = in_memory().await;
        write(&store, b"a", b"v").await;

        store
            .drop_prefix(b"missing:")
            .await
            .expect("drop_prefix returned error");

        assert_eq!(read_value(&store, b"a").await.expect("read"), b"v");
        store.close().await.expect("close");
    }

    // --- NewIterator ---

    #[tokio::test]
    async fn new_iterator_returns_items() {
        let store = in_memory().await;
        store
            .update(|tx: &dyn Tx| {
                Box::pin(async move {
                    tx.set(Entry::new(b"user:1".to_vec(), b"v1".to_vec()))?;
                    tx.set(Entry::new(b"user:2".to_vec(), b"v2".to_vec()))?;
                    tx.set(Entry::new(b"user:3".to_vec(), b"v3".to_vec()))?;
                    tx.set(Entry::new(b"other".to_vec(), b"x".to_vec()))?;
                    Ok(())
                })
            })
            .await
            .expect("set failed");

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx.new_prefix_iterator(b"user:").await.expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(
            keys,
            vec![b"user:1".to_vec(), b"user:2".to_vec(), b"user:3".to_vec()]
        );
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_inclusive_start_exclusive_end() {
        let store = in_memory().await;
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Included(b"b"), Bound::Excluded(b"d"))
            .await
            .expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_unbounded_bounds_yield_all_keys() {
        let store = in_memory().await;
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_exclusive_start_resumes_after_cursor() {
        let store = in_memory().await;
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Excluded(b"b"), Bound::Unbounded)
            .await
            .expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec()]);
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_prefix_end_excludes_following_keys() {
        let store = in_memory().await;
        for (k, v) in [("user:1", "a"), ("user:2", "b"), ("other", "c")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        // The end bound must be exclusive: only user:* keys are in range.
        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Included(b"user:"), Bound::Excluded(b"user;"))
            .await
            .expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(keys, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_no_matches_is_empty() {
        let store = in_memory().await;
        write(&store, b"a", b"1").await;
        write(&store, b"b", b"2").await;

        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Included(b"x"), Bound::Unbounded)
            .await
            .expect("iterator");
        assert!(!it.next().await);
        assert!(it.err().is_none());
        tx.discard();
        store.close().await.expect("close");
    }

    // --- Merge stub ---

    #[tokio::test]
    async fn merge_returns_none() {
        let store = in_memory().await;
        let handle = store.merge(b"key", b"delta").await.expect("merge error");
        assert!(
            handle.is_none(),
            "Merge should return no handle (not yet implemented)"
        );
        store.close().await.expect("close");
    }

    // --- Concurrency ---

    #[tokio::test]
    async fn concurrent_update() {
        let store = Arc::new(in_memory().await);
        write(&store, b"counter", b"0").await;

        let mut handles = Vec::new();
        for _ in 0..10 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let _ = store
                    .update(|tx: &dyn Tx| {
                        Box::pin(async move {
                            tx.set(Entry::new(b"counter".to_vec(), b"0".to_vec()))?;
                            Ok(())
                        })
                    })
                    .await;
            }));
        }
        for handle in handles {
            handle.await.expect("task panicked");
        }

        store
            .read(|tx: &dyn Tx| {
                Box::pin(async move { tx.get(b"counter").await.map(|item| item.value().to_vec()) })
            })
            .await
            .expect("store corrupted after concurrent updates");
        store.close().await.expect("close");
    }

    // --- Durable acknowledgements and followers (RFC 0004) ---

    /// Opens a writer on `store`, sharing it with other handles in the test.
    async fn writer_on(store: &Arc<dyn ObjectStore>) -> SlateDb {
        SlateDb::build("shared-db".to_string(), store.clone(), None)
            .await
            .expect("open writer")
    }

    async fn commit_one(store: &SlateDb, key: &[u8], value: &[u8]) -> Result<Option<u64>, Error> {
        let tx = store.begin(true, TxIsolation::Snapshot).await?;
        tx.set(Entry::new(key.to_vec(), value.to_vec()))?;
        Ok(tx.commit().await?.map(|h| h.sequence_number()))
    }

    #[tokio::test]
    async fn ack_level_defaults_to_buffered() {
        let store = in_memory().await;
        assert_eq!(store.ack_level(), AckLevel::Buffered);
        store.set_ack_level(AckLevel::Durable);
        assert_eq!(store.ack_level(), AckLevel::Durable);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn durable_commit_is_durable_when_it_returns() {
        let store = in_memory().await;
        store.set_ack_level(AckLevel::Durable);
        for i in 0..5u8 {
            let seq = commit_one(&store, &[b'k', i], b"v")
                .await
                .expect("durable commit")
                .expect("a write returns a handle");
            assert!(
                store.db.subscribe().borrow().durable_seq >= seq,
                "commit returned before its write was durable"
            );
        }
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn durable_level_applies_to_transactions_already_open() {
        let store = in_memory().await;
        let tx = store
            .begin(true, TxIsolation::Snapshot)
            .await
            .expect("begin");
        tx.set(Entry::new(b"k".to_vec(), b"v".to_vec()))
            .expect("set");
        store.set_ack_level(AckLevel::Durable);
        let seq = tx
            .commit()
            .await
            .expect("commit")
            .expect("handle")
            .sequence_number();
        assert!(store.db.subscribe().borrow().durable_seq >= seq);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn durable_drop_prefix_is_durable_when_it_returns() {
        let store = in_memory().await;
        write(&store, b"p:1", b"v").await;
        store.set_ack_level(AckLevel::Durable);
        store.drop_prefix(b"p:").await.expect("drop_prefix");
        let status = store.db.subscribe().borrow().clone();
        assert!(status.durable_seq >= store.acks.last_seq.load(Ordering::Acquire));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn durable_commit_on_a_fenced_writer_is_never_acknowledged() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = writer_on(&objects).await;
        old.set_ack_level(AckLevel::Durable);
        commit_one(&old, b"before", b"v")
            .await
            .expect("commit before fencing");

        let new = writer_on(&objects).await; // fences `old`
        let result = commit_one(&old, b"after", b"v").await;
        assert!(
            result.is_err(),
            "a fenced writer acknowledged a durable commit: {result:?}"
        );
        new.close().await.expect("close new writer");
    }

    #[tokio::test]
    async fn fenced_resolves_and_wait_stops_waiting_once_fenced() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = writer_on(&objects).await;
        let new = writer_on(&objects).await; // fences `old`
                                             // A write makes the old writer try its WAL, which is where it notices.
        let _ = commit_one(&old, b"after", b"v").await;
        tokio::time::timeout(Duration::from_secs(10), old.fenced())
            .await
            .expect("fencing was never reported");

        // `WAIT 0` used to wait forever on a fenced writer.
        let waited = tokio::time::timeout(
            Duration::from_secs(5),
            old.await_until(write_handle(u64::MAX), Duration::ZERO),
        )
        .await
        .expect("WAIT 0 on a fenced writer should not block");
        assert_eq!(waited, Err(Error::Fenced));
        new.close().await.expect("close new writer");
    }

    #[tokio::test]
    async fn follower_promotes_and_sees_the_previous_writers_data() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = writer_on(&objects).await;
        write(&old, b"k", b"v").await;

        let mut follower = SlateFollower::build("shared-db".to_string(), objects.clone(), None)
            .await
            .expect("open follower");
        old.close().await.expect("old writer shuts down");

        let new = follower.promote().await.expect("promote");
        assert_eq!(read_value(&new, b"k").await.expect("read"), b"v");
        write(&new, b"k2", b"v2").await;
        new.close().await.expect("close");
    }

    #[tokio::test]
    async fn follower_close_releases_the_reader() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = writer_on(&objects).await;
        let mut follower = SlateFollower::build("shared-db".to_string(), objects.clone(), None)
            .await
            .expect("open follower");
        follower.close().await.expect("close follower");
        follower.close().await.expect("closing twice is harmless");
        writer.close().await.expect("close writer");
    }
}
