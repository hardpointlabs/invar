//! kv: vendor-neutral abstraction over a transactional LSM-tree key/value
//! store, backed by [Fjall](https://github.com/fjall-rs/fjall).
//!
//! This is the Rust counterpart of the Go `kv` package's BadgerDB backend
//! (`kv/badger.go`): an embeddable, pure-Rust LSM tree with serializable
//! snapshot-isolation transactions. Where the SlateDB backend (`slate`)
//! stores the metadata byte natively and keeps TTLs in SlateDB itself, Fjall
//! has no per-key TTL support, so this backend encodes both the metadata byte
//! and the absolute expiry timestamp into the stored value (see
//! `encode`/`decode`).
//!
//! Transactions are provided by Fjall's optimistic (SSI) concurrency control,
//! mirroring the guarantees documented on the `kv` module: snapshot
//! isolation with write-write conflict detection surfaced as
//! [`Error::Conflict`].

use std::collections::Bound;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fjall::{
    Conflict, Iter, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace,
    OptimisticWriteTx, PersistMode, Readable,
};

use crate::kv::{BoxFuture, Entry, Error, Item, KeyValueIterator, KeyValueStore, Tx, WriteHandle};

/// Length of the metadata byte prefix on each stored value.
const META_LEN: usize = 1;
/// Length of the encoded absolute expiry timestamp (Unix millis, big-endian).
const EXPIRY_LEN: usize = 8;
/// Total header length of the value encoding.
const HEADER_LEN: usize = META_LEN + EXPIRY_LEN;

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Maps a Fjall error to the kv abstraction's error space. Fjall reports
/// missing keys as `Ok(None)` rather than an error, so every propagated
/// error here maps to [`Error::Undefined`]; conflicts are handled separately
/// where commits are interpreted.
fn map_fjall_error(_err: fjall::Error) -> Error {
    Error::Undefined
}

/// Encodes a value for storage as `[meta][expiry millis u64 BE][value]`.
///
/// A zero expiry means the entry never expires. When a TTL is set, the
/// absolute expiry is computed at write time, which lets [`decode`] expose
/// both `expires_at()` and the remaining `ttl()` on read.
fn encode(meta: u8, ttl: Duration, value: &[u8]) -> Vec<u8> {
    let expiry = if ttl.is_zero() {
        0
    } else {
        (now_millis().saturating_add(ttl.as_millis() as i64)) as u64
    };

    let mut out = Vec::with_capacity(HEADER_LEN + value.len());
    out.push(meta);
    out.extend_from_slice(&expiry.to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// Decodes a stored value back into `(meta, expiry_millis, value)`.
fn decode(stored: &[u8]) -> (u8, u64, &[u8]) {
    if stored.len() < HEADER_LEN {
        // Defensive: treat a malformed/empty payload as a never-expiring
        // entry with no metadata.
        return (0, 0, stored);
    }

    let meta = stored[0];
    let mut expiry = [0u8; EXPIRY_LEN];
    expiry.copy_from_slice(&stored[META_LEN..HEADER_LEN]);
    (meta, u64::from_be_bytes(expiry), &stored[HEADER_LEN..])
}

/// Translates a stored value into an [`Item`], treating entries that have
/// passed their expiry as absent ([`Error::KeyNotFound`]).
fn decode_item(key: Vec<u8>, stored: &[u8]) -> Result<Item, Error> {
    let (meta, expiry, value) = decode(stored);
    if expiry != 0 && expiry as i64 <= now_millis() {
        return Err(Error::KeyNotFound);
    }

    let expire_ts = if expiry == 0 {
        None
    } else {
        Some(expiry as i64)
    };
    Ok(Item::new(key, value.to_vec(), meta, expire_ts))
}

/// Fjall-backed [`KeyValueStore`].
#[derive(Clone)]
pub struct FjallDb {
    db: OptimisticTxDatabase,
    keyspace: OptimisticTxKeyspace,
}

impl FjallDb {
    /// Opens (creating if needed) a durable store rooted at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<FjallDb, Error> {
        Self::build(OptimisticTxDatabase::builder(path), false)
    }

    /// Opens a store at `path` that is cleaned up automatically when the
    /// store is dropped. Useful for tests.
    pub fn temporary(path: impl AsRef<Path>) -> Result<FjallDb, Error> {
        Self::build(OptimisticTxDatabase::builder(path), true)
    }

    fn build(
        builder: fjall::DatabaseBuilder<OptimisticTxDatabase>,
        temporary: bool,
    ) -> Result<FjallDb, Error> {
        let builder = if temporary {
            builder.temporary(true)
        } else {
            builder
        };
        let db = builder.open().map_err(map_fjall_error)?;
        let keyspace = db
            .keyspace("default", KeyspaceCreateOptions::default)
            .map_err(map_fjall_error)?;
        Ok(FjallDb { db, keyspace })
    }

    /// Returns the number of items currently stored in the default keyspace.
    pub fn approximate_len(&self) -> usize {
        self.keyspace.inner().approximate_len()
    }
}

#[async_trait]
impl KeyValueStore for FjallDb {
    fn new_entry(&self, key: Vec<u8>, value: Vec<u8>) -> Entry {
        Entry::new(key, value)
    }

    async fn begin(&self, _mutating: bool) -> Result<Box<dyn Tx>, Error> {
        let tx = self.db.write_tx().map_err(map_fjall_error)?;
        Ok(Box::new(FjallTx {
            keyspace: self.keyspace.clone(),
            tx: Mutex::new(tx),
        }))
    }

    async fn update<F>(&self, f: F) -> Result<(), Error>
    where
        F: for<'a> FnOnce(&'a dyn Tx) -> BoxFuture<'a, Result<(), Error>> + Send + 'static,
    {
        let tx = self.begin(true).await?;
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
        let tx = self.begin(false).await?;
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
        // Fjall closes its background threads and flushes on drop; the best
        // a still-alive handle can do is force the data out to disk.
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(map_fjall_error)
    }

    async fn merge(&self, _key: &[u8], _operand: &[u8]) -> Result<Option<WriteHandle>, Error> {
        Ok(None)
    }

    async fn sync(&self) -> Result<(), Error> {
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(map_fjall_error)
    }

    async fn destroy(&self) -> Result<(), Error> {
        self.keyspace.inner().clear().map_err(map_fjall_error)
    }

    async fn drop_prefix(&self, prefix: &[u8]) -> Result<(), Error> {
        let mut keys = Vec::new();
        for guard in self.keyspace.inner().prefix(prefix) {
            keys.push(guard.key().map_err(map_fjall_error)?);
        }
        if keys.is_empty() {
            return Ok(());
        }

        let mut tx = self.db.write_tx().map_err(map_fjall_error)?;
        for key in keys {
            tx.remove(&self.keyspace, key);
        }
        match tx.commit().map_err(map_fjall_error)? {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::Conflict),
        }
    }
}

struct FjallTx {
    keyspace: OptimisticTxKeyspace,
    tx: Mutex<OptimisticWriteTx>,
}

#[async_trait]
impl Tx for FjallTx {
    async fn get(&self, key: &[u8]) -> Result<Item, Error> {
        let tx = self.tx.lock().expect("fjall tx mutex poisoned");
        let value = tx.get(&self.keyspace, key).map_err(map_fjall_error)?;
        drop(tx);
        match value {
            Some(value) => decode_item(key.to_vec(), &value),
            None => Err(Error::KeyNotFound),
        }
    }

    fn set(&self, entry: Entry) -> Result<(), Error> {
        let stored = encode(entry.metadata_byte(), entry.ttl_value(), entry.value());
        let mut tx = self.tx.lock().expect("fjall tx mutex poisoned");
        tx.insert(&self.keyspace, entry.key(), stored);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let mut tx = self.tx.lock().expect("fjall tx mutex poisoned");
        tx.remove(&self.keyspace, key);
        Ok(())
    }

    async fn new_range_iterator(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Box<dyn KeyValueIterator>, Error> {
        let range = (start, end);
        let tx = self.tx.lock().expect("fjall tx mutex poisoned");
        let iter = tx.range::<&[u8], _>(&self.keyspace, range);
        drop(tx);
        Ok(Box::new(FjallIterator{
            iter: Some(Mutex::new(iter)),
            cur: None,
            err: None,
        }))
    }

    async fn new_prefix_iterator(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueIterator>, Error> {
        let tx = self.tx.lock().expect("fjall tx mutex poisoned");
        let iter = tx.prefix(&self.keyspace, prefix);
        drop(tx);
        Ok(Box::new(FjallIterator {
            iter: Some(Mutex::new(iter)),
            cur: None,
            err: None,
        }))
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        let tx = self.tx.into_inner().expect("fjall tx mutex poisoned");
        match tx.commit().map_err(map_fjall_error)? {
            Ok(()) => Ok(()),
            Err(Conflict) => Err(Error::Conflict),
        }
    }

    fn discard(self: Box<Self>) {
        let tx = self.tx.into_inner().expect("fjall tx mutex poisoned");
        tx.rollback();
    }
}

/// Iterator over a prefix of the keyspace, seen through the enclosing
/// transaction's snapshot. Expired entries are skipped transparently.
struct FjallIterator {
    iter: Option<Mutex<Iter>>,
    cur: Option<Item>,
    err: Option<Error>,
}

#[async_trait]
impl KeyValueIterator for FjallIterator {
    async fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        let Some(iter) = self.iter.as_mut() else {
            return false;
        };

        loop {
            let guard = iter.get_mut().expect("fjall iter mutex poisoned").next();
            let Some(guard) = guard else {
                self.cur = None;
                return false;
            };

            let (key, value) = match guard.into_inner() {
                Ok((key, value)) => (key.to_vec(), value),
                Err(e) => {
                    self.err = Some(map_fjall_error(e));
                    return false;
                }
            };

            match decode_item(key, &value) {
                Ok(item) => {
                    self.cur = Some(item);
                    return true;
                }
                Err(Error::KeyNotFound) => continue,
                Err(e) => {
                    self.err = Some(e);
                    return false;
                }
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
        // Dropping the iterator releases the snapshot it held open.
        self.iter = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{Error, KeyValueStore, Tx};
    use std::sync::Arc;

    /// Opens a temporary Fjall-backed store in a unique directory. The
    /// directory is removed automatically when the store is dropped.
    fn temp_store() -> FjallDb {
        let path = tempfile::TempDir::new().expect("create tempdir").keep();
        FjallDb::temporary(path).expect("open temporary store")
    }

    async fn write(store: &FjallDb, key: &[u8], value: &[u8]) {
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

    async fn read_value(store: &FjallDb, key: &[u8]) -> Result<Vec<u8>, Error> {
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

    // --- KeyValueStore tests (Fjall backend) ---

    #[tokio::test]
    async fn new_entry() {
        let store = temp_store();
        let entry = store.new_entry(b"mykey".to_vec(), b"myval".to_vec());
        assert_eq!(entry.key(), b"mykey");
        assert_eq!(entry.value(), b"myval");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_entry_isolation() {
        let store = temp_store();
        let e1 = store.new_entry(b"a".to_vec(), b"1".to_vec());
        let e2 = store.new_entry(b"b".to_vec(), b"2".to_vec());
        assert_eq!(e1.key(), b"a");
        assert_eq!(e2.key(), b"b");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn entry_metadata() {
        let store = temp_store();
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
        let store = temp_store();
        let entry = store
            .new_entry(b"k".to_vec(), b"v".to_vec())
            .ttl(Duration::from_secs(5));
        assert_eq!(entry.ttl_value(), Duration::from_secs(5));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn entry_chaining() {
        let store = temp_store();
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
        let store = temp_store();
        write(&store, b"hello", b"world").await;
        assert_eq!(read_value(&store, b"hello").await.expect("read"), b"world");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn update_rollback() {
        let store = temp_store();
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
        let store = temp_store();
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
        let store = temp_store();
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
        let store = temp_store();
        write(&store, b"rkey", b"rval").await;

        let tx = store.begin(false).await.expect("begin");
        let item = tx.get(b"rkey").await.expect("get failed");
        assert_eq!(item.value(), b"rval");
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_mutating() {
        let store = temp_store();
        let tx = store.begin(true).await.expect("begin");
        tx.set(Entry::new(b"mkey".to_vec(), b"mval".to_vec()))
            .expect("set failed");
        tx.commit().await.expect("commit failed");

        assert_eq!(read_value(&store, b"mkey").await.expect("read"), b"mval");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_discard() {
        let store = temp_store();
        let tx = store.begin(true).await.expect("begin");
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
        let store = temp_store();
        write(&store, b"key1", b"val1").await;
        assert_eq!(read_value(&store, b"key1").await.expect("get"), b"val1");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn tx_get_not_found() {
        let store = temp_store();
        let err = read_value(&store, b"nonexistent").await;
        assert_eq!(err.unwrap_err(), Error::KeyNotFound);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn tx_delete() {
        let store = temp_store();
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
        let store = temp_store();
        write(&store, b"ow", b"v1").await;
        write(&store, b"ow", b"v2").await;
        assert_eq!(read_value(&store, b"ow").await.expect("read"), b"v2");
        store.close().await.expect("close");
    }

    // --- Item ---

    #[tokio::test]
    async fn item_key() {
        let store = temp_store();
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

    #[tokio::test]
    async fn item_ttl() {
        let store = temp_store();
        write(&store, b"notls", b"val").await;
        store
            .read(|tx: &dyn Tx| {
                Box::pin(async move {
                    let item = tx.get(b"notls").await?;
                    assert_eq!(item.ttl(), Duration::ZERO);
                    assert_eq!(item.expires_at(), 0);
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_metadata_and_expiry() {
        let store = temp_store();
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
                    assert_ne!(item.expires_at(), 0, "expiry set by TTL(10s)");
                    assert!(item.ttl() > Duration::ZERO);
                    Ok(())
                })
            })
            .await
            .expect("get failed");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_expired_returns_key_not_found() {
        let store = temp_store();
        store
            .update(|tx: &dyn Tx| {
                Box::pin(async move {
                    let entry = Entry::new(b"short".to_vec(), b"val".to_vec())
                        .ttl(Duration::from_millis(10));
                    tx.set(entry)?;
                    Ok(())
                })
            })
            .await
            .expect("set failed");

        tokio::time::sleep(Duration::from_millis(30)).await;
        let err = read_value(&store, b"short").await;
        assert_eq!(err.unwrap_err(), Error::KeyNotFound);
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn item_value_copy() {
        let store = temp_store();
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
        let store = temp_store();
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
        let store = temp_store();
        write(&store, b"closeme", b"val").await;
        store.close().await.expect("close returned error");
    }

    // --- Destroy / DropPrefix ---

    #[tokio::test]
    async fn destroy() {
        let store = temp_store();
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
        let store = temp_store();
        write(&store, b"old", b"v").await;
        store.destroy().await.expect("destroy returned error");

        write(&store, b"new", b"v2").await;
        assert_eq!(read_value(&store, b"new").await.expect("read"), b"v2");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn drop_prefix_drops_matching_keys() {
        let store = temp_store();
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
        let store = temp_store();
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
        let store = temp_store();
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
        let store = temp_store();
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

        let tx = store.begin(false).await.expect("begin");
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
        let store = temp_store();
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store.begin(false).await.expect("begin");
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
        let store = temp_store();
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store.begin(false).await.expect("begin");
        let mut it = tx
            .new_range_iterator(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("iterator");
        let mut keys = Vec::new();
        while it.next().await {
            keys.push(it.item().expect("item after next").key().to_vec());
        }
        assert!(it.err().is_none());
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn new_range_iterator_exclusive_start_resumes_after_cursor() {
        let store = temp_store();
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        let tx = store.begin(false).await.expect("begin");
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
        let store = temp_store();
        for (k, v) in [("user:1", "a"), ("user:2", "b"), ("other", "c")] {
            write(&store, k.as_bytes(), v.as_bytes()).await;
        }

        // The end bound must be exclusive: only user:* keys are in range.
        let tx = store.begin(false).await.expect("begin");
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
        let store = temp_store();
        write(&store, b"a", b"1").await;
        write(&store, b"b", b"2").await;

        let tx = store.begin(false).await.expect("begin");
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
        let store = temp_store();
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
        let store = Arc::new(temp_store());
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
}
