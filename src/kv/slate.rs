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

use std::collections::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::config::{PutOptions, Settings, Ttl};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::aws;
use slatedb::{Db, DbIterator, DbTransaction, Error as SlateError, ErrorKind, IsolationLevel};

use crate::kv::{BoxFuture, Entry, Error, Item, KeyValueIterator, KeyValueStore, Tx, WriteHandle};

/// Maps a SlateDB error to the kv abstraction's error space, mirroring the
/// Go backend: transaction conflicts surface as [`Error::Conflict`],
/// everything else as [`Error::Undefined`].
fn map_slate_error(err: SlateError) -> Error {
    match err.kind() {
        ErrorKind::Transaction => Error::Conflict,
        _ => Error::Undefined,
    }
}

/// Options for opening a SlateDB-backed [`KeyValueStore`].
#[derive(Debug, Clone, Default)]
pub struct SlateDbOpts {
    pub path: String,
    pub bucket_name: String,
    /// Optional; applied to the DbBuilder when `Some`.
    pub settings: Option<Settings>,
}

/// SlateDB-backed [`KeyValueStore`].
#[derive(Clone)]
pub struct SlateDb {
    db: Db,
}

impl SlateDb {
    pub async fn open(opts: SlateDbOpts) -> Result<SlateDb, Error> {
        let store = Arc::new(aws::AmazonS3Builder::from_env()
            .with_bucket_name(opts.bucket_name)
            .build().expect("couldn't create s3 client"));
        Self::build(opts.path, store, opts.settings).await
    }

    /// Open a store backed by an in-memory object store (for tests).
    pub async fn in_memory() -> Result<SlateDb, Error> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::build("test-db".to_string(), store, None).await
    }

    async fn build(
        path: String,
        store: Arc<dyn ObjectStore>,
        settings: Option<Settings>,
    ) -> Result<SlateDb, Error> {
        let mut builder = Db::builder(path, store.clone());
        if let Some(settings) = settings {
            builder = builder.with_settings(settings);
        }
        let db = builder.build().await.map_err(map_slate_error)?;
        Ok(SlateDb { db })
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
            .await
            .map_err(map_slate_error)?;
        for key in keys {
            tx.delete(key).map_err(map_slate_error)?;
        }
        let _ = tx.commit().await.map_err(map_slate_error)?;
        Ok(())
    }
}

#[async_trait]
impl KeyValueStore for SlateDb {
    fn new_entry(&self, key: Vec<u8>, value: Vec<u8>) -> Entry {
        Entry::new(key, value)
    }

    async fn begin(&self, _mutating: bool) -> Result<Box<dyn Tx>, Error> {
        let tx = self
            .db
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(map_slate_error)?;
        Ok(Box::new(SlateTx { tx }))
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
        self.db.close().await.map_err(map_slate_error)
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
}

struct SlateTx {
    tx: DbTransaction,
}

#[async_trait]
impl Tx for SlateTx {
    async fn get(&self, key: &[u8]) -> Result<Item, Error> {
        let kv = self.tx.get_key_value(key).await.map_err(map_slate_error)?;
        match kv {
            Some(kv) => {
                let (value, meta) = decode_value(&kv.value);
                Ok(Item::new(
                    kv.key.to_vec(),
                    value.to_vec(),
                    meta,
                    kv.expire_ts,
                ))
            }
            None => Err(Error::KeyNotFound),
        }
    }

    fn set(&self, entry: Entry) -> Result<(), Error> {
        let stored = encode_value(entry.metadata_byte(), entry.value());
        if !entry.ttl_value().is_zero() {
            let ms = entry.ttl_value().as_millis() as u64;
            self.tx
                .put_with_options(
                    entry.key(),
                    stored,
                    &PutOptions {
                        ttl: Ttl::ExpireAfter(ms),
                    },
                )
                .map_err(map_slate_error)
        } else {
            self.tx.put(entry.key(), stored).map_err(map_slate_error)
        }
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.tx.delete(key).map_err(map_slate_error)
    }

    async fn new_range_iterator(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Box<dyn KeyValueIterator>, Error> {
        let range = (start, end);
        let iter = self
            .tx
            .scan(range)
            .await
            .map_err(map_slate_error)?;
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

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        let _ = self.tx.commit().await.map_err(map_slate_error)?;
        Ok(())
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
        SlateDb::in_memory()
            .await
            .expect("open in-memory store")
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

        let tx = store.begin(false).await.expect("begin");
        let item = tx.get(b"rkey").await.expect("get failed");
        assert_eq!(item.value(), b"rval");
        tx.discard();
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_mutating() {
        let store = in_memory().await;
        let tx = store.begin(true).await.expect("begin");
        tx.set(Entry::new(b"mkey".to_vec(), b"mval".to_vec()))
            .expect("set failed");
        tx.commit().await.expect("commit failed");

        assert_eq!(read_value(&store, b"mkey").await.expect("read"), b"mval");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn begin_discard() {
        let store = in_memory().await;
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
        let store = in_memory().await;
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
        let store = in_memory().await;
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
        let store = in_memory().await;
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
        let store = in_memory().await;
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
}
