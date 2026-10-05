//! Edge-case tests for WATCH / MULTI / EXEC.
//!
//! Every scenario is run against both backends (Fjall and in-memory SlateDB),
//! because the two implement isolation differently.
//!
//! "Expected" behaviour is upstream Redis 6.2: any write command that touches a
//! watched key (even one that leaves its bytes unchanged) aborts the
//! watcher's next EXEC with a nil reply.
//!
//! Sequential tests use two `Session`s over one store to stand in for two
//! connections. Interleaving tests use `PauseStore`, which parks the first
//! SSI transaction just before commit so another session can sneak a commit
//! in, deterministically and without sleeps.

#![cfg(test)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use kv::kv::{Entry, Error as KvError, Item, KeyValueIterator, Tx, TxIsolation, WriteHandle};
use tokio::sync::Notify;

use crate::commands::enqueue_command;
use crate::common::{RedisStore, Session, WatchRegistry};
use crate::resp::RespValue;

// ---------------------------------------------------------------- helpers

async fn run(s: &mut Session, args: &[&str]) -> Vec<RespValue> {
    let args: Vec<Bytes> = args.iter().map(|a| Bytes::copy_from_slice(a.as_bytes())).collect();
    enqueue_command(s, &args).await
}

fn nil() -> Vec<RespValue> {
    vec![RespValue::Array(None)]
}

fn is_committed(reply: &[RespValue]) -> bool {
    matches!(reply, [RespValue::Array(Some(_))])
}

fn sessions(store: Arc<dyn RedisStore>) -> (Session, Session) {
    let reg = Arc::new(WatchRegistry::new());
    (Session::new(store.clone(), reg.clone()), Session::new(store, reg))
}

async fn slate() -> Arc<dyn RedisStore> {
    Arc::new(kv::slate::SlateDb::in_memory().await.expect("slate"))
}

/// Generates `<name>` tests under `slate_backend` from a scenario
/// `async fn(Arc<dyn RedisStore>)`.
macro_rules! both_backends {
    ($($name:ident),* $(,)?) => {
        mod slate_backend {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $name() { super::$name(super::slate().await).await }
            )*
        }
    };
}

/// Common shape: A sets up, A WATCHes `key`, `meddle` runs on B, then A runs
/// MULTI / `queued` / EXEC. Returns A's EXEC reply.
async fn watch_then(
    store: Arc<dyn RedisStore>,
    setup: &[&[&str]],
    key: &str,
    meddle: &[&[&str]],
    queued: &[&str],
) -> Vec<RespValue> {
    let (mut a, mut b) = sessions(store);
    for cmd in setup {
        run(&mut a, cmd).await;
    }
    run(&mut a, &["watch", key]).await;
    for cmd in meddle {
        run(&mut b, cmd).await;
    }
    run(&mut a, &["multi"]).await;
    run(&mut a, queued).await;
    run(&mut a, &["exec"]).await
}

// ------------------------------------------- staleness: must abort (Redis)

/// A -> B -> A on a string value.
async fn aba_string(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"]], "k",
                       &[&["set", "k", "2"], &["set", "k", "1"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// Overwrite with the identical value. Redis still aborts.
async fn same_value_write(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"]], "k",
                       &[&["set", "k", "1"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// Watched key absent -> created -> deleted. The closest WATCH gets to a phantom.
async fn existence_aba(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[], "k",
                       &[&["set", "k", "x"], &["del", "k"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// HSET overwriting an existing field leaves the hash's field count alone.
async fn hash_field_overwrite(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["hset", "h", "f", "1"]], "h",
                       &[&["hset", "h", "f", "2"]], &["hset", "h", "g", "1"]).await;
    assert_eq!(r, nil());
}

/// ZADD changing an existing member's score.
async fn zset_score_update(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["zadd", "z", "1", "m"]], "z",
                       &[&["zadd", "z", "2", "m"]], &["zadd", "z", "1", "n"]).await;
    assert_eq!(r, nil());
}

/// LSET replaces an element in place; list length unchanged.
async fn list_lset(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["rpush", "l", "a"]], "l",
                       &[&["lset", "l", "0", "b"]], &["rpush", "l", "c"]).await;
    assert_eq!(r, nil());
}

/// SADD then SREM of a different member: same cardinality, different contents.
async fn set_swap_member(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["sadd", "s", "a"]], "s",
                       &[&["sadd", "s", "b"], &["srem", "s", "a"]], &["sadd", "s", "c"]).await;
    assert_eq!(r, nil());
}

/// TTL-only change. EXPIRE is a write in Redis and aborts watchers.
async fn expire_only(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"]], "k",
                       &[&["expire", "k", "100"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// RENAME another key (holding the same value) onto the watched key.
async fn rename_onto_same_value(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"], &["set", "j", "1"]], "k",
                       &[&["rename", "j", "k"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// FLUSHDB removes the watched key.
async fn flushdb(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"]], "k",
                       &[&["flushdb"]], &["set", "k", "3"]).await;
    assert_eq!(r, nil());
}

/// Key expires on its own between WATCH and EXEC (Redis >= 6.0.9 aborts).
async fn expires_naturally(store: Arc<dyn RedisStore>) {
    let (mut a, _b) = sessions(store);
    run(&mut a, &["set", "k", "1", "px", "50"]).await;
    run(&mut a, &["watch", "k"]).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

/// The watcher's own write between WATCH and MULTI also invalidates (same value).
async fn own_write_same_value(store: Arc<dyn RedisStore>) {
    let (mut a, _b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["watch", "k"]).await;
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

// ------------------------------------------ controls: must commit (Redis)

/// Same key name in a different logical db is a different key.
async fn other_db_does_not_abort(store: Arc<dyn RedisStore>) {
    let r = watch_then(store, &[&["set", "k", "1"]], "k",
                       &[&["select", "1"], &["set", "k", "2"]], &["set", "k", "3"]).await;
    assert!(is_committed(&r), "got {r:?}");
}

/// EXEC (success or not) clears the watch set.
async fn exec_clears_watches(store: Arc<dyn RedisStore>) {
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["watch", "k"]).await;
    run(&mut b, &["set", "k", "2"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
    // Second transaction carries no stale watch.
    run(&mut b, &["set", "k", "4"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "5"]).await;
    assert!(is_committed(&run(&mut a, &["exec"]).await));
}

/// DISCARD clears the watch set.
async fn discard_clears_watches(store: Arc<dyn RedisStore>) {
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["watch", "k"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["discard"]).await;
    run(&mut b, &["set", "k", "2"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "3"]).await;
    assert!(is_committed(&run(&mut a, &["exec"]).await));
}

/// An aborted EXEC must leave none of its queued writes behind.
async fn aborted_exec_writes_nothing(store: Arc<dyn RedisStore>) {
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["watch", "k"]).await;
    run(&mut b, &["set", "k", "2"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "k", "3"]).await;
    run(&mut a, &["set", "side", "1"]).await;
    run(&mut a, &["rpush", "q", "job"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
    assert_eq!(run(&mut b, &["get", "k"]).await, vec![RespValue::BulkString(Some(Bytes::from_static(b"2")))]);
    assert_eq!(run(&mut b, &["get", "side"]).await, vec![RespValue::BulkString(None)]);
    assert_eq!(run(&mut b, &["llen", "q"]).await, vec![RespValue::Integer(0)]);
}

// ------------------------------------------------------- interleavings

/// Parks the first SSI transaction just before commit until released.
struct Gate {
    armed: AtomicBool,
    reached: Notify,
    release: Notify,
}

struct PauseStore {
    inner: Arc<dyn RedisStore>,
    gate: Arc<Gate>,
}

struct PauseTx {
    inner: Box<dyn Tx>,
    gate: Arc<Gate>,
}

#[async_trait]
impl Tx for PauseTx {
    async fn get(&self, key: &[u8]) -> Result<Item, KvError> { self.inner.get(key).await }
    fn set(&self, entry: Entry) -> Result<(), KvError> { self.inner.set(entry) }
    fn delete(&self, key: &[u8]) -> Result<(), KvError> { self.inner.delete(key) }
    async fn new_range_iterator(&self, start: std::ops::Bound<&[u8]>, end: std::ops::Bound<&[u8]>)
                                -> Result<Box<dyn KeyValueIterator>, KvError> { self.inner.new_range_iterator(start, end).await }
    async fn new_prefix_iterator(&self, prefix: &[u8]) -> Result<Box<dyn KeyValueIterator>, KvError> {
        self.inner.new_prefix_iterator(prefix).await
    }
    async fn commit(self: Box<Self>) -> Result<Option<WriteHandle>, KvError> {
        self.gate.reached.notify_one();
        self.gate.release.notified().await;
        self.inner.commit().await
    }
    fn discard(self: Box<Self>) {
        // An aborted attempt never reaches commit; don't leave the test hanging.
        self.gate.reached.notify_one();
        self.inner.discard()
    }
}

#[async_trait]
impl RedisStore for PauseStore {
    async fn begin(&self, mutating: bool, isolation: TxIsolation) -> Result<Box<dyn Tx>, KvError> {
        let tx = self.inner.begin(mutating, isolation).await?;
        if isolation == TxIsolation::SerializableSnapshot && self.gate.armed.swap(false, Ordering::SeqCst) {
            Ok(Box::new(PauseTx { inner: tx, gate: self.gate.clone() }))
        } else {
            Ok(tx)
        }
    }
    async fn close(&self) -> Result<(), KvError> { self.inner.close().await }
    async fn sync(&self) -> Result<(), KvError> { self.inner.sync().await }
    async fn destroy(&self) -> Result<(), KvError> { self.inner.destroy().await }
    async fn drop_prefix(&self, p: &[u8]) -> Result<(), KvError> { self.inner.drop_prefix(p).await }
    async fn await_until(&self, h: WriteHandle, d: Duration) -> Result<bool, KvError> { self.inner.await_until(h, d).await }
}

fn paused(inner: Arc<dyn RedisStore>) -> (Arc<dyn RedisStore>, Arc<Gate>) {
    let gate = Arc::new(Gate { armed: AtomicBool::new(true), reached: Notify::new(), release: Notify::new() });
    (Arc::new(PauseStore { inner, gate: gate.clone() }), gate)
}

/// A's EXEC passes the WATCH pre-check, then B commits a write to a watched
/// key that A only *read* (not wrote), before A commits. Only read-set
/// tracking (real SSI) can catch this.
async fn commit_lands_mid_exec_on_read_only_watched_key(store: Arc<dyn RedisStore>) {
    let (store, gate) = paused(store);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "x", "1"]).await;
    run(&mut a, &["set", "y", "1"]).await;
    run(&mut a, &["watch", "x", "y"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "x", "2"]).await;
    let exec = tokio::spawn(async move { run(&mut a, &["exec"]).await });

    gate.reached.notified().await;
    run(&mut b, &["set", "y", "99"]).await;
    gate.release.notify_one();

    assert_eq!(exec.await.unwrap(), nil(), "y changed while EXEC was in flight");
}

/// The on-call write-skew example from the blog post, both clients guarded
/// with WATCH on both keys. At most one may commit.
async fn write_skew_with_watch(store: Arc<dyn RedisStore>) {
    let (store, gate) = paused(store);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "oncall:alice", "1"]).await;
    run(&mut a, &["set", "oncall:bob", "1"]).await;

    run(&mut a, &["watch", "oncall:alice", "oncall:bob"]).await;
    run(&mut b, &["watch", "oncall:alice", "oncall:bob"]).await;
    run(&mut a, &["multi"]).await;
    run(&mut a, &["set", "oncall:alice", "0"]).await;
    run(&mut b, &["multi"]).await;
    run(&mut b, &["set", "oncall:bob", "0"]).await;

    let exec_a = tokio::spawn(async move { run(&mut a, &["exec"]).await });
    gate.reached.notified().await; // A is parked just before commit
    let rb = run(&mut b, &["exec"]).await;
    gate.release.notify_one();
    let ra = exec_a.await.unwrap();

    assert!(
        !(is_committed(&ra) && is_committed(&rb)),
        "write skew: both committed (a={ra:?}, b={rb:?})"
    );
}

both_backends!(
    aba_string,
    same_value_write,
    existence_aba,
    hash_field_overwrite,
    zset_score_update,
    list_lset,
    set_swap_member,
    expire_only,
    rename_onto_same_value,
    flushdb,
    expires_naturally,
    own_write_same_value,
    other_db_does_not_abort,
    exec_clears_watches,
    discard_clears_watches,
    aborted_exec_writes_nothing,
    commit_lands_mid_exec_on_read_only_watched_key,
    write_skew_with_watch,
);