//! Edge-case tests for WATCH / MULTI / EXEC.
//!
//! Every scenario is run against both backends (Fjall and in-memory SlateDB),
//! because the two implement isolation differently.
//!
//! "Expected" behaviour is upstream Redis 6.2: any write command that touches a
//! watched key (even one that leaves its bytes unchanged) aborts the
//! watcher's next EXEC with a nil reply.
//!
//! The deliberate exception is a plain command on a watched key: if it loses a
//! conflict, Invar replies nil instead of running it as Redis would.
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
    let args: Vec<Bytes> = args
        .iter()
        .map(|a| Bytes::copy_from_slice(a.as_bytes()))
        .collect();
    enqueue_command(s, &args).await
}

fn nil() -> Vec<RespValue> {
    vec![RespValue::Array(None)]
}

fn bulk(value: &'static str) -> Vec<RespValue> {
    vec![RespValue::BulkString(Some(Bytes::from_static(
        value.as_bytes(),
    )))]
}

fn is_committed(reply: &[RespValue]) -> bool {
    matches!(reply, [RespValue::Array(Some(_))])
}

fn sessions(store: Arc<dyn RedisStore>) -> (Session, Session) {
    let reg = Arc::new(WatchRegistry::new());
    (
        Session::new(store.clone(), reg.clone()),
        Session::new(store, reg),
    )
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
    let r = watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        &[&["set", "k", "2"], &["set", "k", "1"]],
        &["set", "k", "3"],
    )
    .await;
    assert_eq!(r, nil());
}

/// Overwrite with the identical value. Redis still aborts.
async fn same_value_write(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        &[&["set", "k", "1"]],
        &["set", "k", "3"],
    )
    .await;
    assert_eq!(r, nil());
}

/// Watched key absent -> created -> deleted. The closest WATCH gets to a phantom.
async fn existence_aba(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[],
        "k",
        &[&["set", "k", "x"], &["del", "k"]],
        &["set", "k", "3"],
    )
    .await;
    assert_eq!(r, nil());
}

/// HSET overwriting an existing field leaves the hash's field count alone.
async fn hash_field_overwrite(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["hset", "h", "f", "1"]],
        "h",
        &[&["hset", "h", "f", "2"]],
        &["hset", "h", "g", "1"],
    )
    .await;
    assert_eq!(r, nil());
}

/// ZADD changing an existing member's score.
async fn zset_score_update(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["zadd", "z", "1", "m"]],
        "z",
        &[&["zadd", "z", "2", "m"]],
        &["zadd", "z", "1", "n"],
    )
    .await;
    assert_eq!(r, nil());
}

/// LSET replaces an element in place; list length unchanged.
async fn list_lset(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["rpush", "l", "a"]],
        "l",
        &[&["lset", "l", "0", "b"]],
        &["rpush", "l", "c"],
    )
    .await;
    assert_eq!(r, nil());
}

/// SADD then SREM of a different member: same cardinality, different contents.
async fn set_swap_member(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["sadd", "s", "a"]],
        "s",
        &[&["sadd", "s", "b"], &["srem", "s", "a"]],
        &["sadd", "s", "c"],
    )
    .await;
    assert_eq!(r, nil());
}

/// TTL-only change. EXPIRE is a write in Redis and aborts watchers.
async fn expire_only(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        &[&["expire", "k", "100"]],
        &["set", "k", "3"],
    )
    .await;
    assert_eq!(r, nil());
}

/// RENAME another key (holding the same value) onto the watched key.
async fn rename_onto_same_value(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["set", "k", "1"], &["set", "j", "1"]],
        "k",
        &[&["rename", "j", "k"]],
        &["set", "k", "3"],
    )
    .await;
    assert_eq!(r, nil());
}

/// FLUSHDB removes the watched key.
async fn flushdb(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        &[&["flushdb"]],
        &["set", "k", "3"],
    )
    .await;
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
    let r = watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        &[&["select", "1"], &["set", "k", "2"]],
        &["set", "k", "3"],
    )
    .await;
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

/// A pop that leaves the sorted set in place still writes to it.
async fn blocking_pop_without_waiting(store: Arc<dyn RedisStore>) {
    let r = watch_then(
        store,
        &[&["zadd", "z", "1", "m", "2", "n"]],
        "z",
        &[&["bzpopmin", "z", "0"]],
        &["zadd", "z", "3", "o"],
    )
    .await;
    assert_eq!(r, nil());
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
    assert_eq!(
        run(&mut b, &["get", "k"]).await,
        vec![RespValue::BulkString(Some(Bytes::from_static(b"2")))]
    );
    assert_eq!(
        run(&mut b, &["get", "side"]).await,
        vec![RespValue::BulkString(None)]
    );
    assert_eq!(
        run(&mut b, &["llen", "q"]).await,
        vec![RespValue::Integer(0)]
    );
}

// ------------------------------------------------------------- scripts

const SET_K: &str = "return redis.call('set', 'k', '2')";
const SET_K_SHA: &str = "55487e49100c30560cfd7bb6a8f649472cbef086";

/// `watch_then` on an existing string `k`, with `SET k 3` queued.
async fn meddle_with_k(store: Arc<dyn RedisStore>, meddle: &[&[&str]]) -> Vec<RespValue> {
    watch_then(
        store,
        &[&["set", "k", "1"]],
        "k",
        meddle,
        &["set", "k", "3"],
    )
    .await
}

async fn script_write(store: Arc<dyn RedisStore>) {
    let script = "return redis.call('set', KEYS[1], '2')";
    let r = meddle_with_k(store, &[&["eval", script, "1", "k"]]).await;
    assert_eq!(r, nil());
}

/// The written key doesn't have to appear in `KEYS`.
async fn script_write_undeclared_key(store: Arc<dyn RedisStore>) {
    let r = meddle_with_k(store, &[&["eval", SET_K, "0"]]).await;
    assert_eq!(r, nil());
}

async fn evalsha_write(store: Arc<dyn RedisStore>) {
    let (mut a, mut b) = sessions(store);
    // Cache the script before the watch, so only EVALSHA runs after it.
    run(&mut b, &["eval", SET_K, "0"]).await;
    run(&mut a, &["watch", "k"]).await;
    run(&mut b, &["evalsha", SET_K_SHA, "0"]).await;
    queue(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

/// The watcher's own script invalidates its watch too.
async fn own_script_write(store: Arc<dyn RedisStore>) {
    let (mut a, _b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["watch", "k"]).await;
    run(&mut a, &["eval", SET_K, "0"]).await;
    queue(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

/// A script that fails inside `EXEC` keeps the writes it had already made, as
/// the rest of the batch still commits.
async fn failed_script_inside_exec(store: Arc<dyn RedisStore>) {
    let script = "redis.call('set', 'k', '2') return redis.call('nosuchcommand')";
    let r = meddle_with_k(store, &[&["multi"], &["eval", script, "0"], &["exec"]]).await;
    assert_eq!(r, nil());
}

async fn nested_script_write(store: Arc<dyn RedisStore>) {
    let script = "return redis.call('eval', ARGV[1], '0')";
    let r = meddle_with_k(store, &[&["eval", script, "0", SET_K]]).await;
    assert_eq!(r, nil());
}

async fn script_flushdb_then_recreate(store: Arc<dyn RedisStore>) {
    let script = "redis.call('flushdb') return redis.call('set', 'k', '1')";
    let r = meddle_with_k(store, &[&["eval", script, "0"]]).await;
    assert_eq!(r, nil());
}

/// Declaring a key in `KEYS` is not a write, whether the script only reads it
/// or writes something else.
async fn script_without_a_write_to_the_key_does_not_abort(store: Arc<dyn RedisStore>) {
    let r = meddle_with_k(
        store,
        &[
            &["eval", "return redis.call('get', KEYS[1])", "1", "k"],
            &["eval", "return redis.call('set', 'other', '2')", "1", "k"],
        ],
    )
    .await;
    assert!(is_committed(&r), "got {r:?}");
}

/// Outside `MULTI` a failed script's transaction is discarded, so nothing was
/// written.
async fn discarded_script_does_not_abort(store: Arc<dyn RedisStore>) {
    let script = "redis.call('set', 'k', '2') return redis.call('nosuchcommand')";
    let r = meddle_with_k(store, &[&["eval", script, "0"]]).await;
    assert!(is_committed(&r), "got {r:?}");
}

// ------------------------------------------------------- interleavings

/// Where a gated transaction is parked.
#[derive(Clone, Copy, PartialEq)]
enum Park {
    BeforeCommit,
    /// The writes are visible to other sessions, but `commit` hasn't returned.
    AfterCommit,
}

/// Parks one transaction at commit until released.
struct Gate {
    armed: AtomicBool,
    park: Park,
    /// Gate the next transaction of this kind; `None` gates any writable one.
    isolation: Option<TxIsolation>,
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
    async fn get(&self, key: &[u8]) -> Result<Item, KvError> {
        self.inner.get(key).await
    }
    fn set(&self, entry: Entry) -> Result<(), KvError> {
        self.inner.set(entry)
    }
    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.inner.delete(key)
    }
    async fn new_range_iterator(
        &self,
        start: std::ops::Bound<&[u8]>,
        end: std::ops::Bound<&[u8]>,
    ) -> Result<Box<dyn KeyValueIterator>, KvError> {
        self.inner.new_range_iterator(start, end).await
    }
    async fn new_prefix_iterator(
        &self,
        prefix: &[u8],
    ) -> Result<Box<dyn KeyValueIterator>, KvError> {
        self.inner.new_prefix_iterator(prefix).await
    }
    async fn commit(self: Box<Self>) -> Result<Option<WriteHandle>, KvError> {
        let PauseTx { inner, gate } = *self;
        match gate.park {
            Park::BeforeCommit => {
                gate.pause().await;
                inner.commit().await
            }
            Park::AfterCommit => {
                let result = inner.commit().await;
                gate.pause().await;
                result
            }
        }
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
        let wanted = self.gate.isolation.map_or(mutating, |i| i == isolation);
        if wanted && self.gate.armed.swap(false, Ordering::SeqCst) {
            Ok(Box::new(PauseTx {
                inner: tx,
                gate: self.gate.clone(),
            }))
        } else {
            Ok(tx)
        }
    }
    async fn close(&self) -> Result<(), KvError> {
        self.inner.close().await
    }
    async fn sync(&self) -> Result<(), KvError> {
        self.inner.sync().await
    }
    async fn destroy(&self) -> Result<(), KvError> {
        self.inner.destroy().await
    }
    async fn drop_prefix(&self, p: &[u8]) -> Result<(), KvError> {
        self.inner.drop_prefix(p).await
    }
    async fn await_until(&self, h: WriteHandle, d: Duration) -> Result<bool, KvError> {
        self.inner.await_until(h, d).await
    }
}

impl Gate {
    async fn pause(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }

    /// Resolves once a transaction is parked, failing rather than hanging if
    /// none ever is.
    async fn wait_reached(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.reached.notified())
            .await
            .expect("no transaction reached the gate");
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

fn gated(
    inner: Arc<dyn RedisStore>,
    park: Park,
    isolation: Option<TxIsolation>,
    armed: bool,
) -> (Arc<dyn RedisStore>, Arc<Gate>) {
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(armed),
        park,
        isolation,
        reached: Notify::new(),
        release: Notify::new(),
    });
    (
        Arc::new(PauseStore {
            inner,
            gate: gate.clone(),
        }),
        gate,
    )
}

/// Parks the first SSI transaction just before commit.
fn paused(inner: Arc<dyn RedisStore>) -> (Arc<dyn RedisStore>, Arc<Gate>) {
    gated(
        inner,
        Park::BeforeCommit,
        Some(TxIsolation::SerializableSnapshot),
        true,
    )
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

    assert_eq!(
        exec.await.unwrap(),
        nil(),
        "y changed while EXEC was in flight"
    );
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

// ------------------------------------------ commits that are still in flight

/// Queues `MULTI` / `cmd` and returns without running `EXEC`.
async fn queue(s: &mut Session, cmd: &[&str]) {
    run(s, &["multi"]).await;
    run(s, cmd).await;
}

/// Two clients race to claim a job. A's claim is visible in the store but its
/// commit hasn't returned when B runs EXEC. B must not also win.
async fn exec_during_a_watched_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::AfterCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "job", "pending"]).await;
    run(&mut a, &["watch", "job"]).await;
    run(&mut b, &["watch", "job"]).await;
    queue(&mut a, &["set", "job", "a"]).await;
    queue(&mut b, &["set", "job", "b"]).await;

    gate.arm();
    let exec_a = tokio::spawn(async move { run(&mut a, &["exec"]).await });
    gate.wait_reached().await;
    let rb = run(&mut b, &["exec"]).await;
    gate.release.notify_one();

    assert!(is_committed(&exec_a.await.unwrap()));
    assert_eq!(rb, nil(), "both clients claimed the job");
}

/// The same window, opened by a plain write from a client that watches nothing.
async fn exec_during_a_plain_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::AfterCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["watch", "k"]).await;
    queue(&mut a, &["set", "k", "3"]).await;

    gate.arm();
    let write = tokio::spawn(async move { run(&mut b, &["set", "k", "2"]).await });
    gate.wait_reached().await;
    let ra = run(&mut a, &["exec"]).await;
    gate.release.notify_one();
    write.await.unwrap();

    assert_eq!(ra, nil(), "EXEC overwrote a write made after WATCH");
}

/// `BZPOPMIN` commits its pop in a transaction of its own.
async fn exec_during_a_blocking_pop_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::AfterCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["zadd", "z", "1", "m", "2", "n"]).await;
    run(&mut a, &["watch", "z"]).await;
    queue(&mut a, &["zadd", "z", "3", "o"]).await;

    // BZPOPMIN's outer transaction is read-only, so this gates the pop's own.
    gate.arm();
    let pop = tokio::spawn(async move { run(&mut b, &["bzpopmin", "z", "0"]).await });
    gate.wait_reached().await;
    let ra = run(&mut a, &["exec"]).await;
    gate.release.notify_one();
    pop.await.unwrap();

    assert_eq!(ra, nil(), "EXEC ran on a set popped after WATCH");
}

/// Each client watches the key the other one writes, so neither queue touches
/// a watched key. Only one may commit.
async fn disjoint_write_skew_with_watch(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::BeforeCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "x", "1"]).await;
    run(&mut a, &["set", "y", "1"]).await;
    run(&mut a, &["watch", "x"]).await;
    run(&mut b, &["watch", "y"]).await;
    queue(&mut a, &["set", "y", "0"]).await;
    queue(&mut b, &["set", "x", "0"]).await;

    gate.arm();
    let exec_a = tokio::spawn(async move { run(&mut a, &["exec"]).await });
    gate.wait_reached().await;
    let rb = run(&mut b, &["exec"]).await;
    gate.release.notify_one();
    let ra = exec_a.await.unwrap();

    assert!(
        !(is_committed(&ra) && is_committed(&rb)),
        "write skew: both committed (a={ra:?}, b={rb:?})"
    );
}

/// WATCH taken while a write is in flight, before it is visible: the value the
/// client goes on to read is already stale once the write lands.
async fn watch_during_a_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::BeforeCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;

    gate.arm();
    let write = tokio::spawn(async move { run(&mut b, &["set", "k", "2"]).await });
    gate.wait_reached().await;
    run(&mut a, &["watch", "k"]).await;
    gate.release.notify_one();
    write.await.unwrap();
    queue(&mut a, &["set", "k", "3"]).await;

    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

/// A connection dropped in the middle of a commit must not leave its keys
/// unwatchable, and watchers must assume the write may have landed.
async fn abandoned_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::AfterCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["watch", "k"]).await;

    gate.arm();
    let write = tokio::spawn(async move { run(&mut b, &["set", "k", "2"]).await });
    gate.wait_reached().await;
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    queue(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());

    run(&mut a, &["watch", "k"]).await;
    queue(&mut a, &["set", "k", "4"]).await;
    assert!(is_committed(&run(&mut a, &["exec"]).await));
}

/// A script's write is visible in the store but its commit hasn't returned.
async fn exec_during_a_script_commit(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(store, Park::AfterCommit, None, false);
    let (mut a, mut b) = sessions(store);
    run(&mut a, &["set", "k", "1"]).await;
    run(&mut a, &["watch", "k"]).await;
    queue(&mut a, &["set", "k", "3"]).await;

    gate.arm();
    let write = tokio::spawn(async move { run(&mut b, &["eval", SET_K, "0"]).await });
    gate.wait_reached().await;
    let ra = run(&mut a, &["exec"]).await;
    gate.release.notify_one();
    write.await.unwrap();

    assert_eq!(ra, nil(), "EXEC overwrote a script write made after WATCH");
}

// ------------------------- plain commands on watched keys (unlike Redis)

/// A watches `k`, then runs `cmd` outside `MULTI`. B overwrites `written`
/// before `cmd` can commit. Returns `cmd`'s reply and both sessions.
async fn conflicting_plain_command(
    store: Arc<dyn RedisStore>,
    cmd: &'static [&'static str],
    written: &str,
) -> (Vec<RespValue>, Session, Session) {
    let (store, gate) = gated(
        store,
        Park::BeforeCommit,
        Some(TxIsolation::SerializableSnapshot),
        false,
    );
    let (mut a, mut b) = sessions(store);
    run(&mut b, &["set", "k", "1"]).await;
    run(&mut b, &["set", "other", "1"]).await;
    run(&mut a, &["watch", "k"]).await;

    gate.arm();
    let plain = tokio::spawn(async move { (run(&mut a, cmd).await, a) });
    gate.wait_reached().await;
    run(&mut b, &["set", written, "2"]).await;
    gate.release.notify_one();
    let (reply, a) = plain.await.unwrap();
    (reply, a, b)
}

async fn conflicting_plain_read_keeps_watch(store: Arc<dyn RedisStore>) {
    let (reply, mut a, mut b) = conflicting_plain_command(store, &["get", "k"], "k").await;
    assert_eq!(reply, nil());

    queue(&mut a, &["set", "k", "3"]).await;
    assert_eq!(
        run(&mut a, &["exec"]).await,
        nil(),
        "EXEC overwrote a write made after WATCH"
    );
    assert_eq!(run(&mut b, &["get", "k"]).await, bulk("2"));
}

/// The conflicting write is not applied either.
async fn conflicting_plain_write_keeps_watch(store: Arc<dyn RedisStore>) {
    let (reply, mut a, mut b) = conflicting_plain_command(store, &["incr", "k"], "k").await;
    assert_eq!(reply, nil());
    assert_eq!(run(&mut b, &["get", "k"]).await, bulk("2"));

    queue(&mut a, &["set", "k", "3"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

/// The conflict may be on a key nobody watches. The command still replies nil,
/// but the next EXEC has no reason to abort.
async fn conflict_on_unwatched_key_does_not_abort_exec(store: Arc<dyn RedisStore>) {
    let (reply, mut a, _b) =
        conflicting_plain_command(store, &["mget", "k", "other"], "other").await;
    assert_eq!(reply, nil());

    queue(&mut a, &["set", "k", "3"]).await;
    assert!(is_committed(&run(&mut a, &["exec"]).await));
}

/// `BZPOPMIN` commits its pop in a transaction of its own, so its conflict
/// surfaces while the command runs rather than at the dispatcher's commit.
async fn conflicting_blocking_pop_keeps_watch(store: Arc<dyn RedisStore>) {
    let (store, gate) = gated(
        store,
        Park::BeforeCommit,
        Some(TxIsolation::Snapshot),
        false,
    );
    let (mut a, mut b) = sessions(store);
    run(&mut b, &["zadd", "z", "1", "m", "2", "n"]).await;
    run(&mut a, &["watch", "z"]).await;

    gate.arm();
    let pop = tokio::spawn(async move { (run(&mut a, &["bzpopmin", "z", "0"]).await, a) });
    gate.wait_reached().await;
    run(&mut b, &["zadd", "z", "3", "m"]).await;
    gate.release.notify_one();
    let (reply, mut a) = pop.await.unwrap();
    assert_eq!(reply, nil());
    assert_eq!(
        run(&mut b, &["zcard", "z"]).await,
        vec![RespValue::Integer(2)]
    );

    queue(&mut a, &["zadd", "z", "4", "o"]).await;
    assert_eq!(run(&mut a, &["exec"]).await, nil());
}

both_backends!(
    conflicting_plain_read_keeps_watch,
    conflicting_plain_write_keeps_watch,
    conflict_on_unwatched_key_does_not_abort_exec,
    conflicting_blocking_pop_keeps_watch,
    script_write,
    script_write_undeclared_key,
    evalsha_write,
    own_script_write,
    failed_script_inside_exec,
    nested_script_write,
    script_flushdb_then_recreate,
    script_without_a_write_to_the_key_does_not_abort,
    discarded_script_does_not_abort,
    exec_during_a_script_commit,
    aba_string,
    same_value_write,
    existence_aba,
    hash_field_overwrite,
    zset_score_update,
    list_lset,
    set_swap_member,
    expire_only,
    rename_onto_same_value,
    blocking_pop_without_waiting,
    flushdb,
    expires_naturally,
    own_write_same_value,
    other_db_does_not_abort,
    exec_clears_watches,
    discard_clears_watches,
    aborted_exec_writes_nothing,
    commit_lands_mid_exec_on_read_only_watched_key,
    write_skew_with_watch,
    exec_during_a_watched_commit,
    exec_during_a_plain_commit,
    exec_during_a_blocking_pop_commit,
    disjoint_write_skew_with_watch,
    watch_during_a_commit,
    abandoned_commit,
);
