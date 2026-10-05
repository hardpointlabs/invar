//! Per-connection ephemeral state, mirroring the Go `redis/common` `Session`.
//!
//! In Go the `Session` lives on the per-connection `context` and is guaranteed
//! never to be mutated by more than one thread. Tokio makes no thread-pinning
//! promise, but the guarantee that matters still holds: a session is owned by
//! exactly one connection task at a time, so it needs no locks for its own
//! fields. The only process-shared state is the [`WatchRegistry`], which
//! guards itself.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use kv::kv::{Entry, Error as KvError, TxIsolation, WriteHandle};
use smallvec::{smallvec, SmallVec};

use crate::common::op::{DbError, DbResult, NoOp, QueuedOp, WireOp};
use crate::common::registry::WatchRegistry;
use crate::common::store::RedisStore;
use crate::pubsub::PubSubRegistry;
use crate::resp::RespValue;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Prefix marking internal (non-user-accessible) keys.
const INTERNAL_PREFIX: &[u8] = b"-";

/// Errors produced by session-level bookkeeping.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("not inside a MULTI block")]
    NotInMulti,
}

/// Client-scoped state for a single Redis connection: the current DB, the
/// `MULTI` queue and its abort flag, plus the key-derivation helpers command
/// implementations use to build storage keys without knowing the internal key
/// layout.
pub struct Session {
    id: u64,
    /// Current Redis 'db' (keyspace)
    current_db: i32,
    /// Most recent write operation performed in this session
    latest_write: Option<WriteHandle>,
    /// Queued ops; empty when not inside a `MULTI` block.
    queue: Vec<QueuedOp>,
    /// True while inside a `MULTI` block.
    in_multi: bool,
    /// Set when a command failed while queuing, aborting `EXEC`.
    dirty_exec: bool,
    /// True while a Lua script is executing via `redis.call`.
    in_script: bool,
    /// Set by `QUIT`: the listener should close the connection after the
    /// replies are flushed.
    should_close: bool,
    /// Keys registered via `WATCH` for this session. Each entry holds:
    /// - the write version at WATCH time (from the shared registry); any
    ///   increase by EXEC time means the key was written after WATCH — including
    ///   ABA writes — and the transaction is aborted with a nil reply.
    /// - whether the key existed in the store at WATCH time; a change (key
    ///   expired naturally or was created/deleted) also aborts the transaction.
    watched_keys: HashMap<String, (u64, bool)>,
    /// The flush epoch recorded at the time of the most recent `WATCH` call.
    /// If the epoch advances before `EXEC`, one of the watched keys may have
    /// been destroyed by a `FLUSHDB`/`FLUSHALL`, so the transaction aborts.
    watch_flush_epoch: u64,
    /// The client-reported connection name, set via `CLIENT SETNAME` or
    /// `HELLO SETNAME`.
    client_name: String,
    /// The client library name, set via `CLIENT SETINFO LIB-NAME`.
    lib_name: String,
    /// The client library version, set via `CLIENT SETINFO LIB-VER`.
    lib_ver: String,
    /// The peer address of the socket, reported by `CLIENT INFO`/`CLIENT
    /// LIST`. `None` when the session was built without a socket (tests).
    peer_addr: Option<std::net::SocketAddr>,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    pubsub: Arc<PubSubRegistry>,
}

fn is_retryable_conflict(outcome: &Result<DbResult, DbError>) -> bool {
    matches!(outcome, Err(DbError::Kv(KvError::Conflict)))
}

impl Session {
    /// Creates a session with a new pub/sub registry. Only useful for testing.
    pub fn new(store: Arc<dyn RedisStore>, registry: Arc<WatchRegistry>) -> Self {
        Self::new_with_pubsub(store, registry, Arc::new(PubSubRegistry::new()))
    }

    /// Creates a session with an explicit, shared pub/sub registry. Used by
    /// the listener so all connections on the same server share one registry.
    pub fn new_with_pubsub(
        store: Arc<dyn RedisStore>,
        registry: Arc<WatchRegistry>,
        pubsub: Arc<PubSubRegistry>,
    ) -> Self {
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            current_db: 0,
            latest_write: None,
            queue: Vec::new(),
            in_multi: false,
            dirty_exec: false,
            in_script: false,
            should_close: false,
            watched_keys: HashMap::new(),
            watch_flush_epoch: 0,
            client_name: String::new(),
            lib_name: String::new(),
            lib_ver: String::new(),
            peer_addr: None,
            store,
            registry,
            pubsub,
        }
    }

    /// The connection ID, unique process-wide.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The current Redis DB (namespace) number for this connection.
    pub fn current_db(&self) -> i32 {
        self.current_db
    }

    /// Most recent write operation performed in this session, if it exists.
    pub fn latest_write(&self) -> Option<WriteHandle> {
        self.latest_write.clone()
    }

    /// Switches this connection to another Redis DB.
    pub fn switch_db(&mut self, db: i32) {
        self.current_db = db;
    }

    /// The client-reported connection name (`CLIENT SETNAME`).
    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    /// Sets the client-reported connection name.
    pub fn set_client_name(&mut self, name: String) {
        self.client_name = name;
    }

    /// The client library name (`CLIENT SETINFO LIB-NAME`).
    pub fn lib_name(&self) -> &str {
        &self.lib_name
    }

    /// Sets the client library name.
    pub fn set_lib_name(&mut self, name: String) {
        self.lib_name = name;
    }

    /// The client library version (`CLIENT SETINFO LIB-VER`).
    pub fn lib_ver(&self) -> &str {
        &self.lib_ver
    }

    /// Sets the client library version.
    pub fn set_lib_ver(&mut self, version: String) {
        self.lib_ver = version;
    }

    /// The peer address of the connection socket, if known.
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.peer_addr
    }

    /// Records the peer address of the connection socket.
    pub fn set_peer_addr(&mut self, addr: std::net::SocketAddr) {
        self.peer_addr = Some(addr);
    }

    /// Enters a `MULTI` block.
    pub fn enter_multi(&mut self) {
        self.in_multi = true;
        self.dirty_exec = false;
    }

    /// Reports whether the connection is inside a `MULTI` block.
    pub fn in_multi(&self) -> bool {
        self.in_multi
    }

    /// Leaves a `MULTI` block. With `discard` set, the queue is dropped and
    /// the abort flag cleared; otherwise the queue is kept for `EXEC`.
    pub fn exit_multi(&mut self, discard: bool) -> Result<(), SessionError> {
        if !self.in_multi {
            return Err(SessionError::NotInMulti);
        }
        self.in_multi = false;
        if discard {
            self.queue.clear();
            self.dirty_exec = false;
            self.watched_keys.clear();
        }
        Ok(())
    }

    /// Flags the current `MULTI` transaction for abort because a command
    /// failed while it was being queued. Only takes effect while inside a
    /// `MULTI` block, so runtime errors during `EXEC` (or outside a
    /// transaction) are ignored.
    pub fn mark_dirty(&mut self) {
        if self.in_multi {
            self.dirty_exec = true;
        }
    }

    /// Reports whether the current `MULTI` transaction is flagged for abort.
    pub fn is_dirty(&self) -> bool {
        self.dirty_exec
    }

    /// Marks the session as executing a Lua script. Blocking commands must
    /// degrade to non-blocking pops while this is set.
    pub fn enter_script(&mut self) {
        self.in_script = true;
    }

    /// Clears the Lua-script execution flag.
    pub fn exit_script(&mut self) {
        self.in_script = false;
    }

    /// Reports whether the current session context allows a blocking command
    /// to actually block. Returns `false` whenever the caller must degrade to
    /// an immediate, non-blocking pop — inside a `MULTI`/`EXEC` transaction or
    /// a Lua script, where stalling the connection is forbidden by the Redis
    /// specification.
    pub fn should_block(&self) -> bool {
        !self.in_multi && !self.in_script
    }

    /// Requests the connection to be closed after the current reply batch is
    /// flushed (used by `QUIT`).
    pub fn request_close(&mut self) {
        self.should_close = true;
    }

    /// Reports whether the listener should close the connection (set by
    /// `QUIT`).
    pub fn should_close(&self) -> bool {
        self.should_close
    }

    /// Records the current write version and existence of each raw user key in
    /// the session-local WATCH set. Called by the `WATCH` command handler.
    pub async fn snapshot_and_watch(&mut self, raw_keys: &[Bytes]) {
        self.watch_flush_epoch = self.registry.flush_epoch();
        let tx = self.store.begin(false, TxIsolation::Snapshot).await;
        for k in raw_keys {
            let sk = String::from_utf8_lossy(&self.public_key(k)).into_owned();
            let version = self.registry.get_version(&sk);
            let existed = match &tx {
                Ok(tx) => tx.get(sk.as_bytes()).await.is_ok(),
                Err(_) => false,
            };
            self.watched_keys.insert(sk, (version, existed));
        }
    }

    /// Clears the session-local WATCH set.
    pub fn unwatch(&mut self) {
        self.watched_keys.clear();
    }

    /// Reports whether any of the queued ops' keys intersect with the current
    /// WATCH set. When true, the enclosing transaction must use
    /// `SerializableSnapshot` isolation.
    fn needs_ssi(&self) -> bool {
        if self.watched_keys.is_empty() {
            return false;
        }
        self.queue.iter().any(|op| {
            op.keys.as_ref().is_some_and(|ks| {
                ks.iter().any(|k| self.watched_keys.contains_key(k))
            })
        })
    }

    /// Returns a single-element SmallVec holding the storage key for `key`.
    /// Used by QueuedOp constructors to populate the `keys` field.
    pub fn key_sv(&self, key: &[u8]) -> SmallVec<[String; 2]> {
        smallvec![String::from_utf8_lossy(&self.public_key(key)).into_owned()]
    }

    /// Returns a SmallVec of storage keys for a slice of user-provided keys.
    /// Used by multi-key QueuedOp constructors to populate the `keys` field.
    pub fn keys_sv(&self, keys: &[Bytes]) -> SmallVec<[String; 2]> {
        keys.iter()
            .map(|k| String::from_utf8_lossy(&self.public_key(k)).into_owned())
            .collect()
    }

    /// The shared key-value store for this connection.
    pub fn store(&self) -> Arc<dyn RedisStore> {
        self.store.clone()
    }

    /// The process-wide watch registry.
    pub fn registry(&self) -> Arc<WatchRegistry> {
        self.registry.clone()
    }

    /// The process-wide pub/sub registry.
    pub fn pubsub(&self) -> Arc<PubSubRegistry> {
        self.pubsub.clone()
    }

    /// The raw prefix for public keys stored in the current Redis DB.
    pub fn prefix(&self) -> Vec<u8> {
        let mut prefix = self.current_db.to_string().into_bytes();
        prefix.push(b':');
        prefix
    }

    /// The raw prefix for internal (private) keys in the current Redis DB.
    pub fn private_prefix(&self) -> Vec<u8> {
        let mut prefix = INTERNAL_PREFIX.to_vec();
        prefix.extend_from_slice(&self.prefix());
        prefix
    }

    /// Derives the full storage key for a public key in the current DB.
    pub fn public_key(&self, key: &[u8]) -> Vec<u8> {
        let mut derived = self.prefix();
        derived.extend_from_slice(key);
        derived
    }

    /// Derives the storage key for a public key in a specific DB.
    pub fn public_key_for_db(&self, db: i32, key: &[u8]) -> Vec<u8> {
        let mut derived = db.to_string().into_bytes();
        derived.push(b':');
        derived.extend_from_slice(key);
        derived
    }

    /// Derives a private (internal) storage key in the current DB.
    pub fn private_key(&self, key: &[u8]) -> Vec<u8> {
        let mut derived =
            Vec::with_capacity(INTERNAL_PREFIX.len() + self.prefix().len() + key.len());
        derived.extend_from_slice(INTERNAL_PREFIX);
        derived.extend_from_slice(&self.prefix());
        derived.extend_from_slice(key);
        derived
    }

    /// Creates a public entry in the current DB.
    pub fn new_public_entry(&self, key: &[u8], value: &[u8]) -> Entry {
        Entry::new(self.public_key(key), value.to_vec())
    }

    /// Creates a public entry in a specific DB (used by `MOVE`, which writes
    /// the target DB without switching the session).
    pub fn new_entry_for_db(&self, db: i32, key: &[u8], value: &[u8]) -> Entry {
        Entry::new(self.public_key_for_db(db, key), value.to_vec())
    }

    /// Creates a private (internal) entry in the current DB.
    pub fn new_private_entry(&self, key: &[u8], value: &[u8]) -> Entry {
        Entry::new(self.private_key(key), value.to_vec())
    }

    /// Enqueues an op for later execution within a database transaction.
    ///
    /// Returns `Some(+QUEUED)` when the client is inside a `MULTI` block, in
    /// which case execution is deferred to `EXEC`.
    pub fn enqueue_op(&mut self, op: QueuedOp) -> Option<RespValue> {
        self.queue.push(op);
        if self.in_multi {
            Some(RespValue::SimpleString(Bytes::from_static(b"QUEUED")))
        } else {
            None
        }
    }

    /// Enqueues a wire-only op (one with no database effect).
    /// TODO is this smart? can't we just apply some sugar to QueuedOp creation and run wire-only
    /// ops through `enqueue_op()`?
    pub fn enqueue_wire_op(&mut self, wire_op: Box<dyn WireOp>) -> Option<RespValue> {
        self.enqueue_op(QueuedOp {
            db_op: Box::new(NoOp),
            wire_op,
            is_mutating: false,
            allowed_in_tx: true,
        abort_in_tx: false,
        keys: None,
        })
    }

    /// Attempts to acquire a database transaction and execute the pending
    /// operations.
    ///
    /// When `batch` is true (executing an `EXEC`) the results are wrapped in a
    /// single RESP array and a runtime error in one command does not roll back
    /// its siblings — matching Redis semantics.
    ///
    /// Returns the RESP replies to send to the client.
    pub async fn dispatch_pending_ops(&mut self, batch: bool) -> Vec<RespValue> {
        // Scenario A: we're inside a MULTI block — do nothing.
        if self.in_multi {
            return Vec::new();
        }

        if batch && self.dirty_exec {
            // A command failed while queuing, which aborts the whole txn.
            self.queue.clear();
            self.dirty_exec = false;
            self.watched_keys.clear();
            return vec![RespValue::Error(Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors.",
            ))];
        }

        if self.queue.is_empty() {
            if batch {
                return vec![RespValue::Array(Some(Vec::new()))];
            }
            return Vec::new();
        }

        let mutating = self.needs_writable_tx();
        let isolation = if self.needs_ssi() {
            TxIsolation::SerializableSnapshot
        } else {
            TxIsolation::Snapshot
        };
        // Concurrent writer transactions (BullMQ workers claim jobs with
        // overlapping write sets) make the optimistic store return
        // `Error::Conflict` — addStandardJob and friends must not fail.
        // Re-run the same batch in a fresh transaction until the commit lands.
        const MAX_CONFLICT_RETRIES: usize = 16;

        let mut outcomes: Vec<Result<DbResult, DbError>>;

        let mut conflict_retries = 0usize;
        'commit: loop {
            // Try to acquire a tx
            let tx = match self.store.begin(mutating, isolation).await {
                Ok(tx) => tx,
                Err(e) => {
                    self.queue.clear();
                    return vec![RespValue::Error(format!("ERR {e}").into())];
                }
            };

            outcomes = Vec::with_capacity(self.queue.len());

            // WATCH pre-check. Abort if any of:
            // 1. The flush epoch advanced (FLUSHDB/FLUSHALL ran since WATCH).
            // 2. A watched key's write version increased — catches any committed
            //    write, including same-value and ABA (write then restore).
            // 3. A watched key's existence changed — catches natural TTL expiry,
            //    which leaves the version counter unchanged.
            // Reading each key inside the transaction also registers it in the
            // SSI readset, so a concurrent write during EXEC is caught at commit.
            if batch && !self.watched_keys.is_empty() {
                if self.registry.flush_epoch() != self.watch_flush_epoch {
                    self.queue.clear();
                    self.watched_keys.clear();
                    return vec![RespValue::Array(None)];
                }
                for (key, (watched_version, existed_at_watch)) in &self.watched_keys {
                    if self.registry.get_version(key) != *watched_version {
                        self.queue.clear();
                        self.watched_keys.clear();
                        return vec![RespValue::Array(None)];
                    }
                    let exists_now = match tx.get(key.as_bytes()).await {
                        Ok(_) => true,
                        Err(KvError::KeyNotFound) => false,
                        Err(_) => {
                            self.queue.clear();
                            self.watched_keys.clear();
                            return vec![RespValue::Array(None)];
                        }
                    };
                    if exists_now != *existed_at_watch {
                        self.queue.clear();
                        self.watched_keys.clear();
                        return vec![RespValue::Array(None)];
                    }
                }
            }

            // Apply all our commands to the acquired tx
            for i in 0..self.queue.len() {
                let outcome = self.queue[i].db_op.run(&*tx).await;

                if is_retryable_conflict(&outcome) {
                    release_claims(&self.queue, &outcomes);
                    // Under SSI (WATCH-guarded transaction): a conflict means a
                    // watched key changed — return nil to the client, never retry.
                    if isolation == TxIsolation::SerializableSnapshot {
                        self.queue.clear();
                        self.watched_keys.clear();
                        return vec![RespValue::Array(None)];
                    }
                    // Not a command-level error -- the whole snapshot this attempt was
                    // built on is stale. Abandon this attempt (batch or not) and retry
                    // the entire queue from a fresh transaction, same as a commit-time
                    // conflict.
                    conflict_retries += 1;
                    if conflict_retries >= MAX_CONFLICT_RETRIES {
                        metrics::counter!("invar_conflict_failures").increment(1);
                        self.queue.clear();
                        return vec![RespValue::Error(Bytes::from_static(
                            b"ERR Couldn't commit transaction",
                        ))];
                    }
                    metrics::counter!("invar_conflict_retries").increment(1);
                    tokio::time::sleep(Duration::from_millis(conflict_retries as u64)).await;
                    continue 'commit;
                }

                if outcome.is_err() && !batch {
                    // Any claims made by earlier ops in this batch must be
                    // returned to the front of their queues since the
                    // transaction will be discarded.
                    for (j, result) in outcomes.iter().enumerate() {
                        if let Ok(r) = result {
                            self.queue[j].db_op.release_claims(r);
                        }
                    }
                    let reply = self.queue[i].wire_op.reply(outcome);
                    self.queue.clear();
                    return vec![reply];
                }
                // Inside EXEC a runtime error is confined to its own array
                // element: sibling commands still run and the transaction
                // still commits.
                outcomes.push(outcome);
            }

            // Commit the tx
            match tx.commit().await {
                Ok(handle) => {
                    tracing::debug!(ops = outcomes.len(), "tx committed");
                    self.latest_write = handle;
                    // Bump write versions for every key touched by a mutating op
                    // so that concurrent WATCH sessions detect the change at EXEC.
                    let changed_keys: Vec<String> = self.queue.iter()
                        .filter(|op| op.is_mutating)
                        .flat_map(|op| op.keys.iter().flatten().cloned())
                        .collect();
                    if !changed_keys.is_empty() {
                        self.registry.bump_versions(&changed_keys);
                    }
                    break 'commit;
                }
                Err(KvError::Conflict) if isolation == TxIsolation::SerializableSnapshot => {
                    // Under SSI (WATCH-guarded transaction): a commit conflict means
                    // a watched key was concurrently modified — EXEC returns nil.
                    release_claims(&self.queue, &outcomes);
                    self.queue.clear();
                    self.watched_keys.clear();
                    return vec![RespValue::Array(None)];
                }
                Err(e) => match e {
                    KvError::Conflict if conflict_retries < MAX_CONFLICT_RETRIES => {
                        conflict_retries += 1;
                        let backoff = Duration::from_millis(5) * 2u32.pow(conflict_retries.min(6) as u32)
                            + Duration::from_millis(rand::random::<u64>() % 10);
                        tracing::debug!(ops = outcomes.len(), retries = conflict_retries, "write tx conflict; retrying batch");
                        metrics::counter!("invar_conflict_retries").increment(1);
                        tokio::time::sleep(backoff).await;
                    }
                    KvError::Conflict => {
                        release_claims(&self.queue, &outcomes);
                        self.queue.clear();
                        metrics::counter!("invar_conflict_failures").increment(1);
                        return vec![RespValue::Error(Bytes::from_static(
                            b"ERR Couldn't commit transaction",
                        ))];
                    }
                    e => {
                        release_claims(&self.queue, &outcomes);
                        self.queue.clear();
                        return vec![RespValue::Error(format!("ERR {e}").into())];
                    }
                },
            }
            // The commit conflicted with a concurrent writer: this attempt's
            // claims must be returned before we re-run from a fresh snapshot.
            release_claims(&self.queue, &outcomes);
        }

        let mut replies = Vec::with_capacity(self.queue.len());
        for (i, outcome) in outcomes.into_iter().enumerate() {
            replies.push(self.queue[i].wire_op.reply(outcome));
        }
        let replies = if batch {
            vec![RespValue::Array(Some(replies))]
        } else {
            replies
        };

        self.queue.clear();
        if batch {
            // Redis clears the WATCH set after every EXEC (or DISCARD), whether
            // the transaction committed or not.
            self.watched_keys.clear();
        }
        replies
    }

    /// Whether any queued op requires a writable transaction.
    fn needs_writable_tx(&self) -> bool {
        self.queue.iter().any(|op| op.is_mutating)
    }
}

/// Returns any claims made by successful ops back to the front of their
/// queues, e.g. when a transaction is discarded or fails to commit.
fn release_claims(queue: &[QueuedOp], outcomes: &[Result<DbResult, DbError>]) {
    for (j, result) in outcomes.iter().enumerate() {
        if let Ok(r) = result {
            queue[j].db_op.release_claims(r);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::op::{DbError, DbResult, WireOp};
    use crate::strings;
    use crate::testutil::test_session;

    /// A wire-only op that replies `+OK`.
    struct TestWireOp;

    impl WireOp for TestWireOp {
        fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        }
    }

    #[test]
    fn key_derivation_uses_current_db() {
        let mut session = test_session();
        assert_eq!(session.prefix(), b"0:");
        assert_eq!(session.public_key(b"foo"), b"0:foo");
        assert_eq!(session.private_key(b"foo"), b"-0:foo");
        assert_eq!(session.public_key_for_db(2, b"foo"), b"2:foo");

        session.switch_db(3);
        assert_eq!(session.prefix(), b"3:");
        assert_eq!(session.public_key(b"foo"), b"3:foo");

        let entry = session.new_public_entry(b"foo", b"bar");
        assert_eq!(entry.key(), b"3:foo");
        assert_eq!(entry.value(), b"bar");
    }

    #[test]
    fn multi_lifecycle() {
        let mut session = test_session();
        assert!(!session.in_multi());
        assert!(matches!(
            session.exit_multi(true),
            Err(SessionError::NotInMulti)
        ));

        session.enter_multi();
        assert!(session.in_multi());

        let queued = session.enqueue_wire_op(Box::new(TestWireOp));
        assert_eq!(
            queued,
            Some(RespValue::SimpleString(Bytes::from_static(b"QUEUED")))
        );

        session.exit_multi(false).unwrap();
        assert!(!session.in_multi());
        assert_eq!(session.queue.len(), 1);

        session.enter_multi();
        session.exit_multi(true).unwrap();
        assert!(session.queue.is_empty());
    }

    #[test]
    fn should_block_is_false_in_multi_and_scripts() {
        let mut session = test_session();
        assert!(session.should_block());
        session.enter_multi();
        assert!(!session.should_block());
        session.exit_multi(true).unwrap();

        session.enter_script();
        assert!(!session.should_block());
        session.exit_script();
        assert!(session.should_block());
    }

    #[test]
    fn mark_dirty_only_takes_effect_in_multi() {
        let mut session = test_session();
        session.mark_dirty();
        assert!(!session.is_dirty());

        session.enter_multi();
        session.mark_dirty();
        assert!(session.is_dirty());
    }

    fn key_str(session: &Session, key: &[u8]) -> String {
        String::from_utf8_lossy(&session.public_key(key)).into_owned()
    }

    #[test]
    fn needs_ssi_false_with_no_watched_keys() {
        let mut session = test_session();
        let op = strings::set(&session, b"foo", b"bar", None);
        session.enqueue_op(op);
        assert!(!session.needs_ssi(), "no watched keys → SI");
    }

    #[test]
    fn needs_ssi_false_when_watched_key_not_in_queue() {
        let mut session = test_session();
        session.watched_keys.insert(key_str(&session, b"other"), (0, false));
        let op = strings::set(&session, b"foo", b"bar", None);
        session.enqueue_op(op);
        assert!(!session.needs_ssi(), "watched key not touched by queued op → SI");
    }

    #[test]
    fn needs_ssi_true_when_watched_key_in_queue() {
        let mut session = test_session();
        session.watched_keys.insert(key_str(&session, b"foo"), (0, false));
        let op = strings::set(&session, b"foo", b"bar", None);
        session.enqueue_op(op);
        assert!(session.needs_ssi(), "watched key touched by queued op → SSI");
    }

    #[test]
    fn needs_ssi_true_when_any_watched_key_matches() {
        let mut session = test_session();
        // watch two keys; queue an op that touches only the second one
        session.watched_keys.insert(key_str(&session, b"unrelated"), (0, false));
        session.watched_keys.insert(key_str(&session, b"bar"), (0, false));
        let op = strings::set(&session, b"bar", b"val", None);
        session.enqueue_op(op);
        assert!(session.needs_ssi(), "one of the watched keys is touched → SSI");
    }

    #[test]
    fn needs_ssi_cleared_after_unwatch() {
        let mut session = test_session();
        session.watched_keys.insert(key_str(&session, b"foo"), (0, false));
        let op = strings::set(&session, b"foo", b"bar", None);
        session.enqueue_op(op);
        assert!(session.needs_ssi());

        session.unwatch();
        assert!(!session.needs_ssi(), "unwatch clears the set → back to SI");
    }

    #[tokio::test]
    async fn dispatch_executes_queued_set_and_persists() {
        let mut session = test_session();
        let op = strings::set(&session, b"foo", b"bar", None);
        session.enqueue_op(op);
        let replies = session.dispatch_pending_ops(false).await;
        assert_eq!(
            replies,
            vec![RespValue::SimpleString(Bytes::from_static(b"OK"))]
        );

        let store = session.store();
        let tx = store.begin(false, TxIsolation::Snapshot).await.unwrap();
        let item = tx.get(&session.public_key(b"foo")).await.unwrap();
        assert_eq!(item.value(), b"bar");
        drop(tx);
    }
}
