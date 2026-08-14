//! Per-connection ephemeral state, mirroring the Go `redis/common` `Session`.
//!
//! In Go the `Session` lives on the per-connection `context` and is guaranteed
//! never to be mutated by more than one thread. Tokio makes no thread-pinning
//! promise, but the guarantee that matters still holds: a session is owned by
//! exactly one connection task at a time, so it needs no locks for its own
//! fields. The only process-shared state is the [`WatchRegistry`], which
//! guards itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use kv::kv::Entry;

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
    current_db: i32,
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

impl Session {
    pub fn new(store: Arc<dyn RedisStore>, registry: Arc<WatchRegistry>) -> Self {
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            current_db: 0,
            queue: Vec::new(),
            in_multi: false,
            dirty_exec: false,
            in_script: false,
            should_close: false,
            client_name: String::new(),
            lib_name: String::new(),
            lib_ver: String::new(),
            peer_addr: None,
            store,
            registry,
            pubsub: Arc::new(PubSubRegistry::new()),
        }
    }

    /// Creates a session with an explicit, shared pub/sub registry.  Used by
    /// the listener so all connections on the same server share one registry.
    pub fn new_with_pubsub(
        store: Arc<dyn RedisStore>,
        registry: Arc<WatchRegistry>,
        pubsub: Arc<PubSubRegistry>,
    ) -> Self {
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            current_db: 0,
            queue: Vec::new(),
            in_multi: false,
            dirty_exec: false,
            in_script: false,
            should_close: false,
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
    pub fn enqueue_wire_op(&mut self, wire_op: Box<dyn WireOp>) -> Option<RespValue> {
        self.enqueue_op(QueuedOp {
            db_op: Box::new(NoOp),
            wire_op,
            is_mutating: false,
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
        let tx = match self.store.begin(mutating).await {
            Ok(tx) => tx,
            Err(e) => {
                self.queue.clear();
                return vec![RespValue::Error(format!("ERR {e}").into())];
            }
        };

        let mut outcomes: Vec<Result<DbResult, DbError>> = Vec::with_capacity(self.queue.len());

        for i in 0..self.queue.len() {
            let outcome = self.queue[i].db_op.run(&*tx).await;
            if outcome.is_err() && !batch {
                // Any claims made by earlier ops in this batch must be
                // returned to the front of their queues since the transaction
                // will be discarded.
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
            // element: sibling commands still run and the transaction still
            // commits.
            outcomes.push(outcome);
        }

        if tx.commit().await.is_err() {
            // The transaction failed to commit — release all claims back to
            // the front of their queues so their waiters remain longest.
            for (j, result) in outcomes.iter().enumerate() {
                if let Ok(r) = result {
                    self.queue[j].db_op.release_claims(r);
                }
            }
            self.queue.clear();
            return vec![RespValue::Error(Bytes::from_static(
                b"ERR Couldn't commit transaction",
            ))];
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
        replies
    }

    /// Whether any queued op requires a writable transaction.
    fn needs_writable_tx(&self) -> bool {
        self.queue.iter().any(|op| op.is_mutating)
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

    #[tokio::test]
    async fn dispatch_executes_queued_set_and_persists() {
        let mut session = test_session();
        let op = strings::set(&session, b"foo", b"bar");
        session.enqueue_op(op);
        let replies = session.dispatch_pending_ops(false).await;
        assert_eq!(
            replies,
            vec![RespValue::SimpleString(Bytes::from_static(b"OK"))]
        );

        let store = session.store();
        let tx = store.begin(false).await.unwrap();
        let item = tx.get(&session.public_key(b"foo")).await.unwrap();
        assert_eq!(item.value(), b"bar");
        drop(tx);
    }
}
