//! Test-only helpers for the `redis` crate.

#![cfg(test)]

use std::sync::Arc;

use kv::fjall::FjallDb;

use crate::common::{RedisStore, Session, WatchRegistry};

/// Opens a temporary Fjall-backed store.
pub(crate) fn test_store() -> Arc<dyn RedisStore> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep();
    Arc::new(FjallDb::temporary(&path).expect("fjall"))
}

/// Builds a session over a fresh test store.
pub(crate) fn test_session() -> Session {
    Session::new(test_store(), Arc::new(WatchRegistry::new()))
}

/// Runs one op in its own transaction (committing if it mutates) and renders
/// the reply.
pub(crate) async fn exec(session: &Session, op: crate::common::QueuedOp) -> crate::resp::RespValue {
    use kv::kv::TxIsolation;
    let store = session.store();
    let tx = store.begin(op.is_mutating, TxIsolation::Snapshot).await.expect("tx");
    let outcome = op.db_op.run(&*tx).await;
    if op.is_mutating {
        tx.commit().await.expect("commit");
    }
    op.wire_op.reply(outcome)
}

/// Converts the compound key `key`, built the normal (legacy) way, to the
/// namespaced layout: its elements move under `namespace` and its sentinel
/// records it. Everything that reads or writes the key afterwards must find
/// the elements there.
pub(crate) async fn namespace_key(session: &Session, key: &[u8], namespace: &[u8]) {
    use crate::common::namespace::{base_type, encode_sentinel, is_namespaced, ElementPrefix};
    use kv::kv::{Entry, TxIsolation};

    let public = session.public_key(key);
    let store = session.store();
    let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
    let item = tx.get(&public).await.expect("key exists");
    assert!(!is_namespaced(item.metadata()), "key is already namespaced");

    crate::keys::move_elements(&*tx, &public, base_type(item.metadata()), &session.private_key(key), namespace)
        .await
        .expect("move elements");
    let (value, metadata) = encode_sentinel(
        item.value().to_vec(),
        item.metadata(),
        &ElementPrefix::explicit(namespace.to_vec()),
    );
    tx.set(Entry::new(public, value).metadata(metadata).keep_ttl_of(&item)).expect("set");
    tx.commit().await.expect("commit");
}

/// How many entries are stored under `prefix`.
pub(crate) async fn entries_under(session: &Session, prefix: &[u8]) -> usize {
    use kv::kv::TxIsolation;
    let store = session.store();
    let tx = store.begin(false, TxIsolation::Snapshot).await.expect("tx");
    let mut it = tx.new_prefix_iterator(prefix).await.expect("iterator");
    let mut count = 0;
    while it.next().await {
        count += 1;
    }
    it.close().await.expect("close");
    count
}

/// The type byte stored with the entry at `public_key`.
pub(crate) async fn stored_type_byte(session: &Session, public_key: &[u8]) -> Option<u8> {
    use kv::kv::TxIsolation;
    let store = session.store();
    let tx = store.begin(false, TxIsolation::Snapshot).await.expect("tx");
    tx.get(public_key).await.ok().map(|item| item.metadata())
}
