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

/// Every `(key, value)` stored under `prefix`, in key order.
pub(crate) async fn entries_snapshot(session: &Session, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    use kv::kv::TxIsolation;
    let store = session.store();
    let tx = store.begin(false, TxIsolation::Snapshot).await.expect("tx");
    let mut it = tx.new_prefix_iterator(prefix).await.expect("iterator");
    let mut entries = Vec::new();
    while it.next().await {
        if let Some(item) = it.item() {
            entries.push((item.key().to_vec(), item.value().to_vec()));
        }
    }
    it.close().await.expect("close");
    entries
}

/// The namespace the compound key `key` was created with. Panics if the key
/// is a legacy (name-addressed) one.
pub(crate) async fn namespace_of(session: &Session, key: &[u8]) -> Vec<u8> {
    use crate::common::namespace::{is_namespaced, prefix_of};
    use kv::kv::TxIsolation;

    let public = session.public_key(key);
    let store = session.store();
    let tx = store.begin(false, TxIsolation::Snapshot).await.expect("tx");
    let item = tx.get(&public).await.expect("key exists");
    assert!(is_namespaced(item.metadata()), "key has no namespace");
    prefix_of(&item, &session.private_key(key)).expect("trailer").to_vec()
}

/// Converts the compound key `key` from the namespaced layout to the legacy
/// one: its elements move under the prefix derived from its name and its
/// sentinel stops recording a namespace. For tests that need data laid out the
/// way earlier releases wrote it.
pub(crate) async fn legacy_key(session: &Session, key: &[u8]) {
    use crate::common::namespace::{base_type, is_namespaced, payload, prefix_of};
    use kv::kv::{Entry, TxIsolation};

    let public = session.public_key(key);
    let legacy = session.private_key(key);
    let store = session.store();
    let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
    let item = tx.get(&public).await.expect("key exists");
    assert!(is_namespaced(item.metadata()), "key is already in the legacy layout");

    let from = prefix_of(&item, &legacy).expect("trailer");
    crate::keys::move_elements(&*tx, &public, base_type(item.metadata()), &from, &legacy)
        .await
        .expect("move elements");
    let value = payload(&item).expect("payload").to_vec();
    tx.set(Entry::new(public, value).metadata(base_type(item.metadata())).keep_ttl_of(&item)).expect("set");
    tx.commit().await.expect("commit");
}
