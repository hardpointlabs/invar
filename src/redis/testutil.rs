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
