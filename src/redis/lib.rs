//! Redis wire-protocol support for Invar.
//!
//! Mirrors the Go `redis` package: a Tokio TCP listener ([`listener`]), the
//! RESP codec ([`resp`]), command dispatch ([`commands`]), and the shared
//! per-connection plumbing ([`common`]).

pub mod bloom;
pub mod bitmap;
pub mod commands;
pub mod common;
pub mod conn;
pub mod hash;
pub mod hll;
pub mod json;
pub mod keys;
pub mod list;
pub mod listener;
pub mod pubsub;
pub mod pubsub_cmds;
pub mod resp;
pub mod set;
pub mod strings;
pub mod zset;

#[cfg(test)]
pub mod testutil;

pub use common::{RedisStore, Session, WatchRegistry};
pub use listener::RedisListener;
pub use pubsub::PubSubRegistry;
pub use resp::{RespDecoder, RespError, RespValue};
