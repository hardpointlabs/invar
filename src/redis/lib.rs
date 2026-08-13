//! Redis wire-protocol support for Invar.
//!
//! Mirrors the Go `redis` package: a Tokio TCP listener ([`listener`]), the
//! RESP codec ([`resp`]), command dispatch ([`commands`]), and the shared
//! per-connection plumbing ([`common`]).

pub mod commands;
pub mod common;
pub mod listener;
pub mod resp;
pub mod strings;

#[cfg(test)]
pub mod testutil;

pub use common::{RedisStore, Session, WatchRegistry};
pub use listener::RedisListener;
pub use resp::{RespDecoder, RespError, RespValue};
