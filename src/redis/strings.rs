//! Redis string commands: `SET` and `GET`.
//!
//! These are the first commands built on the session plumbing and demonstrate
//! the two-halves pattern: each command returns a [`QueuedOp`] whose [`DbOp`]
//! performs the KV-store operations (deriving storage keys via the session)
//! and whose [`WireOp`] translates the result into the RESP reply.

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// The metadata byte stored on every string entry.
const TYPE_STRING: u8 = ValueType::String as u8;

/// `SET key value` — stores a string value, overwriting any existing value.
pub fn set(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SetOp {
            key: session.public_key(key),
            value: value.to_vec(),
        }),
        wire_op: Box::new(SetOpWire),
        is_mutating: true,
    }
}

/// `GET key` — returns the string value stored at key, or nil if missing.
pub fn get(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetOp {
            key: session.public_key(key),
        }),
        wire_op: Box::new(GetOpWire),
        is_mutating: false,
    }
}

struct SetOp {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for SetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let entry = Entry::new(self.key.clone(), self.value.clone()).metadata(TYPE_STRING);
        Box::pin(async move {
            tx.set(entry)?;
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

struct SetOpWire;

impl WireOp for SetOpWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => err_resp(&e),
        }
    }
}

struct GetOp {
    key: Vec<u8>,
}

impl DbOp for GetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        Box::pin(async move {
            let item = match tx.get(&key).await {
                Ok(item) => item,
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            if item.metadata() != TYPE_STRING {
                return Err(DbError::WrongType);
            }
            let result: DbResult = Box::new(Some(item.value().to_vec()));
            Ok(result)
        })
    }
}

struct GetOpWire;

impl WireOp for GetOpWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<Vec<u8>>>() {
                Ok(boxed) => match *boxed {
                    Some(value) => RespValue::BulkString(Some(Bytes::from(value))),
                    None => RespValue::BulkString(None),
                },
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad GET result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}
