//! Redis scripting support: the `EVAL` command.
//!
//! Runs a Lua script one-shot through the piccolo interpreter. Scripts see the
//! core stdlib only (`base`, `coroutine`, `math`, `string`, `table` — no I/O),
//! mirroring the sandbox Redis applies to its Lua engine, and are handed the
//! standard `KEYS` and `ARGV` tables. Redis commands are not exposed to the
//! script yet. The script's return value is converted to a Redis reply the same
//! way Redis converts Lua return values (see `value_to_result`).
//!
//! piccolo's [`Lua`] is `!Send` while the op's future must be `Send`, so the
//! interpreter is created, run to completion, and dropped entirely within the
//! synchronous section of `EvalOp::run` — before the future ever reaches an
//! `.await`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use piccolo::Error as PicError;
use piccolo::{Closure, Context, Executor, Fuel, Lua, Value};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::resp::RespValue;
use kv::kv::{BoxFuture, Tx};

/// Approximate VM-instruction budget granted to the interpreter between
/// deadline checks.
const FUEL_PER_TICK: i32 = 1 << 20;

/// Wall-clock limit for a single script run, mirroring Redis's default
/// `lua-time-limit`.
const MAX_EXECUTION_TIME: Duration = Duration::from_secs(5);

/// The owned Lua return value of a script, shaped like a Redis reply.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EvalResult {
    Nil,
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<EvalResult>),
}

/// `EVAL script numkeys [key ...] [arg ...]`.
pub fn eval(script: Bytes, keys: Vec<Bytes>, argv: Vec<Bytes>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(EvalOp { script, keys, argv }),
        wire_op: Box::new(EvalWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct EvalOp {
    script: Bytes,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
}

impl DbOp for EvalOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let script = self.script.clone();
        let keys = self.keys.clone();
        let argv = self.argv.clone();
        Box::pin(async move {
            let result = run_script(&script, &keys, &argv)?;
            let result: DbResult = Box::new(result);
            Ok(result)
        })
    }
}

struct EvalWire;

impl WireOp for EvalWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(result) => match result.downcast::<EvalResult>() {
                Ok(result) => result_reply(&result),
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error")),
            },
            Err(err) => err_resp(&err),
        }
    }
}

/// Runs `script` once and converts its return value to [`EvalResult`], mapping
/// compile and runtime failures to the messages Redis emits.
fn run_script(script: &[u8], keys: &[Bytes], argv: &[Bytes]) -> Result<EvalResult, DbError> {
    let mut lua = Lua::core();

    // Compile the script, stashing the closure so it survives the arena exit.
    // A failure here is a syntax error, distinct from a runtime error.
    let closure =     match lua.try_enter(|ctx| Ok(ctx.stash(Closure::load(ctx, None, script)?))) {
        Ok(closure) => closure,
        Err(err) => {
            return Err(DbError::Redis(format!(
                "Error compiling script (new script): {}",
                static_error_message(&err)
            )))
        }
    };

    match lua.try_enter(|ctx| {
        let closure = ctx.fetch(&closure);
        let keys_table = table_from_slice(ctx, keys)?;
        let argv_table = table_from_slice(ctx, argv)?;
        ctx.set_global("KEYS", keys_table)?;
        ctx.set_global("ARGV", argv_table)?;
        run_executor(ctx, closure)
    }) {
        Ok(result) => Ok(result),
        Err(err) => Err(DbError::Redis(format!(
            "Error running script (new script): {}",
            static_error_message(&err)
        ))),
    }
}

/// Renders the message of a [`piccolo::StaticError`] without piccolo's
/// `lua error:` / `runtime error:` prefixes.
fn static_error_message(err: &piccolo::StaticError) -> String {
    match err {
        piccolo::StaticError::Lua(e) => e.to_string(),
        piccolo::StaticError::Runtime(e) => e.to_string(),
    }
}

/// Runs the loaded closure to completion, enforcing a wall-clock deadline.
fn run_executor<'gc>(
    ctx: Context<'gc>,
    closure: Closure<'gc>,
) -> Result<EvalResult, PicError<'gc>> {
    let executor = Executor::start(ctx, closure.into(), ());
    let deadline = Instant::now() + MAX_EXECUTION_TIME;
    let mut fuel = Fuel::with(FUEL_PER_TICK);
    loop {
        if executor.step(ctx, &mut fuel) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(PicError::from(Value::String(ctx.intern(
                b"Script exceeded its execution time limit".as_slice(),
            ))));
        }
        fuel.refill(FUEL_PER_TICK, FUEL_PER_TICK);
    }
    match executor.take_result::<Value>(ctx) {
        Ok(Ok(value)) => value_to_result(ctx, value),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(PicError::from(Value::String(ctx.intern(
            b"Script did not return a value".as_slice(),
        )))),
    }
}

/// Builds a Lua array table (indices 1..n) from the given byte slices.
fn table_from_slice<'gc>(
    ctx: Context<'gc>,
    values: &[Bytes],
) -> Result<piccolo::Table<'gc>, PicError<'gc>> {
    let table = piccolo::Table::new(&ctx);
    for (i, value) in values.iter().enumerate() {
        let key = Value::Integer(i as i64 + 1);
        let value = Value::from(ctx.intern(value.as_ref()));
        table.set_value(&ctx, key, value).map_err(|err| {
            PicError::from(Value::String(ctx.intern(err.to_string().as_bytes())))
        })?;
    }
    Ok(table)
}

/// Converts a Lua return value to [`EvalResult`] following Redis's reply rules:
/// nil -> null, booleans -> 0/1 integers, numbers -> integers, strings -> bulk,
/// sequential tables -> arrays (hash part -> array of interleaved value/key).
fn value_to_result<'gc>(
    ctx: Context<'gc>,
    value: Value<'gc>,
) -> Result<EvalResult, PicError<'gc>> {
    match value {
        Value::Nil => Ok(EvalResult::Nil),
        Value::Boolean(b) => Ok(EvalResult::Integer(if b { 1 } else { 0 })),
        Value::Integer(n) => Ok(EvalResult::Integer(n)),
        Value::Number(f) => Ok(EvalResult::Integer(f as i64)),
        Value::String(s) => Ok(EvalResult::Bulk(s.as_bytes().to_vec())),
        Value::Table(t) => table_to_result(ctx, t),
        _ => Err(PicError::from(Value::String(ctx.intern(
            b"Script returned an unsupported value type".as_slice(),
        )))),
    }
}

/// Converts a Lua table to [`EvalResult`]. A non-empty sequence is converted
/// element-wise; a hash-only table is converted to an array of interleaved
/// value, key pairs, mirroring Redis.
fn table_to_result<'gc>(
    ctx: Context<'gc>,
    table: piccolo::Table<'gc>,
) -> Result<EvalResult, PicError<'gc>> {
    let len = table.length();
    if len > 0 {
        let mut items = Vec::with_capacity(len as usize);
        for i in 1..=len {
            items.push(value_to_result(ctx, table.get(ctx, i))?);
        }
        Ok(EvalResult::Array(items))
    } else {
        let mut items = Vec::new();
        let mut key = Value::Nil;
        while let piccolo::table::NextValue::Found { key: k, value: v } = table.next(key) {
            items.push(value_to_result(ctx, v)?);
            items.push(value_to_result(ctx, k)?);
            key = k;
        }
        Ok(EvalResult::Array(items))
    }
}

/// Renders a converted [`EvalResult`] into its RESP reply.
fn result_reply(result: &EvalResult) -> RespValue {
    match result {
        EvalResult::Nil => RespValue::BulkString(None),
        EvalResult::Integer(n) => RespValue::Integer(*n),
        EvalResult::Bulk(b) => RespValue::BulkString(Some(Bytes::copy_from_slice(b))),
        EvalResult::Array(items) => {
            RespValue::Array(Some(items.iter().map(result_reply).collect()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Runs an op through its own transaction and renders the reply.
    async fn exec(op: QueuedOp) -> RespValue {
        let session = test_session();
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    fn eval_op(script: &str, keys: &[&str], argv: &[&str]) -> QueuedOp {
        eval(
            Bytes::copy_from_slice(script.as_bytes()),
            keys.iter()
                .map(|k| Bytes::copy_from_slice(k.as_bytes()))
                .collect(),
            argv.iter()
                .map(|a| Bytes::copy_from_slice(a.as_bytes()))
                .collect(),
        )
    }

    fn expect_integer(reply: &RespValue) -> i64 {
        match reply {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_bulk(reply: &RespValue) -> Option<Bytes> {
        match reply {
            RespValue::BulkString(b) => b.clone(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    fn expect_array(reply: &RespValue) -> Vec<RespValue> {
        match reply {
            RespValue::Array(Some(items)) => items.clone(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn expect_error_contains(reply: &RespValue, needle: &str) {
        match reply {
            RespValue::Error(msg) => {
                assert!(
                    String::from_utf8_lossy(msg).contains(needle),
                    "error {msg:?} missing {needle:?}"
                )
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn returns_integer() {
        let reply = exec(eval_op("return 1", &[], &[])).await;
        assert_eq!(expect_integer(&reply), 1);

        let reply = exec(eval_op("return -5", &[], &[])).await;
        assert_eq!(expect_integer(&reply), -5);
    }

    #[tokio::test]
    async fn returns_boolean_as_integer() {
        let reply = exec(eval_op("return true", &[], &[])).await;
        assert_eq!(expect_integer(&reply), 1);

        let reply = exec(eval_op("return false", &[], &[])).await;
        assert_eq!(expect_integer(&reply), 0);
    }

    #[tokio::test]
    async fn returns_string() {
        let reply = exec(eval_op("return 'hello'", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn returns_nil() {
        let reply = exec(eval_op("return nil", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn truncates_float_to_integer() {
        let reply = exec(eval_op("return 3.7", &[], &[])).await;
        assert_eq!(expect_integer(&reply), 3);
    }

    #[tokio::test]
    async fn returns_array() {
        let reply = exec(eval_op("return {1,2,3}", &[], &[])).await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 3);
        for (i, item) in items.iter().enumerate() {
            assert_eq!(expect_integer(item), i as i64 + 1);
        }
    }

    #[tokio::test]
    async fn returns_nested_array() {
        let reply = exec(eval_op("return {1,{2,3}}", &[], &[])).await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_integer(&items[0]), 1);
        let nested = expect_array(&items[1]);
        assert_eq!(expect_integer(&nested[0]), 2);
        assert_eq!(expect_integer(&nested[1]), 3);
    }

    #[tokio::test]
    async fn returns_empty_table() {
        let reply = exec(eval_op("return {}", &[], &[])).await;
        assert_eq!(expect_array(&reply), Vec::new());
    }

    #[tokio::test]
    async fn returns_hash_table_pairs() {
        let reply = exec(eval_op("return {a=1}", &[], &[])).await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_integer(&items[0]), 1);
        assert_eq!(expect_bulk(&items[1]).as_deref(), Some(b"a".as_slice()));
    }

    #[tokio::test]
    async fn script_sees_keys_and_argv() {
        let reply = exec(eval_op(
            "return KEYS[1] .. ':' .. ARGV[1] .. ':' .. #KEYS .. ':' .. #ARGV",
            &["k1", "k2"],
            &["a1", "a2", "a3"],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"k1:a1:2:3".as_slice()));
    }

    #[tokio::test]
    async fn script_has_no_io_library() {
        let reply = exec(eval_op("return io", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn runtime_error_replies_with_message() {
        let reply = exec(eval_op("error('boom')", &[], &[])).await;
        expect_error_contains(&reply, "Error running script (new script): boom");
    }

    #[tokio::test]
    async fn compile_error_replies_with_message() {
        let reply = exec(eval_op("this is not lua", &[], &[])).await;
        expect_error_contains(&reply, "Error compiling script (new script):");
    }

    #[tokio::test]
    async fn unsupported_return_type_errors() {
        let reply = exec(eval_op("return function() end", &[], &[])).await;
        expect_error_contains(&reply, "unsupported value type");
    }
}
