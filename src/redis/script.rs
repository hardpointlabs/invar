//! Redis scripting support: the `EVAL` command.
//!
//! Runs a Lua script one-shot through the piccolo interpreter. Scripts see the
//! core stdlib only (`base`, `coroutine`, `math`, `string`, `table` — no I/O),
//! mirroring the sandbox Redis applies to its Lua engine, and are handed the
//! standard `KEYS` and `ARGV` tables plus a `redis` table exposing `call()` and
//! `pcall()` for executing Redis commands from within the script. The script's
//! return value is converted to a Redis reply the same way Redis converts Lua
//! return values (see `value_to_result`).
//!
//! `redis.call()` errors propagate as Lua runtime exceptions automatically.
//!
//! piccolo's [`Lua`] is `!Send` while the op's future must be `Send`, so the
//! interpreter is created, run to completion, and dropped entirely within the
//! synchronous section of `EvalOp::run` — before the future ever reaches an
//! `.await`.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use gc_arena::Collect;
use piccolo::Error as PicError;
use piccolo::{Callback, CallbackReturn, Closure, Context, Executor, Fuel, Lua, Stack, Value};
use sha1::{Digest, Sha1};

use crate::commands::dispatch_command;
use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::registry::{Claim, WatchRegistry};
use crate::common::session::Session;
use crate::common::store::RedisStore;
use crate::resp::RespValue;
use kv::kv::{BoxFuture, Tx};

/// Global cache mapping SHA1 hex digests to their Lua script source.
/// Populated by EVAL, consulted by EVALSHA.
fn script_cache() -> &'static Mutex<HashMap<String, Bytes>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Bytes>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn sha1_hex(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let hash = hasher.finalize();
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Stores a script in the global cache, keyed by its SHA1 hex digest.
fn cache_script(script: &[u8]) -> String {
    let digest = sha1_hex(script);
    let mut cache = script_cache().lock().unwrap();
    cache.entry(digest.clone()).or_insert_with(|| Bytes::copy_from_slice(script));
    digest
}

/// Looks up a cached script by its SHA1 hex digest.
fn lookup_script(sha1: &str) -> Option<Bytes> {
    script_cache().lock().unwrap().get(sha1).cloned()
}

/// Approximate VM-instruction budget granted to the interpreter between
/// deadline checks.
const FUEL_PER_TICK: i32 = 1 << 20;

/// Root type for the piccolo GC arena, holding the context needed by `redis.call()`.
///
/// The raw pointer to the transaction is safe because:
/// 1. The transaction outlives the Lua interpreter (created in `EvalOp::run`)
/// 2. The callback completes synchronously before returning control to piccolo
struct ScriptContext {
    /// Raw pointer to the current transaction. Safe because the transaction
    /// outlives the Lua interpreter. Initialized to a null-like value and set
    /// before running the script.
    tx: UnsafeCell<Option<*const dyn Tx>>,
    /// The key-value store for creating new transactions.
    store: Arc<dyn RedisStore>,
    /// The watch registry for blocking commands.
    registry: Arc<WatchRegistry>,
    /// The current Redis DB number for key derivation.
    current_db: i32,
    /// Claims (`XREAD`, `BZPOP*`...) made by `redis.call()` commands issued
    /// inside the script. A script runs against a transaction that does not
    /// commit until after the Lua code finishes, so a claim must not be woken
    /// mid-script (the reader would wake up before the write is committed).
    /// These are woken by the post-commit `EvalWire::reply` instead.
    deferred_claims: Mutex<Vec<Claim>>,
}

// SAFETY: ScriptContext is used within a single-threaded Lua interpreter.
// The UnsafeCell is only accessed from the callback, which runs synchronously.
unsafe impl Send for ScriptContext {}
unsafe impl Sync for ScriptContext {}

// SAFETY: ScriptContext contains no GC pointers, only raw pointers and Arcs.
// We implement Collect with needs_trace() = false to satisfy piccolo's requirements.
unsafe impl Collect for ScriptContext {
    fn needs_trace() -> bool {
        false
    }
}

/// Converts a piccolo `Value` to a `Bytes` for use as a Redis command argument.
/// Returns `Err` if the value cannot be converted to a string.
fn value_to_bytes<'gc>(ctx: Context<'gc>, value: Value<'gc>) -> Result<Bytes, PicError<'gc>> {
    match value {
        Value::Nil => Ok(Bytes::new()),
        Value::Boolean(b) => Ok(Bytes::from(if b { "1" } else { "0" }.to_string())),
        Value::Integer(n) => Ok(Bytes::from(n.to_string())),
        Value::Number(f) => Ok(Bytes::from(f.to_string())),
        Value::String(s) => Ok(Bytes::copy_from_slice(s.as_bytes())),
        _ => Err(PicError::from(Value::String(ctx.intern(
            b"redis.call() argument must be a string, number, boolean, or nil".as_slice(),
        )))),
    }
}

/// Converts a `RespValue` back to a piccolo `Value`.
/// Builds a Lua table with a single string field, i.e. `{ok = "..."}` or
/// `{err = "..."}`. This mirrors how Redis represents status and error replies
/// in the Lua sandbox so that `redis.call("TYPE", k)["ok"]` works.
fn reply_table<'gc>(ctx: Context<'gc>, key: &[u8], msg: &[u8]) -> Value<'gc> {
    let table = piccolo::Table::new(&ctx);
    let k = Value::from(ctx.intern(key));
    let v = Value::from(ctx.intern(msg));
    table.set_value(&ctx, k, v).ok();
    Value::Table(table)
}

fn resp_to_piccolo<'gc>(ctx: Context<'gc>, resp: RespValue) -> Value<'gc> {
    match resp {
        RespValue::SimpleString(s) => reply_table(ctx, b"ok", s.as_ref()),
        RespValue::Error(msg) => reply_table(ctx, b"err", msg.as_ref()),
        RespValue::Integer(n) => Value::Integer(n),
        RespValue::BulkString(opt) => match opt {
            Some(b) => Value::from(ctx.intern(b.as_ref())),
            // Redis maps null bulk replies (`$-1`) to Lua `false`, not `nil`,
            // so arrays with missing fields (`HMGET`) keep their positions for
            // `ipairs`/`#` handling (e.g. BullMQ's getTimestamp).
            None => Value::from(false),
        },
        RespValue::Array(opt) => match opt {
            Some(items) => {
                let table = piccolo::Table::new(&ctx);
                for (i, item) in items.into_iter().enumerate() {
                    let key = Value::Integer(i as i64 + 1);
                    let value = resp_to_piccolo(ctx, item);
                    table.set_value(&ctx, key, value).ok();
                }
                Value::Table(table)
            }
            None => Value::from(false),
        },
    }
}

// ---------------------------------------------------------------------------
// Lua `string.match` pattern engine.
//
// piccolo's core stdlib only ships `string.len/lower/reverse/sub/upper`, but
// Redis scripts (notably BullMQ's job key destructuring, `string.match(jobKey,
// ".*:(.*)")`) depend on `string.match`. Redis's Lua is Lua 5.1, so we
// implement a Lua-5.1-style pattern matcher over bytes rather than leaning on a
// regex crate: patterns use `.` (any), `%a %d %p %s %w ...` classes (uppercase
// = complement), `[...]` sets, `%x` literal escapes, `^`/`$` anchors, the
// `* + - ?` quantifiers, and `( )` captures.
// ---------------------------------------------------------------------------

/// A single character class from a Lua pattern.
#[derive(Clone, Debug)]
enum PatClass {
    /// A literal byte (including `%x` escapes).
    Literal(u8),
    /// `.` — matches any byte.
    Any,
    /// A `%a`, `%d`, ... class (or its uppercase complement).
    Percent(u8),
    /// `[...]` / `[^...]` character set.
    Set { negated: bool, items: Vec<SetItem> },
}

/// A character-set member: a single byte or an inclusive range.
#[derive(Clone, Debug)]
enum SetItem {
    Byte(u8),
    Range(u8, u8),
    Percent(u8),
}

impl PatClass {
    fn matches(&self, c: u8) -> bool {
        match self {
            PatClass::Literal(l) => *l == c,
            PatClass::Any => true,
            PatClass::Percent(p) => percent_matches(*p, c),
            PatClass::Set { negated, items } => {
                let m = items.iter().any(|it| it.matches(c));
                if *negated {
                    !m
                } else {
                    m
                }
            }
        }
    }
}

impl SetItem {
    fn matches(&self, c: u8) -> bool {
        match self {
            SetItem::Byte(b) => *b == c,
            SetItem::Range(lo, hi) => c >= *lo && c <= *hi,
            SetItem::Percent(p) => percent_matches(*p, c),
        }
    }
}

/// `%x` classifier. Alphabetic codes match the class, uppercase codes match the
/// complement, and any other `%x` pair is handled as a literal escape before
/// this is ever called.
fn percent_matches(code: u8, c: u8) -> bool {
    let base = match code.to_ascii_lowercase() {
        b'a' => c.is_ascii_alphabetic(),
        b'c' => c.is_ascii_control(),
        b'd' => c.is_ascii_digit(),
        b'l' => c.is_ascii_lowercase(),
        b'p' => c.is_ascii_punctuation(),
        b's' => c.is_ascii_whitespace(),
        b'u' => c.is_ascii_uppercase(),
        b'w' => c.is_ascii_alphanumeric(),
        b'x' => c.is_ascii_hexdigit(),
        b'z' => c == 0,
        _ => {
            // Unknown class char — should not happen (escapes handled earlier).
            return false;
        }
    };
    if code.is_ascii_uppercase() {
        !base
    } else {
        base
    }
}

#[derive(Clone, Copy, Debug)]
enum Quant {
    // `*`
    Star,
    // `+`
    Plus,
    // `-` (non-greedy 0+)
    Minus,
    // `?`
    Question,
}

#[derive(Clone, Debug)]
enum PatNode {
    Open(usize),
    Close(usize),
    Match(PatClass, Option<Quant>),
}

/// Parses a Lua pattern `pat` into a node list (with `^`/`$` anchors stripped
/// into the returned flags). Returns `None` on malformed patterns.
fn parse_pattern(mut pat: &[u8]) -> Option<(Vec<PatNode>, bool, bool, usize)> {
    let mut nodes = Vec::new();
    let mut captures: Vec<Option<(usize, usize)>> = Vec::new();
    let mut capture_count = 0;

    let mut i = 0;
    let mut anchored_start = false;
    let mut anchored_end = false;

    if pat.first() == Some(&b'^') {
        anchored_start = true;
        i = 1;
    }
    if pat.last() == Some(&b'$') {
        anchored_end = true;
        // Strip the trailing `$`, but only if there's more than just `^`/`$`.
    }

    if anchored_end {
        // Determine effective length excluding trailing `$`.
        let mut end = pat.len();
        if end > 0 && pat[end - 1] == b'$' {
            end -= 1;
        }
        pat = &pat[..end];
    }

    while i < pat.len() {
        let b = pat[i];
        match b {
            b'(' => {
                capture_count += 1;
                nodes.push(PatNode::Open(capture_count - 1));
                captures.push(None);
                i += 1;
            }
            b')' => {
                if capture_count == 0 {
                    return None;
                }
                nodes.push(PatNode::Close(capture_count - 1));
                i += 1;
            }
            _ => {
                let class = parse_class(pat, &mut i)?;
                // Check for a trailing quantifier.
                let quant = match pat.get(i) {
                    Some(b'*') => {
                        i += 1;
                        Some(Quant::Star)
                    }
                    Some(b'+') => {
                        i += 1;
                        Some(Quant::Plus)
                    }
                    Some(b'-') => {
                        i += 1;
                        Some(Quant::Minus)
                    }
                    Some(b'?') => {
                        i += 1;
                        Some(Quant::Question)
                    }
                    _ => None,
                };
                nodes.push(PatNode::Match(class, quant));
            }
        }
    }

    Some((nodes, anchored_start, anchored_end, capture_count))
}

/// Parses a single character class starting at `i`, advancing `i` past it.
fn parse_class(pat: &[u8], i: &mut usize) -> Option<PatClass> {
    let b = *pat.get(*i)?;
    match b {
        b'.' => {
            *i += 1;
            Some(PatClass::Any)
        }
        b'%' => {
            let esc = *pat.get(*i + 1)?;
            *i += 2;
            // `%` followed by an alphabetic char is a class; otherwise it
            // escapes the literal char (`%.`, `%*`, `%/`, `%%`).
            if esc.is_ascii_alphabetic() {
                Some(PatClass::Percent(esc))
            } else {
                let lit = if esc == b'%' { b'%' } else { esc };
                Some(PatClass::Literal(lit))
            }
        }
        b'[' => parse_set(pat, i),
        c => {
            *i += 1;
            Some(PatClass::Literal(c))
        }
    }
}

fn parse_set(pat: &[u8], i: &mut usize) -> Option<PatClass> {
    // `[` at *i
    let mut j = *i + 1;
    let mut negated = false;
    if pat.get(j) == Some(&b'^') {
        negated = true;
        j += 1;
    }
    let mut items = Vec::new();
    let mut first = true;
    loop {
        let c = *pat.get(j)?;
        if c == b']' && !first {
            break;
        }
        first = false;
        if c == b'%' {
            let esc = *pat.get(j + 1)?;
            if esc.is_ascii_alphabetic() {
                items.push(SetItem::Percent(esc));
            } else {
                items.push(SetItem::Byte(if esc == b'%' { b'%' } else { esc }));
            }
            j += 2;
            continue;
        }
        // A `c-d` range (not a trailing range open at `]`).
        if pat.get(j + 1) == Some(&b'-')
            && pat.get(j + 2).is_some()
            && pat.get(j + 2) != Some(&b']')
        {
            let hi = *pat.get(j + 2)?;
            items.push(SetItem::Range(c, hi));
            j += 3;
            continue;
        }
        items.push(SetItem::Byte(c));
        j += 1;
    }
    *i = j + 1;
    Some(PatClass::Set { negated, items })
}

/// Matches `nodes[pi..]` against `s[si..]`, filling in capture positions in
/// `caps`. Returns the first `si` at which the remainder of the pattern is
/// satisfied, or `None`.
fn match_nodes(
    s: &[u8],
    nodes: &[PatNode],
    pi: usize,
    si: usize,
    caps: &mut [Option<(usize, usize)>],
) -> Option<usize> {
    if pi == nodes.len() {
        return Some(si);
    }
    match &nodes[pi] {
        PatNode::Open(idx) => {
            let saved = caps[*idx];
            caps[*idx] = Some((si, 0));
            let r = match_nodes(s, nodes, pi + 1, si, caps);
            if r.is_none() {
                caps[*idx] = saved;
            }
            r
        }
        PatNode::Close(idx) => {
            let prev = caps[*idx];
            if let Some((start, _)) = prev {
                caps[*idx] = Some((start, si));
            }
            let r = match_nodes(s, nodes, pi + 1, si, caps);
            if r.is_none() {
                caps[*idx] = prev;
            }
            r
        }
        PatNode::Match(class, quant) => {
            let next_pi = pi + 1;
            match quant {
                None => {
                    if si < s.len() && class.matches(s[si]) {
                        match_nodes(s, nodes, next_pi, si + 1, caps)
                    } else {
                        None
                    }
                }
                Some(Quant::Question) => {
                    if si < s.len() && class.matches(s[si]) {
                        if let Some(r) = match_nodes(s, nodes, next_pi, si + 1, caps) {
                            return Some(r);
                        }
                    }
                    match_nodes(s, nodes, next_pi, si, caps)
                }
                Some(Quant::Star) | Some(Quant::Plus) | Some(Quant::Minus) => {
                    // Compute the maximal run of matching chars.
                    let mut run = 0;
                    while si + run < s.len() && class.matches(s[si + run]) {
                        run += 1;
                    }
                    let min = if matches!(quant, Some(Quant::Plus)) { 1 } else { 0 };
                    if run < min {
                        return None;
                    }
                    let order: Vec<usize> = match quant {
                        Some(Quant::Minus) => (min..=run).collect(),
                        _ => (min..=run).rev().collect(),
                    };
                    for k in order {
                        if let Some(r) = match_nodes(s, nodes, next_pi, si + k, caps) {
                            return Some(r);
                        }
                    }
                    None
                }
            }
        }
    }
}

/// A successful `string.match` result: the whole-match span plus any captures.
struct PatternMatch {
    whole: (usize, usize),
    captures: Vec<Option<(usize, usize)>>,
}

/// Runs `string.match` semantics: returns the whole match plus any captures.
fn lua_pattern_match(s: &[u8], pat: &[u8]) -> Option<PatternMatch> {
    let (nodes, anchored_start, anchored_end, capture_count) = parse_pattern(pat)?;
    // A trailing `$` is an anchor; strip it from the node match by requiring
    // the match to reach the end of the string.
    let starts: Vec<usize> = if anchored_start {
        vec![0]
    } else {
        (0..=s.len()).collect()
    };
    for start in starts {
        let mut caps = vec![None; capture_count];
        if let Some(end) = match_nodes(s, &nodes, 0, start, &mut caps) {
            if anchored_end && end != s.len() {
                continue;
            }
            return Some(PatternMatch {
                whole: (start, end),
                captures: caps,
            });
        }
    }
    None
}

/// Executes a Redis command from within a Lua script, sharing the script's transaction.
///
/// This is the core of `redis.call()`: it parses the command, dispatches it,
/// runs the database operation against the shared transaction, and returns
/// the result to Lua.
fn execute_redis_call<'gc>(
    ctx: Context<'gc>,
    stack: &mut Stack<'gc, '_>,
    script_ctx: &ScriptContext,
) -> Result<Value<'gc>, PicError<'gc>> {
    let argc = stack.len();
    if argc == 0 {
        return Err(PicError::from(Value::String(ctx.intern(
            b"redis.call() requires at least one argument (the command name)".as_slice(),
        ))));
    }

    // Collect all arguments from the stack
    let mut args = Vec::with_capacity(argc);
    for i in 0..argc {
        let value = stack.get(i);
        args.push(value_to_bytes(ctx, value)?);
    }
    stack.clear();

    // Create a temporary session for key derivation
    let session = Session::new(script_ctx.store.clone(), script_ctx.registry.clone());
    // Note: We don't set current_db on the session because dispatch_command
    // will use it for key derivation, but we need to use the script's DB.

    // Dispatch the command - this is synchronous
    // Note: dispatch_command needs a mutable session, but we create a fresh one
    let mut session = session;
    session.switch_db(script_ctx.current_db);
    let cmd = dispatch_command(&mut session, &args);

    // Get the transaction reference
    let tx = unsafe {
        let tx_opt = &*script_ctx.tx.get();
        match tx_opt {
            Some(tx_ptr) => &**tx_ptr,
            None => {
                return Err(PicError::from(Value::String(ctx.intern(
                    b"redis.call() called outside of a transaction".as_slice(),
                ))));
            }
        }
    };

    // Run the database operation - this is effectively synchronous for Fjall
    let future = cmd.db_op.run(tx);
    let mut outcome = futures::executor::block_on(future);

    // Push any claim the op made (XADD/ZADD waking a blocked reader) onto the
    // script's deferred list. Requesting a `WireOp::reply` here would wake the
    // reader before this EVAL's transaction commits, losing the entry. The
    // claims are woken by `EvalWire::reply` after the script transaction
    // commits.
    if let Ok(result) = &mut outcome {
        cmd.db_op.defer_claims(result, &script_ctx.deferred_claims);
    }
    let resp = cmd.wire_op.reply(outcome);

    // Check if the response is an error - if so, raise it as a Lua error
    match resp {
        RespValue::Error(msg) => Err(PicError::from(Value::String(ctx.intern(msg.as_ref())))),
        _ => Ok(resp_to_piccolo(ctx, resp)),
    }
}

/// Wall-clock limit for a single script run, mirroring Redis's default
/// `lua-time-limit`.
const MAX_EXECUTION_TIME: Duration = Duration::from_secs(5);

/// The owned Lua return value of a script, shaped like a Redis reply.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EvalResult {
    Nil,
    Integer(i64),
    Bulk(Vec<u8>),
    /// A status reply, produced by `redis.status_reply(...)` or a `{ok = ...}`
    /// table.
    Status(Vec<u8>),
    /// An error reply, produced by `redis.error_reply(...)` or a `{err = ...}`
    /// table.
    Err(Vec<u8>),
    Array(Vec<EvalResult>),
}

fn debug_fmt(r: &EvalResult) -> String {
    match r {
        EvalResult::Nil => "nil".to_string(),
        EvalResult::Integer(i) => format!("int({i})"),
        EvalResult::Bulk(b) => format!("bulk({})", String::from_utf8_lossy(b)),
        EvalResult::Status(s) => format!("status({})", String::from_utf8_lossy(s)),
        EvalResult::Err(e) => format!("err({})", String::from_utf8_lossy(e)),
        EvalResult::Array(v) => {
            let mut out = String::from("[");
            for (i, item) in v.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&debug_fmt(item));
            }
            out.push(']');
            out
        }
    }
}

/// `EVAL script numkeys [key ...] [arg ...]`.
pub fn eval(
    script: Bytes,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    current_db: i32,
) -> QueuedOp {
    // Cache the script by its SHA1 digest so EVALSHA can find it later.
    cache_script(&script);

    QueuedOp {
        db_op: Box::new(EvalOp {
            script,
            keys,
            argv,
            store,
            registry,
            current_db,
        }),
        wire_op: Box::new(EvalWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `EVALSHA sha1 numkeys [key ...] [arg ...]`.
pub fn evalsha(
    sha1_hex: Bytes,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    current_db: i32,
) -> QueuedOp {
    let sha1_str = String::from_utf8_lossy(&sha1_hex).to_string();
    match lookup_script(&sha1_str) {
        Some(script) => {
            // Script found in cache — execute it like EVAL.
            QueuedOp {
                db_op: Box::new(EvalOp {
                    script,
                    keys,
                    argv,
                    store,
                    registry,
                    current_db,
                }),
                wire_op: Box::new(EvalWire),
                is_mutating: true,
                allowed_in_tx: true,
                abort_in_tx: false,
            }
        }
        None => {
            // Script not cached — return NOSCRIPT error (Redis convention).
            QueuedOp {
                db_op: Box::new(NoscriptOp),
                wire_op: Box::new(NoscriptWire),
                is_mutating: false,
                allowed_in_tx: true,
                abort_in_tx: false,
            }
        }
    }
}

struct NoscriptOp;

impl DbOp for NoscriptOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async { Err(DbError::Redis("NOSCRIPT No matching script. Use EVAL.".into())) })
    }
}

struct NoscriptWire;

impl WireOp for NoscriptWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Err(DbError::Redis(msg)) => RespValue::Error(Bytes::from(msg)),
            Err(_) => RespValue::Error(Bytes::from_static(b"ERR NOSCRIPT internal error")),
            Ok(_) => unreachable!(),
        }
    }
}

/// The boxed outcome of an `EVAL`: the script's Lua reply plus any deferred
/// waiter claims its `redis.call()` commands made. Delivered to [`EvalWire`]
/// only after the transaction commits, at which point the claims are woken.
struct EvalOutcome {
    result: EvalResult,
    claims: Vec<Claim>,
}

struct EvalOp {
    script: Bytes,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    store: Arc<dyn RedisStore>,
    registry: Arc<WatchRegistry>,
    current_db: i32,
}

impl DbOp for EvalOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let script = self.script.clone();
        let keys = self.keys.clone();
        let argv = self.argv.clone();
        let store = self.store.clone();
        let registry = self.registry.clone();
        let current_db = self.current_db;
        let sha1 = sha1_hex(&self.script);
        let hint = String::from_utf8_lossy(&self.script[..self.script.len().min(48)])
            .replace('\n', " ")
            .to_string();
        Box::pin(async move {
            tracing::debug!(
                %sha1,
                hints = %hint,
                keys = keys.len(),
                argv = argv.len(),
                "eval script",
            );
            let result = run_script(&script, &keys, &argv, &store, &registry, current_db, tx);
            match &result {
                Ok((r, _)) => tracing::debug!(%sha1, reply = %debug_fmt(r), "eval ok"),
                Err(e) => tracing::warn!(%sha1, error = %e, "eval failed"),
            }
            let (result, claims) = result?;
            let result: DbResult = Box::new(EvalOutcome { result, claims });
            Ok(result)
        })
    }

    /// The script's transaction failed to commit: return each deferred claim
    /// to the front of its queue so no blocked client is left waiting forever.
    fn release_claims(&self, result: &DbResult) {
        if let Some(outcome) = result.downcast_ref::<EvalOutcome>() {
            for claim in &outcome.claims {
                self.registry.release_front(claim);
            }
        }
    }
}

struct EvalWire;

impl WireOp for EvalWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(result) => match result.downcast::<EvalOutcome>() {
                Ok(outcome) => {
                    // Post-commit: the script's transaction has landed, so any
                    // XADD/ZADD wake is now safe to deliver.
                    for claim in &outcome.claims {
                        claim.wake();
                    }
                    result_reply(&outcome.result)
                }
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error")),
            },
            Err(err) => err_resp(&err),
        }
    }
}

/// Runs `script` once and converts its return value to [`EvalResult`], mapping
/// compile and runtime failures to the messages Redis emits.
///
/// Returns the script's reply plus any `XADD`/`ZADD` claims it made. A claim
/// must only be woken after the transaction that ran this script commits, so
/// the caller (`[`EvalOp`]`/`[`EvalWire`]`) accepts the claims and wakes them
/// from its post-commit reply.
///
/// The `store`, `registry`, `current_db`, and `tx` parameters are used to create
/// the `redis.call()` / `redis.pcall()` functions available to the script.
fn run_script(
    script: &[u8],
    keys: &[Bytes],
    argv: &[Bytes],
    store: &Arc<dyn RedisStore>,
    registry: &Arc<WatchRegistry>,
    current_db: i32,
    tx: &dyn Tx,
) -> Result<(EvalResult, Vec<Claim>), DbError> {
    let mut lua = Lua::core();

    // Compile the script, stashing the closure so it survives the arena exit.
    // A failure here is a syntax error, distinct from a runtime error.
    let closure = match lua.try_enter(|ctx| Ok(ctx.stash(Closure::load(ctx, None, script)?))) {
        Ok(closure) => closure,
        Err(err) => {
            return Err(DbError::Redis(format!(
                "Error compiling script (new script): {}",
                static_error_message(&err)
            )))
        }
    };

    // Create the script context for redis.call() support
    let script_ctx = Arc::new(ScriptContext {
        tx: UnsafeCell::new(Some(tx as *const dyn Tx)),
        store: store.clone(),
        registry: registry.clone(),
        current_db,
        deferred_claims: Mutex::new(Vec::new()),
    });

    match lua.try_enter(|ctx| {
        let closure = ctx.fetch(&closure);
        let keys_table = table_from_slice(ctx, keys)?;
        let argv_table = table_from_slice(ctx, argv)?;
        ctx.set_global("KEYS", keys_table)?;
        ctx.set_global("ARGV", argv_table)?;

        // Create the redis table with call() and pcall()
        let redis_table = piccolo::Table::new(&ctx);

        // Create the call function
        let call_script_ctx = script_ctx.clone();
        let call_fn = Callback::from_fn_with(
            &ctx,
            call_script_ctx,
            |script_ctx, ctx, _exec, mut stack| {
                match execute_redis_call(ctx, &mut stack, script_ctx) {
                    Ok(value) => {
                        stack.push_back(value);
                        Ok(CallbackReturn::Return)
                    }
                    Err(err) => Err(err),
                }
            },
        );
        let call_key = Value::from(ctx.intern(b"call"));
        redis_table
            .set_value(&ctx, call_key, call_fn.into())
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;

        // Create the pcall function
        let pcall_script_ctx = script_ctx.clone();
        let pcall_fn = Callback::from_fn_with(
            &ctx,
            pcall_script_ctx,
            |script_ctx, ctx, _exec, mut stack| {
                match execute_redis_call(ctx, &mut stack, script_ctx) {
                    Ok(value) => {
                        stack.push_back(Value::Boolean(true));
                        stack.push_back(value);
                        Ok(CallbackReturn::Return)
                    }
                    Err(err) => {
                        stack.push_back(Value::Boolean(false));
                        let err_msg = match err {
                            PicError::Lua(val) => val.0,
                            PicError::Runtime(val) => {
                                let msg = val.0.to_string();
                                Value::String(ctx.intern(msg.as_bytes()))
                            }
                        };
                        stack.push_back(err_msg);
                        Ok(CallbackReturn::Return)
                    }
                }
            },
        );
        let pcall_key = Value::from(ctx.intern(b"pcall"));
        redis_table
            .set_value(&ctx, pcall_key, pcall_fn.into())
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;

        // redis.error_reply(msg) -> a Lua table {err = msg} that becomes an
        // error reply when returned from a script.
        let error_reply_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            let msg = match stack.get(0) {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::Integer(n) => n.to_string().into_bytes(),
                Value::Number(f) => f.to_string().into_bytes(),
                Value::Nil => Vec::new(),
                other => other.to_string().into_bytes(),
            };
            stack.clear();
            stack.push_back(reply_table(ctx, b"err", &msg));
            Ok(CallbackReturn::Return)
        });
        let error_reply_key = Value::from(ctx.intern(b"error_reply"));
        redis_table
            .set_value(&ctx, error_reply_key, error_reply_fn.into())
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;

        // redis.status_reply(msg) -> a Lua table {ok = msg} that becomes a
        // status reply when returned from a script.
        let status_reply_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            let msg = match stack.get(0) {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::Integer(n) => n.to_string().into_bytes(),
                Value::Number(f) => f.to_string().into_bytes(),
                Value::Nil => Vec::new(),
                other => other.to_string().into_bytes(),
            };
            stack.clear();
            stack.push_back(reply_table(ctx, b"ok", &msg));
            Ok(CallbackReturn::Return)
        });
        let status_reply_key = Value::from(ctx.intern(b"status_reply"));
        redis_table
            .set_value(&ctx, status_reply_key, status_reply_fn.into())
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;

        ctx.set_global("redis", redis_table)?;

        // Add `tonumber` — piccolo's core stdlib omits it, but Redis/Lua scripts
        // rely on it heavily (e.g. `tonumber(ARGV[1])`).
        let tonumber_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            if stack.is_empty() {
                return Err(PicError::from(Value::String(ctx.intern(
                    b"bad argument #1 to 'tonumber' (value expected)",
                ))));
            }
            let val: Value = stack.consume(ctx)?;
            match val {
                Value::Integer(n) => {
                    stack.push_back(Value::Integer(n));
                }
                Value::Number(n) => {
                    stack.push_back(Value::Integer(n as i64));
                }
                Value::String(s) => {
                    let s = s.as_bytes();
                    // Skip leading/trailing whitespace
                    let mut start = 0;
                    while start < s.len() && s[start].is_ascii_whitespace() {
                        start += 1;
                    }
                    let mut end = s.len();
                    while end > start && s[end - 1].is_ascii_whitespace() {
                        end -= 1;
                    }
                    let s = &s[start..end];
                    // Handle optional sign
                    let (neg, s) = if s.first() == Some(&b'-') {
                        (true, &s[1..])
                    } else if s.first() == Some(&b'+') {
                        (false, &s[1..])
                    } else {
                        (false, s)
                    };
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(n) = s.parse::<i64>() {
                            stack.push_back(Value::Integer(if neg { -n } else { n }));
                        } else if let Ok(n) = s.parse::<f64>() {
                            let n = if neg { -n } else { n };
                            stack.push_back(Value::Number(n));
                        } else {
                            stack.push_back(Value::Nil);
                        }
                    } else {
                        stack.push_back(Value::Nil);
                    }
                }
                Value::Boolean(b) => {
                    stack.push_back(Value::Integer(if b { 1 } else { 0 }));
                }
                _ => {
                    stack.push_back(Value::Nil);
                }
            }
            Ok(CallbackReturn::Return)
        });
        ctx.set_global("tonumber", Value::from(tonumber_fn))?;

        // Add `unpack` as a top-level global (alias for table.unpack).
        // Lua 5.1 had `unpack` as a global; Lua 5.2+ moved it to table.unpack.
        // BullMQ scripts and many Redis scripts use the bare `unpack` form.
        let table_val = ctx.get_global("table");
        let unpack_fn = match table_val {
            Value::Table(table) => {
                let unpack_key = Value::from(ctx.intern(b"unpack"));
                table.get_value(unpack_key)
            }
            _ => Value::Nil,
        };
        ctx.set_global("unpack", unpack_fn)?;

        // Supplement the `table` global with standard Lua functions missing from
        // piccolo's core (which only provides pack/unpack). BullMQ and many other
        // Redis scripts depend on insert, remove, sort, concat, and move.
        if let Value::Table(table) = table_val {
            let insert_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
                // table.insert(table, [pos,] value)
                let nargs = stack.len();
                if nargs < 2 {
                    return Err(PicError::from(Value::String(ctx.intern(
                        b"table.insert: wrong number of arguments",
                    ))));
                }
                let tbl = match stack.get(0) {
                    Value::Table(t) => t,
                    _ => {
                        return Err(PicError::from(Value::String(ctx.intern(
                            b"table.insert: bad argument #1 (table expected)",
                        ))));
                    }
                };
                if nargs == 2 {
                    // table.insert(tbl, value) — append at end
                    let len = tbl.length();
                    let val = stack.get(1);
                    tbl.set_value(&ctx, Value::Integer(len + 1), val)
                        .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;
                } else {
                    // table.insert(tbl, pos, value)
                    let pos = match stack.get(1) {
                        Value::Integer(n) => n,
                        _ => {
                            return Err(PicError::from(Value::String(ctx.intern(
                                b"table.insert: bad argument #2 (number expected)",
                            ))));
                        }
                    };
                    let val = stack.get(2);
                    let len = tbl.length();
                    for i in (pos..=len).rev() {
                        let v = tbl.get_value(Value::Integer(i));
                        tbl.set_value(&ctx, Value::Integer(i + 1), v).map_err(|e| {
                            PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                        })?;
                    }
                    tbl.set_value(&ctx, Value::Integer(pos), val).map_err(|e| {
                        PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                    })?;
                }
                stack.clear();
                Ok(CallbackReturn::Return)
            });
            table.set(ctx, "insert", insert_fn).map_err(|e| {
                PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
            })?;

            let remove_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
                // table.remove(tbl [, pos])
                let tbl = match stack.get(0) {
                    Value::Table(t) => t,
                    _ => {
                        return Err(PicError::from(Value::String(ctx.intern(
                            b"table.remove: bad argument #1 (table expected)",
                        ))));
                    }
                };
                let len = tbl.length();
                let pos = match stack.get(1) {
                    Value::Integer(n) => n,
                    Value::Nil => len,
                    _ => len,
                };
                let removed = tbl.get_value(Value::Integer(pos));
                for i in pos..len {
                    let v = tbl.get_value(Value::Integer(i + 1));
                    tbl.set_value(&ctx, Value::Integer(i), v).map_err(|e| {
                        PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                    })?;
                }
                tbl.set_value(&ctx, Value::Integer(len), Value::Nil).map_err(|e| {
                    PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                })?;
                stack.clear();
                stack.push_back(removed);
                Ok(CallbackReturn::Return)
            });
            table.set(ctx, "remove", remove_fn).map_err(|e| {
                PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
            })?;

            let sort_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
                let tbl = match stack.get(0) {
                    Value::Table(t) => t,
                    _ => {
                        return Err(PicError::from(Value::String(ctx.intern(
                            b"table.sort: bad argument #1 (table expected)",
                        ))));
                    }
                };
                stack.clear();
                let len = tbl.length();
                if len < 2 {
                    return Ok(CallbackReturn::Return);
                }
                let mut vals: Vec<Value> = (1..=len)
                    .map(|i| tbl.get_value(Value::Integer(i)))
                    .collect();
                // Insertion sort (no comparator support — Lua comparators require
                // piccolo executor access which isn't available in callbacks).
                // Redis scripts rarely sort with comparators.
                vals.sort_by(|a, b| {
                    let sa = a.to_string();
                    let sb = b.to_string();
                    sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
                });
                for (i, v) in vals.into_iter().enumerate() {
                    tbl.set_value(&ctx, Value::Integer(i as i64 + 1), v).map_err(|e| {
                        PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                    })?;
                }
                Ok(CallbackReturn::Return)
            });
            table.set(ctx, "sort", sort_fn).map_err(|e| {
                PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
            })?;

            let concat_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
                let tbl = match stack.get(0) {
                    Value::Table(t) => t,
                    _ => {
                        return Err(PicError::from(Value::String(ctx.intern(
                            b"table.concat: bad argument #1 (table expected)",
                        ))));
                    }
                };
                let sep_str = match stack.get(1) {
                    Value::String(s) => s.as_bytes().to_vec(),
                    _ => Vec::new(),
                };
                let start = match stack.get(2) {
                    Value::Integer(n) => n,
                    _ => 1,
                };
                let end = match stack.get(3) {
                    Value::Integer(n) => n,
                    _ => tbl.length(),
                };
                stack.clear();
                let mut result = Vec::new();
                for idx in start..=end {
                    let v = tbl.get_value(Value::Integer(idx));
                    match v {
                        Value::String(s) => result.extend_from_slice(s.as_bytes()),
                        Value::Integer(n) => result.extend_from_slice(n.to_string().as_bytes()),
                        Value::Number(f) => result.extend_from_slice(f.to_string().as_bytes()),
                        Value::Boolean(b) => {
                            result.extend_from_slice(if b { b"true" } else { b"false" });
                        }
                        Value::Nil => {}
                        _ => {}
                    }
                    if idx < end && !sep_str.is_empty() {
                        result.extend_from_slice(&sep_str);
                    }
                }
                stack.push_back(Value::from(ctx.intern(&result)));
                Ok(CallbackReturn::Return)
            });
            table.set(ctx, "concat", concat_fn).map_err(|e| {
                PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
            })?;
        }

        // Add `string.match` — piccolo's core stdlib omits it, but Redis scripts
        // rely on it (e.g. BullMQ's destructureJobKey: string.match(jobKey, ".*:(.*)")).
        if let Value::Table(string_table) = ctx.get_global("string") {
            let match_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
                let subj = match stack.get(0) {
                    Value::String(s) => s.as_bytes().to_vec(),
                    other => other.to_string().into_bytes(),
                };
                let pat = match stack.get(1) {
                    Value::String(s) => s.as_bytes().to_vec(),
                    other => other.to_string().into_bytes(),
                };
                stack.clear();
                match lua_pattern_match(&subj, &pat) {
                    None => {
                        stack.push_back(Value::Nil);
                    }
                    Some(m) => {
                        if m.captures.is_empty() {
                            let slice = &subj[m.whole.0..m.whole.1];
                            stack.push_back(Value::from(ctx.intern(slice)));
                        } else {
                            // Push each capture as a separate return value,
                            // matching real Lua `string.match`.
                            for cap in &m.captures {
                                match cap {
                                    Some((s, e)) => {
                                        let slice = &subj[*s..*e];
                                        stack.push_back(Value::from(ctx.intern(slice)));
                                    }
                                    None => stack.push_back(Value::Nil),
                                }
                            }
                        }
                    }
                }
                Ok(CallbackReturn::Return)
            });
            string_table.set(ctx, "match", match_fn).map_err(|e| {
                PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
            })?;
        }

        // Add `cmsgpack` table with `unpack` (MessagePack decoder).
        // BullMQ uses cmsgpack.unpack(ARGV[n]) to deserialize job options.
        let cmsgpack_table = piccolo::Table::new(&ctx);
        let cmsgpack_unpack_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            let data: Value = stack.consume(ctx)?;
            match data {
                Value::Nil => {
                    stack.push_back(Value::Nil);
                    Ok(CallbackReturn::Return)
                }
                Value::String(s) => {
                    let bytes = s.as_bytes();
                    let decoded = msgpack_to_lua(ctx, bytes)?;
                    stack.push_back(decoded);
                    Ok(CallbackReturn::Return)
                }
                _ => {
                    // Non-string input: try to_string it first
                    let s = data.to_string();
                    let bytes = s.into_bytes();
                    let decoded = msgpack_to_lua(ctx, &bytes)?;
                    stack.push_back(decoded);
                    Ok(CallbackReturn::Return)
                }
            }
        });
        cmsgpack_table
            .set_value(
                &ctx,
                Value::from(ctx.intern(b"unpack")),
                cmsgpack_unpack_fn.into(),
            )
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;
        ctx.set_global("cmsgpack", cmsgpack_table)?;

        // Add `cjson` table with `encode` (JSON encoder).
        // BullMQ uses cjson.encode(opts) and cjson.encode(parent) in job scripts.
        let cjson_table = piccolo::Table::new(&ctx);
        let cjson_encode_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            let val: Value = stack.consume(ctx)?;
            let json = piccolo_to_json(ctx, val)?;
            let json_str = serde_json::to_string(&json)
                .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;
            stack.push_back(Value::String(ctx.intern(json_str.as_bytes())));
            Ok(CallbackReturn::Return)
        });
        cjson_table
            .set_value(
                &ctx,
                Value::from(ctx.intern(b"encode")),
                cjson_encode_fn.into(),
            )
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;

        // `cjson.decode` — JSON decoder, a companion to `cjson.encode`.
        // BullMQ's moveToFinished/finished paths call cjson.decode(parent) to
        // recover the parent's id/queueKey when a job completes.
        let cjson_decode_fn = Callback::from_fn(&ctx, |ctx, _exec, mut stack| {
            let val: Value = stack.consume(ctx)?;
            let json_str = match val {
                Value::Nil => {
                    stack.push_back(Value::Nil);
                    return Ok(CallbackReturn::Return);
                }
                Value::String(s) => String::from_utf8_lossy(s.as_bytes()).to_string(),
                other => other.to_string(),
            };
            let json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                PicError::from(Value::String(ctx.intern(
                    format!("cjson.decode: {}", e).as_bytes(),
                )))
            })?;
            let decoded = json_to_lua(ctx, json)?;
            stack.push_back(decoded);
            Ok(CallbackReturn::Return)
        });
        cjson_table
            .set_value(
                &ctx,
                Value::from(ctx.intern(b"decode")),
                cjson_decode_fn.into(),
            )
            .map_err(|e| PicError::from(Value::String(ctx.intern(e.to_string().as_bytes()))))?;
        ctx.set_global("cjson", cjson_table)?;

        run_executor(ctx, closure)
    }) {
        Ok(result) => {
            // The script succeeded; hand its deferred claims to the caller so
            // they can be woken once the containing transaction commits.
            let claims = std::mem::take(&mut *script_ctx.deferred_claims.lock().unwrap());
            Ok((result, claims))
        }
        Err(err) => {
            // The script failed; any XADD/ZADD claims made before the failure
            // must go back to the front of their queues — the reader gets nil
            // and re-issues, avoiding a lost wake.
            let claims = std::mem::take(&mut *script_ctx.deferred_claims.lock().unwrap());
            for claim in &claims {
                registry.release_front(claim);
            }
            Err(DbError::Redis(format!(
                "Error running script (new script): {}",
                static_error_message(&err)
            )))
        }
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

/// Decodes a MessagePack binary payload into a piccolo Lua value.
fn msgpack_to_lua<'gc>(
    ctx: Context<'gc>,
    data: &[u8],
) -> Result<Value<'gc>, PicError<'gc>> {
    let val = rmpv::decode::read_value(&mut std::io::Cursor::new(data)).map_err(|e| {
        PicError::from(Value::String(ctx.intern(
            format!("cmsgpack.unpack: {}", e).as_bytes(),
        )))
    })?;

    mp_value_to_lua(ctx, val)
}

fn mp_value_to_lua<'gc>(
    ctx: Context<'gc>,
    val: rmpv::Value,
) -> Result<Value<'gc>, PicError<'gc>> {
    match val {
        rmpv::Value::Nil => Ok(Value::Nil),
        rmpv::Value::Boolean(b) => Ok(Value::Boolean(b)),
        rmpv::Value::Integer(i) => {
            let n = i.as_i64().unwrap_or(0);
            Ok(Value::Integer(n))
        }
        rmpv::Value::F32(f) => Ok(Value::Number(f as f64)),
        rmpv::Value::F64(f) => Ok(Value::Number(f)),
        rmpv::Value::String(s) => {
            let bytes = s.into_bytes();
            Ok(Value::from(ctx.intern(&bytes)))
        }
        rmpv::Value::Binary(b) => Ok(Value::from(ctx.intern(&b))),
        rmpv::Value::Array(arr) => {
            let table = piccolo::Table::new(&ctx);
            for (i, item) in arr.into_iter().enumerate() {
                let key = Value::Integer(i as i64 + 1);
                let value = mp_value_to_lua(ctx, item)?;
                table.set_value(&ctx, key, value).map_err(|e| {
                    PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                })?;
            }
            Ok(Value::Table(table))
        }
        rmpv::Value::Map(map) => {
            let table = piccolo::Table::new(&ctx);
            for (k, v) in map {
                let key = mp_value_to_lua(ctx, k)?;
                let value = mp_value_to_lua(ctx, v)?;
                table.set_value(&ctx, key, value).map_err(|e| {
                    PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                })?;
            }
            Ok(Value::Table(table))
        }
        rmpv::Value::Ext(_, _) => {
            // MessagePack extension types — return raw binary as a fallback
            Ok(Value::Nil)
        }
    }
}

/// Converts a piccolo `Value` to a `serde_json::Value` for `cjson.encode`.
/// Tables with contiguous integer keys 1..n are encoded as JSON arrays;
/// all other tables are encoded as JSON objects.
fn piccolo_to_json<'gc>(
    _ctx: Context<'gc>,
    val: Value<'gc>,
) -> Result<serde_json::Value, PicError<'gc>> {
    match val {
        Value::Nil => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        Value::Integer(n) => Ok(serde_json::Value::Number(n.into())),
        Value::Number(n) => {
            let num = serde_json::Number::from_f64(n)
                .unwrap_or(serde_json::Number::from(0));
            Ok(serde_json::Value::Number(num))
        }
        Value::String(s) => Ok(serde_json::Value::String(
            String::from_utf8_lossy(s.as_bytes()).to_string(),
        )),
        Value::Table(table) => {
            let len = table.length();
            if len > 0 {
                // Check if it's a sequential array (keys 1..len all present)
                let mut arr = Vec::with_capacity(len as usize);
                for i in 1..=len {
                    let v = table.get_value(Value::Integer(i));
                    arr.push(piccolo_to_json(_ctx, v)?);
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                // Object with string keys
                let mut map = serde_json::Map::new();
                let mut key = Value::Nil;
                while let piccolo::table::NextValue::Found { key: k, value: v } =
                    table.next(key)
                {
                    let json_val = piccolo_to_json(_ctx, v)?;
                    match k {
                        Value::String(s) => {
                            map.insert(
                                String::from_utf8_lossy(s.as_bytes()).to_string(),
                                json_val,
                            );
                        }
                        Value::Integer(n) => {
                            map.insert(n.to_string(), json_val);
                        }
                        _ => {
                            let key_str = k.to_string();
                            map.insert(key_str, json_val);
                        }
                    }
                    key = k;
                }
                Ok(serde_json::Value::Object(map))
            }
        }
        _ => Ok(serde_json::Value::Null),
    }
}

/// Converts a `serde_json::Value` to a piccolo Lua value for `cjson.decode`,
/// mirroring `msgpack_to_lua`. JSON objects become Lua tables with string keys,
/// arrays become Lua sequence tables.
fn json_to_lua<'gc>(
    ctx: Context<'gc>,
    json: serde_json::Value,
) -> Result<Value<'gc>, PicError<'gc>> {
    match json {
        serde_json::Value::Null => Ok(Value::Nil),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Number(f))
            } else {
                Ok(Value::Number(0.0))
            }
        }
        serde_json::Value::String(s) => Ok(Value::from(ctx.intern(s.as_bytes()))),
        serde_json::Value::Array(arr) => {
            let table = piccolo::Table::new(&ctx);
            for (i, item) in arr.into_iter().enumerate() {
                let key = Value::Integer(i as i64 + 1);
                let value = json_to_lua(ctx, item)?;
                table.set_value(&ctx, key, value).map_err(|e| {
                    PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                })?;
            }
            Ok(Value::Table(table))
        }
        serde_json::Value::Object(map) => {
            let table = piccolo::Table::new(&ctx);
            for (k, v) in map {
                let value = json_to_lua(ctx, v)?;
                table
                    .set_value(&ctx, Value::from(ctx.intern(k.as_bytes())), value)
                    .map_err(|e| {
                        PicError::from(Value::String(ctx.intern(e.to_string().as_bytes())))
                    })?;
            }
            Ok(Value::Table(table))
        }
    }
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
        Value::Boolean(b) => Ok(if b {
            EvalResult::Integer(1)
        } else {
            EvalResult::Nil
        }),
        Value::Integer(n) => Ok(EvalResult::Integer(n)),
        Value::Number(f) => Ok(EvalResult::Integer(f as i64)),
        Value::String(s) => Ok(EvalResult::Bulk(s.as_bytes().to_vec())),
        Value::Table(t) => table_to_result(ctx, t),
        _ => Err(PicError::from(Value::String(ctx.intern(
            b"Script returned an unsupported value type".as_slice(),
        )))),
    }
}

/// Extracts a byte slice from a value used as an `ok`/`err` reply payload.
fn reply_payload<'gc>(ctx: Context<'gc>, value: Value<'gc>) -> Result<Vec<u8>, PicError<'gc>> {
    match value {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Integer(n) => Ok(n.to_string().into_bytes()),
        Value::Number(f) => Ok(f.to_string().into_bytes()),
        Value::Boolean(b) => Ok((if b { "true" } else { "false" }).to_string().into_bytes()),
        _ => Err(PicError::from(Value::String(ctx.intern(
            b"table has no integer keys".as_slice(),
        )))),
    }
}

/// Converts a Lua table to [`EvalResult`]. A table carrying an `ok` or `err`
/// string field represents a status or error reply (produced by
/// `redis.status_reply`/`redis.error_reply`). Otherwise a non-empty sequence is
/// converted element-wise and a hash-only table is converted to an array of
/// interleaved value, key pairs, mirroring Redis.
fn table_to_result<'gc>(
    ctx: Context<'gc>,
    table: piccolo::Table<'gc>,
) -> Result<EvalResult, PicError<'gc>> {
    // `{err = "..."}` / `{ok = "..."}` reply tables.
    let err_val = table.get(ctx, "err");
    if !matches!(err_val, Value::Nil) {
        return Ok(EvalResult::Err(reply_payload(ctx, err_val)?));
    }
    let ok_val = table.get(ctx, "ok");
    if !matches!(ok_val, Value::Nil) {
        return Ok(EvalResult::Status(reply_payload(ctx, ok_val)?));
    }

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
        EvalResult::Status(msg) => RespValue::SimpleString(Bytes::copy_from_slice(msg)),
        EvalResult::Err(msg) => RespValue::Error(Bytes::copy_from_slice(msg)),
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
        let session = test_session();
        eval(
            Bytes::copy_from_slice(script.as_bytes()),
            keys.iter()
                .map(|k| Bytes::copy_from_slice(k.as_bytes()))
                .collect(),
            argv.iter()
                .map(|a| Bytes::copy_from_slice(a.as_bytes()))
                .collect(),
            session.store(),
            session.registry(),
            session.current_db(),
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

        // Redis ≥6 maps Lua `false` replies to the null bulk string.
        let reply = exec(eval_op("return false", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
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

    #[tokio::test]
    async fn redis_call_set_and_get() {
        let reply = exec(eval_op(
            "redis.call('SET', 'testkey', 'testval'); return redis.call('GET', 'testkey')",
            &[],
            &[],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"testval".as_slice()));
    }

    #[tokio::test]
    async fn redis_call_returns_integer() {
        let reply = exec(eval_op(
            "redis.call('SET', 'counter', '10'); return redis.call('INCR', 'counter')",
            &[],
            &[],
        ))
        .await;
        assert_eq!(expect_integer(&reply), 11);
    }

    #[tokio::test]
    async fn redis_call_returns_nil_for_missing_key() {
        let reply = exec(eval_op("return redis.call('GET', 'nonexistent')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn redis_call_with_multiple_args() {
        let reply = exec(eval_op(
            "redis.call('SET', 'key1', 'val1'); redis.call('SET', 'key2', 'val2'); return {redis.call('GET', 'key1'), redis.call('GET', 'key2')}",
            &[],
            &[],
        ))
        .await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_bulk(&items[0]).as_deref(), Some(b"val1".as_slice()));
        assert_eq!(expect_bulk(&items[1]).as_deref(), Some(b"val2".as_slice()));
    }

    #[tokio::test]
    async fn redis_call_error_propagates() {
        let reply = exec(eval_op("redis.call('ECHO')", &[], &[])).await;
        expect_error_contains(&reply, "ERR wrong number of arguments");
    }

    #[tokio::test]
    async fn redis_pcall_catches_error() {
        let reply = exec(eval_op(
            "local ok, err = pcall(redis.call, 'ECHO'); return {ok, err}",
            &[],
            &[],
        ))
        .await;
        let items = expect_array(&reply);
        // pcall failure yields Lua `false`, which Redis ≥6 replies as null bulk.
        assert_eq!(expect_bulk(&items[0]), None);
        // Error message should be present
    }

    #[tokio::test]
    async fn redis_call_with_lua_arguments() {
        let reply = exec(eval_op(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            &["mykey"],
            &["myvalue"],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"myvalue".as_slice()));
    }

    #[tokio::test]
    async fn string_match_captures_group_after_last_colon() {
        let reply = exec(eval_op(
            "return string.match('bull:job:123', '.*:(.*)')",
            &[],
            &[],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"123".as_slice()));
    }

    #[tokio::test]
    async fn string_match_anchored_and_classes() {
        let reply = exec(eval_op(
            "return string.match('abc123def', '^%a+%d+')",
            &[],
            &[],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"abc123".as_slice()));
    }

    #[tokio::test]
    async fn string_match_single_value_when_no_captures() {
        let reply = exec(eval_op("return string.match('hello world', 'o w')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"o w".as_slice()));
    }

    #[tokio::test]
    async fn string_match_escaped_literal_dot() {
        let reply = exec(eval_op("return string.match('a.b', 'a%.b')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"a.b".as_slice()));

        // `.` unescaped matches any char, so it also matches a.b.
        let reply = exec(eval_op("return string.match('axb', 'a.b')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"axb".as_slice()));
    }

    #[tokio::test]
    async fn string_match_empty_quantifier_and_optional() {
        // `?` makes the preceding class optional.
        let reply = exec(eval_op("return string.match('color', 'colou?r')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"color".as_slice()));

        let reply = exec(eval_op("return string.match('colour', 'colou?r')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"colour".as_slice()));

        // A string without the optional-u of "color"/"colour" does not match.
        let reply = exec(eval_op("return string.match('colr', 'colou?r')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn string_match_no_match_returns_nil() {
        let reply = exec(eval_op("return string.match('foo', 'bar')", &[], &[])).await;
        assert_eq!(expect_bulk(&reply), None);
    }

    #[tokio::test]
    async fn string_match_multiple_captures() {
        let reply = exec(eval_op(
            "local a, b = string.match('12:34', '(%d+):(%d+)'); return {a, b}",
            &[],
            &[],
        ))
        .await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_bulk(&items[0]).as_deref(), Some(b"12".as_slice()));
        assert_eq!(expect_bulk(&items[1]).as_deref(), Some(b"34".as_slice()));
    }

    #[tokio::test]
    async fn cjson_decode_object_returns_string_keyed_table() {
        let reply = exec(eval_op(
            "local o = cjson.decode('{\"id\":\"a1\",\"queueKey\":\"bull:q\"}'); return {o['id'], o['queueKey']}",
            &[],
            &[],
        ))
        .await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_bulk(&items[0]).as_deref(), Some(b"a1".as_slice()));
        assert_eq!(
            expect_bulk(&items[1]).as_deref(),
            Some(b"bull:q".as_slice())
        );
    }

    #[tokio::test]
    async fn cjson_decode_array() {
        let reply = exec(eval_op(
            "local a = cjson.decode('[1,2,3]'); return {a[1], a[2], a[3]}",
            &[],
            &[],
        ))
        .await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 3);
        assert_eq!(expect_integer(&items[0]), 1);
        assert_eq!(expect_integer(&items[1]), 2);
        assert_eq!(expect_integer(&items[2]), 3);
    }

    #[tokio::test]
    async fn redis_type_call_ok_index() {
        let reply = exec(eval_op(
            "redis.call('SET', KEYS[1], 'x'); local t = redis.call('TYPE', KEYS[1])['ok']; return t",
            &["mykey"],
            &[],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"string".as_slice()));
    }

    #[tokio::test]
    async fn redis_node_type_ok_index() {
        let reply = exec(eval_op(
            "local t = redis.call('TYPE', KEYS[1])['ok']; return t",
            &["doesnotexist"],
            &[],
        ))
        .await;
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"none".as_slice()));
    }

    #[tokio::test]
    async fn redis_status_reply_returns_status() {
        let reply = exec(eval_op(
            "return redis.status_reply('everything ok')",
            &[],
            &[],
        ))
        .await;
        match reply {
            RespValue::SimpleString(s) => {
                assert_eq!(s.as_ref(), b"everything ok");
            }
            other => panic!("expected simple string, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn redis_error_reply_returns_error() {
        let reply = exec(eval_op(
            "return redis.error_reply('something bad')",
            &[],
            &[],
        ))
        .await;
        match reply {
            RespValue::Error(s) => {
                assert_eq!(s.as_ref(), b"something bad");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn redis_error_reply_used_inside_script_propagates() {
        let reply = exec(eval_op(
            "local ok, e = pcall(function() return redis.error_reply('nope') end); return e and e['err'] or 'no-err'",
            &[],
            &[],
        ))
        .await;
        // error_reply just builds a table; it does not raise. The table's err
        // field should be accessible.
        assert_eq!(expect_bulk(&reply).as_deref(), Some(b"nope".as_slice()));
    }

    #[tokio::test]
    async fn redis_call_lrange_from_script() {
        let op = eval_op(
            "redis.call('RPUSH', KEYS[1], 'a', 'b', 'c'); return redis.call('LRANGE', KEYS[1], 0, 2)",
            &["mylist"],
            &[],
        );
        let reply = exec(op).await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 3);
        assert_eq!(expect_bulk(&items[0]).as_deref(), Some(b"a".as_slice()));
        assert_eq!(expect_bulk(&items[1]).as_deref(), Some(b"b".as_slice()));
        assert_eq!(expect_bulk(&items[2]).as_deref(), Some(b"c".as_slice()));
    }

    #[tokio::test]
    async fn redis_call_lrange_string_args_from_script() {
        let op = eval_op(
            "redis.call('RPUSH', KEYS[1], 'a', 'b', 'c'); local k = KEYS[1]; local start = ARGV[1]; local stop = ARGV[2]; return redis.call('LRANGE', k, start, stop)",
            &["mylist"],
            &["0", "1"],
        );
        let reply = exec(op).await;
        let items = expect_array(&reply);
        assert_eq!(items.len(), 2);
        assert_eq!(expect_bulk(&items[0]).as_deref(), Some(b"a".as_slice()));
        assert_eq!(expect_bulk(&items[1]).as_deref(), Some(b"b".as_slice()));
    }
}
