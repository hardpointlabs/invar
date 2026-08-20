//! Redis command dispatch: routes a decoded command array to its handler,
//! enforcing the `MULTI`/`EXEC`/`DISCARD` lifecycle, and flushes any queued
//! operations through the session's transaction dispatcher.

use bytes::Bytes;

use crate::bitmap;
use crate::bloom;
use crate::common::op::{NoOp, WireOp};
use crate::common::{DbError, DbResult, QueuedOp};
use crate::common::session::Session;
use crate::conn;
use crate::hash;
use crate::hll;
use crate::json;
use crate::keys;
use crate::list;
use crate::resp::{ok_resp, RespValue};
use crate::server;
use crate::set;
use crate::strings;
use crate::zset;
use crate::pubsub;
use crate::script;

pub async fn enqueue_command(session: &mut Session, args: &[Bytes]) -> Vec<RespValue> {
    // short-circuit in the case of a command not allowed in a script
    if let Some(noscript_response) = dispatch_noscript(session, args).await {
        return noscript_response;
    }

    let cmd = dispatch_command(session, args);

    // Error ops abort the MULTI immediately without being enqueued.
    // error_op() already called mark_dirty(); return the original error reply.
    if session.in_multi() && cmd.abort_in_tx {
        let reply = cmd.wire_op.reply(Ok(Box::new(())));
        return vec![reply];
    }

    if session.in_multi() && !cmd.allowed_in_tx {
        return vec![error(session, "Command not allowed inside a transaction")];
    }

    let mut replies = Vec::new();

    if let Some(queued) = session.enqueue_op(cmd) {
        replies.push(queued);
    }

    replies.extend(session.dispatch_pending_ops(false).await);
    replies
}

pub async fn dispatch_noscript(session: &mut Session, args: &[Bytes]) -> Option<Vec<RespValue>> {
    let Some(name) = args.first() else {
        return Some(vec![error(session, "ERR empty command")]);
    };
    let name: Vec<u8> = name.iter().map(u8::to_ascii_lowercase).collect();
    match name.as_slice() {
        b"multi" => {
            if session.in_multi() {
                // Nested MULTI is an error but does NOT abort the outer
                // transaction, so it bypasses dirty tracking.
                Some(vec![RespValue::Error(Bytes::from_static(b"ERR MULTI calls can not be nested"))])
            } else {
                session.enter_multi();
                Some(vec![ok_resp()])
            }
        }
        b"exec" => match session.exit_multi(false) {
            Ok(()) => Some(session.dispatch_pending_ops(true).await),
            _ => Some(vec![error(session, "ERR EXEC without MULTI")]),
        },
        b"discard" => match session.exit_multi(true) {
            Ok(()) => Some(vec![ok_resp()]),
            _ => Some(vec![error(session, "DISCARD without MULTI")]),
        },
        _ => None
    }
}

/// Dispatches a single decoded command. Returns the RESP replies to write
/// back to the client (possibly multiple, e.g. pipelined queue replies).
pub fn dispatch_command(session: &mut Session, args: &[Bytes]) -> QueuedOp {
    let Some(name) = args.first() else {
        return error_op(session, "ERR empty command");
    };
    let name: Vec<u8> = name.iter().map(u8::to_ascii_lowercase).collect();

    match name.as_slice() {
        b"ping" => match &args[1..] {
            [] => conn::ping(None),
            [msg] => conn::ping(Some(msg.clone())),
            _ => error_op(session, "ERR wrong number of arguments for 'ping' command"),
        },
        b"echo" => match &args[1..] {
            [msg] => conn::echo(msg.clone()),
            _ => error_op(session, "ERR wrong number of arguments for 'echo' command"),
        },
        b"eval" => {
            // EVAL script numkeys [key ...] [arg ...]
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'eval' command");
            }
            let script = args[1].clone();
            let Some(num_keys) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if num_keys < 0 {
                return error_op(session, "ERR number of keys can't be negative");
            }
            let num_keys = num_keys as usize;
            if args.len() < 3 + num_keys {
                return error_op(session, "ERR wrong number of arguments for 'eval' command");
            }
            let keys = args[3..3 + num_keys].to_vec();
            let argv = args[3 + num_keys..].to_vec();
            script::eval(
                script,
                keys,
                argv,
                session.store(),
                session.registry(),
                session.current_db(),
            )
        }
        b"select" => {
            match &args[1..] {
                [db] => match parse_i64(db) {
                    Some(db) if db >= 0 => {
                        session.switch_db(db as i32);
                        if session.in_multi() {
                            // Deferred: reply `+OK` at EXEC, matching the
                            // transactional semantics where each queued
                            // command yields one array element.
                            conn::ok_op()
                        } else {
                            ok()
                        }
                    }
                    _ => error_op(session, "ERR invalid DB index"),
                },
                _ => error_op(session, "ERR wrong number of arguments for 'select' command"),
            }
        }
        b"client" => {
            server::client(session, args)
        }
        b"info" => {
            server::info()
        }
        b"hello" => {
            server::hello(session, args)
        }
        b"sync" | b"psync" => {
            conn::sync()
        }
        b"wait" => {
            conn::wait()
        }
        b"lolwut" => {
            conn::lolwut(conn::VERSION, conn::COMMIT)
        }
        b"time" => {
            conn::time()
        }
        b"module" => {
            conn::module(args)
        }
        b"save" => {
            server::save(session)
        }
        b"bgsave" => {
            conn::bgsave(session)
        }
        b"dbsize" => {
            conn::dbsize(session)
        }
        b"quit" => {
            session.request_close();
            ok()
        }
        b"flushall" => {
            server::flushall(session)
        }
        b"flushdb" => {
            server::flushdb(session)
        }
        b"setbit" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'setbit' command");
            }
            let Some(offset) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if offset < 0 {
                return error_op(session, "ERR bit offset is not an integer or out of range");
            }
            let Some(value) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if value != 0 && value != 1 {
                return error_op(session, "ERR bit is not an integer or out of range");
            }
            bitmap::set_bit(session, &args[1], offset, value)
        }
        b"getbit" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'getbit' command");
            }
            let Some(offset) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if offset < 0 {
                return error_op(session, "ERR bit offset is not an integer or out of range");
            }
            bitmap::get_bit(session, &args[1], offset)
        }
        b"bitcount" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'bitcount' command");
            }
            let mut start_given = false;
            let mut end_given = false;
            let mut start_val = 0i64;
            let mut end_val = 0i64;
            let mut use_bit = false;
            let mut i = 2usize;
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                start_val = v;
                start_given = true;
                i += 1;
            }
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                end_val = v;
                end_given = true;
                i += 1;
            }
            if i < args.len() {
                let unit = args[i].to_ascii_lowercase();
                if unit == *b"bit" {
                    use_bit = true;
                } else if unit != *b"byte" {
                    return error_op(session, "ERR syntax error");
                }
                i += 1;
            }
            if i < args.len() {
                return error_op(session, "ERR syntax error");
            }
            if start_given != end_given {
                return error_op(session, "ERR syntax error");
            }
            bitmap::bit_count(
                session,
                &args[1],
                start_given,
                end_given,
                start_val,
                end_val,
                use_bit,
            )
        }
        b"bitpos" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'bitpos' command");
            }
            let Some(bit) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if bit != 0 && bit != 1 {
                return error_op(session, "ERR bit is not an integer or out of range");
            }
            let mut start_given = false;
            let mut start_val = 0i64;
            let mut end_val = 0i64;
            let mut use_bit = false;
            let mut i = 3usize;
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                start_val = v;
                start_given = true;
                i += 1;
            }
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                end_val = v;
                i += 1;
            }
            if i < args.len() {
                let unit = args[i].to_ascii_lowercase();
                if unit == *b"bit" {
                    use_bit = true;
                } else if unit != *b"byte" {
                    return error_op(session, "ERR syntax error");
                }
                i += 1;
            }
            if i < args.len() {
                return error_op(session, "ERR syntax error");
            }
            bitmap::bit_pos(
                session,
                &args[1],
                bit,
                start_given,
                start_val,
                end_val,
                use_bit,
            )
        }
        b"bitop" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'bitop' command");
            }
            let Some(op) = bitmap::parse_bit_op(&args[1]) else {
                return error_op(session, "ERR syntax error");
            };
            if op == bitmap::BitOpType::Not && args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'bitop' command");
            }
            let src_keys: Vec<&[u8]> = args[3..].iter().map(|b| b.as_ref()).collect();
            bitmap::bit_op(session, &args[2], op, &src_keys)
        }
        b"set" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'set' command");
            };
            strings::set(session, key, value)
        }
        b"get" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'get' command");
            }
            strings::get(session, &args[1])
        }
        b"setex" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'setex' command");
            }
            let Some(seconds) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::set_ex(session, &args[1], &args[3], seconds)
        }
        b"psetex" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'psetex' command");
            }
            let Some(ms) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::pset_ex(session, &args[1], &args[3], ms)
        }
        b"getset" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'getset' command");
            };
            strings::get_set(session, key, value)
        }
        b"getdel" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'getdel' command");
            }
            strings::get_del(session, &args[1])
        }
        b"strlen" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'strlen' command");
            }
            strings::strlen(session, &args[1])
        }
        b"substr" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'substr' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(end) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::substr(session, &args[1], start, end)
        }
        b"getrange" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'getrange' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(end) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::substr(session, &args[1], start, end)
        }
        b"setnx" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'setnx' command");
            };
            strings::set_nx(session, key, value)
        }
        b"append" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'append' command");
            };
            strings::append(session, key, value)
        }
        b"getex" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'getex' command");
            }
            strings::get_ex(session, &args[1..])
        }
        b"incrbyfloat" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'incrbyfloat' command");
            };
            let Some(amount) = parse_f64(amount) else {
                return error_op(session, "ERR value is not a float");
            };
            strings::incr_by_float(session, key, amount)
        }
        b"mset" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return error_op(session, "ERR wrong number of arguments for 'mset' command");
            }
            strings::mset(session, &args[1..])
        }
        b"msetnx" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return error_op(session, "ERR wrong number of arguments for 'msetnx' command");
            }
            strings::mset_nx(session, &args[1..])
        }
        b"setrange" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'setrange' command");
            }
            let Some(offset) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if offset < 0 {
                return error_op(session, "ERR offset is out of range");
            }
            strings::set_range(session, &args[1], offset, &args[3])
        }
        b"incr" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'incr' command");
            }
            strings::increment(session, &args[1], 1)
        }
        b"incrby" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'incrby' command");
            };
            let Some(amount) = parse_i64(amount) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::increment(session, key, amount)
        }
        b"decr" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'decr' command");
            }
            strings::increment(session, &args[1], -1)
        }
        b"decrby" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'decrby' command");
            };
            let Some(amount) = parse_i64(amount) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            strings::increment(session, key, -amount)
        }
        b"bf.reserve" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'bf.reserve' command");
            }
            let Some(err_rate) = parse_f64(&args[2]) else {
                return error_op(session, "ERR value is not a float");
            };
            let Some(capacity) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if capacity < 1 {
                return error_op(session, "ERR capacity must be positive");
            }
            let mut expansion = 2i64;
            let mut non_scaling = false;
            let mut i = 4;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"expansion") && i + 1 < args.len() {
                    i += 1;
                    match parse_i64(&args[i]) {
                        Some(v) => expansion = v,
                        None => {
                            return error_op(session, "ERR value is not an integer or out of range");
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"nonscaling") {
                    non_scaling = true;
                }
                i += 1;
            }
            bloom::reserve(
                session,
                &args[1],
                err_rate,
                capacity as u64,
                expansion,
                non_scaling,
            )
        }
        b"bf.add" => {
            let Some((key, item)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'bf.add' command");
            };
            bloom::add(session, key, item)
        }
        b"bf.exists" => {
            let Some((key, item)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'bf.exists' command");
            };
            bloom::exists(session, key, item)
        }
        b"bf.madd" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'bf.madd' command");
            }
            bloom::madd(session, &args[1], &args[2..])
        }
        b"bf.mexists" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'bf.mexists' command");
            }
            bloom::mexists(session, &args[1], &args[2..])
        }
        b"bf.insert" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'bf.insert' command");
            }
            let mut capacity = 0u64;
            let mut error_rate = 0.0f64;
            let mut expansion = 0i64;
            let mut no_create = false;
            let mut non_scaling = false;
            let mut items: Vec<Bytes> = Vec::new();
            let mut i = 2;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"capacity") && i + 1 < args.len() {
                    i += 1;
                    match parse_i64(&args[i]) {
                        Some(v) => capacity = v.max(0) as u64,
                        None => {
                            return error_op(session, "ERR value is not an integer or out of range");
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"error") && i + 1 < args.len() {
                    i += 1;
                    match parse_f64(&args[i]) {
                        Some(v) => error_rate = v,
                        None => {
                            return error_op(session, "ERR value is not a float");
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"expansion") && i + 1 < args.len() {
                    i += 1;
                    match parse_i64(&args[i]) {
                        Some(v) => expansion = v,
                        None => {
                            return error_op(session, "ERR value is not an integer or out of range");
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"nocreate") {
                    no_create = true;
                } else if args[i].eq_ignore_ascii_case(b"nonscaling") {
                    non_scaling = true;
                } else if args[i].eq_ignore_ascii_case(b"items") {
                    items = args.get(i + 1..).unwrap_or(&[]).to_vec();
                    break;
                } else {
                    return error_op(session, format!("ERR syntax error at {}", String::from_utf8_lossy(&args[i])),);
                }
                i += 1;
            }
            if items.is_empty() {
                return error_op(session, "ERR ITEMS argument required");
            }
            let info = bloom::InsertInfo {
                capacity,
                error: error_rate,
                expansion,
                no_create,
                non_scaling,
                items,
            };
            bloom::insert(session, &args[1], info)
        }
        b"bf.info" => {
            let Some(key) = args.get(1) else {
                return error_op(session, "ERR wrong number of arguments for 'bf.info' command");
            };
            bloom::info(session, key)
        }
        b"json.set" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.set' command");
            }
            let Some(value) = json::parse_json(&args[3]) else {
                return error_op(session, "ERR invalid JSON");
            };
            let mut nx = false;
            let mut xx = false;
            let mut ft = json::FphaType::None;
            let mut i = 4;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"nx") {
                    nx = true;
                } else if args[i].eq_ignore_ascii_case(b"xx") {
                    xx = true;
                } else if args[i].eq_ignore_ascii_case(b"fpha") {
                    if i + 1 >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    i += 1;
                    match json::parse_fpha(&args[i]) {
                        Some(parsed) => ft = parsed,
                        None => {
                            return error_op(session, "ERR syntax error");
                        }
                    }
                } else {
                    return error_op(session, "ERR syntax error");
                }
                i += 1;
            }
            if nx && xx {
                return error_op(session, "ERR NX and XX are mutually exclusive");
            }
            if ft != json::FphaType::None {
                if let Err(e) = json::validate_fpha(&value, ft) {
                    return error_op(session, e);
                }
            }
            json::set(session, &args[1], &args[2], value, nx, xx)
        }
        b"json.get" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.get' command");
            }
            let paths: Vec<String> = args[2..]
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            json::get(session, &args[1], paths)
        }
        b"json.del" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.del' command");
            }
            let paths: Vec<String> = args[2..]
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            json::del(session, &args[1], paths)
        }
        b"json.type" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.type' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::json_type(session, &args[1], &path)
        }
        b"json.arrappend" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrappend' command");
            }
            let mut values = Vec::with_capacity(args.len() - 3);
            for v in &args[3..] {
                match json::parse_json(v) {
                    Some(jv) => values.push(jv),
                    None => {
                        return error_op(session, "ERR invalid JSON");
                    }
                }
            }
            json::arr_append(session, &args[1], &args[2], values)
        }
        b"json.arrindex" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrindex' command");
            }
            let Some(value) = json::parse_json(&args[3]) else {
                return error_op(session, "ERR invalid JSON");
            };
            json::arr_index(session, &args[1], &args[2], value)
        }
        b"json.arrlen" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrlen' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::arr_len(session, &args[1], &path)
        }
        b"json.numincrby" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.numincrby' command");
            }
            let Some(delta) = parse_f64(&args[3]) else {
                return error_op(session, "ERR value is not a number");
            };
            json::num_incr_by(session, &args[1], &args[2], delta)
        }
        b"json.nummultby" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.nummultby' command");
            }
            let Some(factor) = parse_f64(&args[3]) else {
                return error_op(session, "ERR value is not a number");
            };
            json::num_mult_by(session, &args[1], &args[2], factor)
        }
        b"json.objkeys" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.objkeys' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::obj_keys(session, &args[1], &path)
        }
        b"json.objlen" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.objlen' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::obj_len(session, &args[1], &path)
        }
        b"json.strappend" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'json.strappend' command");
            }
            let (path, value_idx) = if args.len() == 4 {
                (args[2].clone(), 3)
            } else if args.len() == 3 {
                (Bytes::from_static(b"$"), 2)
            } else {
                (Bytes::from_static(b"$"), 3)
            };
            let Some(suffix) = json::parse_json_string(&args[value_idx]) else {
                return error_op(session, "ERR invalid JSON string");
            };
            json::str_append(session, &args[1], &path, suffix)
        }
        b"json.strlen" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.strlen' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::str_len(session, &args[1], &path)
        }
        b"json.mget" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'json.mget' command");
            }
            let last = args.len() - 1;
            let path = String::from_utf8_lossy(&args[last]).into_owned();
            let keys: Vec<Vec<u8>> = args[1..last].iter().map(|k| k.to_vec()).collect();
            json::mget(session, keys, path)
        }
        b"json.resp" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.resp' command");
            }
            let path = if args.len() >= 3 {
                String::from_utf8_lossy(&args[2]).into_owned()
            } else {
                String::new()
            };
            json::resp(session, &args[1], path)
        }
        b"json.clear" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.clear' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            json::clear(session, &args[1], &path)
        }
        b"json.arrpop" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrpop' command");
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            let mut idx = -1i64;
            if args.len() >= 4 {
                match parse_i64(&args[3]) {
                    Some(v) => idx = v,
                    None => {
                        return error_op(session, "value is not an integer or out of range");
                    }
                }
            }
            json::arr_pop(session, &args[1], &path, idx)
        }
        b"json.arrtrim" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrtrim' command");
            }
            let Some(start) = parse_i64(&args[3]) else {
                return error_op(session, "value is not an integer or out of range");
            };
            let mut stop = -1i64;
            if args.len() >= 5 {
                match parse_i64(&args[4]) {
                    Some(v) => stop = v,
                    None => {
                        return error_op(session, "value is not an integer or out of range");
                    }
                }
            }
            json::arr_trim(session, &args[1], &args[2], start, stop)
        }
        b"json.arrinsert" => {
            if args.len() < 5 {
                return error_op(session, "ERR wrong number of arguments for 'json.arrinsert' command");
            }
            let Some(index) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let mut values = Vec::with_capacity(args.len() - 4);
            for v in &args[4..] {
                match json::parse_json(v) {
                    Some(jv) => values.push(jv),
                    None => {
                        return error_op(session, "ERR invalid JSON");
                    }
                }
            }
            json::arr_insert(session, &args[1], &args[2], index, values)
        }
        b"pfadd" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'pfadd' command");
            }
            hll::pfadd(session, &args[1], &args[2..])
        }
        b"pfcount" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'pfcount' command");
            }
            hll::pfcount(session, &args[1..])
        }
        b"pfmerge" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'pfmerge' command");
            }
            hll::pfmerge(session, &args[1], &args[2..])
        }
        b"lpush" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'lpush' command");
            }
            list::lpush(session, &args[1], &args[2..])
        }
        b"rpush" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'rpush' command");
            }
            list::rpush(session, &args[1], &args[2..])
        }
        b"lpop" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'lpop' command");
            }
            list::lpop(session, &args[1])
        }
        b"rpop" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'rpop' command");
            }
            list::rpop(session, &args[1])
        }
        b"llen" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'llen' command");
            }
            list::llen(session, &args[1])
        }
        b"lrange" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'lrange' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            list::lrange(session, &args[1], start, stop)
        }
        b"lindex" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'lindex' command");
            }
            let Some(index) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            list::lindex(session, &args[1], index)
        }
        b"lset" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'lset' command");
            }
            let Some(index) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            list::lset(session, &args[1], index, &args[3])
        }
        b"lrem" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'lrem' command");
            }
            let Some(count) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            list::lrem(session, &args[1], count, &args[3])
        }
        b"ltrim" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'ltrim' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            list::ltrim(session, &args[1], start, stop)
        }
        b"linsert" => {
            if args.len() != 5 {
                return error_op(session, "ERR wrong number of arguments for 'linsert' command");
            }
            let before = args[2].eq_ignore_ascii_case(b"before");
            list::linsert(session, &args[1], before, &args[3], &args[4])
        }
        b"lpushx" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'lpushx' command");
            }
            list::lpushx(session, &args[1], &args[2])
        }
        b"rpushx" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'rpushx' command");
            }
            list::rpushx(session, &args[1], &args[2])
        }
        b"sadd" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'sadd' command");
            }
            set::sadd(session, &args[1], &args[2..])
        }
        b"srem" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'srem' command");
            }
            set::srem(session, &args[1], &args[2..])
        }
        b"scard" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'scard' command");
            }
            set::scard(session, &args[1])
        }
        b"smembers" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'smembers' command");
            }
            set::smembers(session, &args[1])
        }
        b"sismember" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'sismember' command");
            }
            set::sismember(session, &args[1], &args[2])
        }
        b"spop" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'spop' command");
            }
            set::spop(session, &args[1])
        }
        b"srandmember" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'srandmember' command");
            }
            set::srandmember(session, &args[1], 1)
        }
        b"smove" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'smove' command");
            }
            set::smove(session, &args[1], &args[2], &args[3])
        }
        b"sdiff" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'sdiff' command");
            }
            set::sdiff(session, &args[1..])
        }
        b"sinter" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'sinter' command");
            }
            set::sinter(session, &args[1..])
        }
        b"sunion" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'sunion' command");
            }
            set::sunion(session, &args[1..])
        }
        b"sdiffstore" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'sdiffstore' command");
            }
            set::sdiffstore(session, &args[1], &args[2..])
        }
        b"sinterstore" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'sinterstore' command");
            }
            set::sinterstore(session, &args[1], &args[2..])
        }
        b"sunionstore" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'sunionstore' command");
            }
            set::sunionstore(session, &args[1], &args[2..])
        }
        b"zadd" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zadd' command");
            }
            zset::zadd(session, &args[1], &args[2..])
        }
        b"zcard" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'zcard' command");
            }
            zset::zcard(session, &args[1])
        }
        b"zcount" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zcount' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            zset::zcount(session, &args[1], &min, &max)
        }
        b"zincrby" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zincrby' command");
            }
            let Some(incr) = parse_f64(&args[2]) else {
                return error_op(session, "ERR value is not a float");
            };
            zset::zincrby(session, &args[1], incr, &args[3])
        }
        b"zinter" | b"zinterstore" => {
            let is_store = name == b"zinterstore";
            if args.len() < 4 {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let arg_start = if is_store { 2 } else { 1 };
            let Some(num_keys) = parse_i64(&args[arg_start]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if num_keys < 0 {
                return error_op(session, "ERR value is not an integer or out of range");
            }
            let num_keys = num_keys as usize;
            if args.len() < arg_start + 1 + num_keys {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let keys = args[arg_start + 1..arg_start + 1 + num_keys].to_vec();
            let mut i = arg_start + 1 + num_keys;
            let mut weights: Vec<f64> = Vec::new();
            let mut aggregate = String::from("SUM");
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"weights") {
                    i += 1;
                    for _ in 0..num_keys {
                        if i >= args.len() {
                            break;
                        }
                        let Some(w) = parse_f64(&args[i]) else {
                            return error_op(session, "ERR value is not a float");
                        };
                        weights.push(w);
                        i += 1;
                    }
                    if weights.len() != num_keys {
                        return error_op(session, "ERR weight count does not match number of keys");
                    }
                } else if args[i].eq_ignore_ascii_case(b"aggregate") {
                    i += 1;
                    if i >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    aggregate = String::from_utf8_lossy(&args[i]).to_string();
                    if !args[i].eq_ignore_ascii_case(b"sum")
                        && !args[i].eq_ignore_ascii_case(b"min")
                        && !args[i].eq_ignore_ascii_case(b"max")
                    {
                        return error_op(session, "ERR syntax error");
                    }
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"withscores") && !is_store {
                    i += 1;
                } else {
                    return error_op(session, "ERR syntax error");
                }
            }
            if is_store {
                zset::zinterstore(session, &args[1], &aggregate, &weights, &keys)
            } else {
                let has_with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"withscores"));
                zset::zinter(
                    session,
                    &aggregate,
                    &weights,
                    has_with_scores,
                    &keys,
                )
            }
        }
        b"zlexcount" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zlexcount' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            zset::zlexcount(session, &args[1], &min, &max)
        }
        b"zpopmax" | b"zpopmin" => {
            let want_min = name == b"zpopmin";
            if args.len() < 2 {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let mut count = 1usize;
            if args.len() >= 3 {
                match parse_i64(&args[2]) {
                    Some(v) if v >= 0 => count = v as usize,
                    _ => {
                        return error_op(session, "ERR value is not an integer or out of range");
                    }
                }
            }
            if want_min {
                zset::zpopmin(session, &args[1], count)
            } else {
                zset::zpopmax(session, &args[1], count)
            }
        }
        b"bzpopmin" | b"bzpopmax" => {
            let want_min = name == b"bzpopmin";
            // BZPOPMIN key [key ...] timeout
            if args.len() < 3 {
                return error_op(session, format!("ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name)));
            }
            let timeout_arg = &args[args.len() - 1];
            let Some(timeout) = parse_f64(timeout_arg) else {
                return error_op(session, "ERR timeout is not a float or out of range");
            };
            if timeout < 0.0 {
                return error_op(session, "ERR timeout is not a float or out of range");
            }
            let keys: Vec<&[u8]> = args[1..args.len() - 1].iter().map(|b| b.as_ref()).collect();
            zset::bzpop(session, &keys, timeout, want_min)
        }
        b"exists" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'exists' command");
            }
            keys::exists(session, &args[1..])
        }
        b"mget" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'mget' command");
            }
            keys::mget(session, &args[1..])
        }
        b"object" => {
            // TODO this is hard-wired to only implement the IDLETIME subcommand stub
            // Replace if/when we decide to implement more OBJECT subcommands
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'object' command");
            }
            keys::idle_time(session)
        }
        b"move" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'move' command");
            }
            let Some(target_db) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if target_db < 0 {
                return error_op(session, "ERR invalid DB index");
            }
            keys::move_op(session, &args[1], target_db as i32)
        }
        b"rename" => {
            let Some((old_key, new_key)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'rename' command");
            };
            keys::rename(session, old_key, new_key)
        }
        b"renamenx" => {
            let Some((old_key, new_key)) = parse_pair(&args[1..]) else {
                return error_op(session, "ERR wrong number of arguments for 'renamenx' command");
            };
            keys::rename_nx(session, old_key, new_key)
        }
        b"pttl" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'pttl' command");
            }
            keys::pttl(session, &args[1])
        }
        b"ttl" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'ttl' command");
            }
            keys::ttl(session, &args[1])
        }
        b"expire" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'expire' command");
            }
            let Some(seconds) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            keys::expire(session, &args[1], seconds)
        }
        b"type" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'type' command");
            }
            keys::key_type(session, &args[1])
        }
        b"del" | b"unlink" => {
            if args.len() < 2 {
                let name = if name.as_slice() == b"del" {
                    "del"
                } else {
                    "unlink"
                };
                return error_op(session, format!("ERR wrong number of arguments for '{name}' command"),);
            }
            keys::del(session, &args[1..])
        }
        b"scan" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'scan' command");
            }
            let mut count = 10usize;
            let mut pattern: Option<Vec<u8>> = None;
            let mut type_filter: Option<u8> = None;
            let mut i = 2;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"match") {
                    if i + 1 >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    pattern = Some(args[i + 1].to_vec());
                    i += 2;
                } else if args[i].eq_ignore_ascii_case(b"count") {
                    if i + 1 >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    let Some(n) = parse_i64(&args[i + 1]) else {
                        return error_op(session, "ERR value is not an integer or out of range");
                    };
                    if n < 1 {
                        return error_op(session, "ERR syntax error");
                    }
                    count = n as usize;
                    i += 2;
                } else if args[i].eq_ignore_ascii_case(b"type") {
                    if i + 1 >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    // Unknown type names match nothing (Redis 7.x behaviour).
                    type_filter = keys::type_byte(&args[i + 1]);
                    i += 2;
                } else {
                    return error_op(session, "ERR syntax error");
                }
            }
            let pattern = match pattern {
                Some(p) if p == b"*" => None,
                other => other,
            };
            keys::scan(
                session,
                &args[1],
                count,
                pattern,
                type_filter,
            )
        }
        b"zrange" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrange' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let with_scores = args
                .get(4)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
           zset::zrange(session, &args[1], start, stop, with_scores)
        }
        b"zrangebylex" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrangebylex' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
                let Some(offset) = parse_i64(&args[5]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                let Some(count) = parse_i64(&args[6]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                limit_offset = offset;
                limit_count = count;
                has_limit = true;
            }
            zset::zrangebylex(
                session,
                &args[1],
                &min,
                &max,
                limit_offset,
                limit_count,
                has_limit,
            )
        }
        b"zrangebyscore" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrangebyscore' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            let mut with_scores = false;
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            let mut i = 4;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"withscores") {
                    with_scores = true;
                } else if args[i].eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
                    let Some(offset) = parse_i64(&args[i + 1]) else {
                        return error_op(session, "ERR value is not an integer or out of range");
                    };
                    let Some(count) = parse_i64(&args[i + 2]) else {
                        return error_op(session, "ERR value is not an integer or out of range");
                    };
                    limit_offset = offset;
                    limit_count = count;
                    has_limit = true;
                    i += 2;
                }
                i += 1;
            }
            let limit = if has_limit {
                Some((limit_offset, limit_count))
            } else {
                None
            };
            zset::zrangebyscore(
                session,
                &args[1],
                &min,
                &max,
                with_scores,
                limit,
            )
        }
        b"zrank" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'zrank' command");
            }
            zset::zrank(session, &args[1], &args[2])
        }
        b"zrem" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'zrem' command");
            }
            zset::zrem(session, &args[1], &args[2..])
        }
        b"zremrangebylex" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zremrangebylex' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            zset::zremrangebylex(session, &args[1], &min, &max)
        }
        b"zremrangebyrank" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zremrangebyrank' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            zset::zremrangebyrank(session, &args[1], start, stop)
        }
        b"zremrangebyscore" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'zremrangebyscore' command");
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            zset::zremrangebyscore(session, &args[1], &min, &max)
        }
        b"zrevrange" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrevrange' command");
            }
            let Some(start) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let with_scores = args
                .get(4)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
            zset::zrevrange(session, &args[1], start, stop, with_scores)
        }
        b"zrevrangebylex" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrevrangebylex' command");
            }
            let max = String::from_utf8_lossy(&args[2]).into_owned();
            let min = String::from_utf8_lossy(&args[3]).into_owned();
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
                let Some(offset) = parse_i64(&args[5]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                let Some(count) = parse_i64(&args[6]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                limit_offset = offset;
                limit_count = count;
                has_limit = true;
            }
            zset::zrevrangebylex(
                session,
                &args[1],
                &max,
                &min,
                limit_offset,
                limit_count,
                has_limit,
            )
        }
        b"zrevrangebyscore" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zrevrangebyscore' command");
            }
            let max = String::from_utf8_lossy(&args[2]).into_owned();
            let min = String::from_utf8_lossy(&args[3]).into_owned();
            let mut with_scores = false;
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            let mut i = 4;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"withscores") {
                    with_scores = true;
                } else if args[i].eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
                    let Some(offset) = parse_i64(&args[i + 1]) else {
                        return error_op(session, "ERR value is not an integer or out of range");
                    };
                    let Some(count) = parse_i64(&args[i + 2]) else {
                        return error_op(session, "ERR value is not an integer or out of range");
                    };
                    limit_offset = offset;
                    limit_count = count;
                    has_limit = true;
                    i += 2;
                }
                i += 1;
            }
            let limit = if has_limit {
                Some((limit_offset, limit_count))
            } else {
                None
            };
            zset::zrevrangebyscore(
                session,
                &args[1],
                &max,
                &min,
                with_scores,
                limit,
            )
        }
        b"zrevrank" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'zrevrank' command");
            }
            zset::zrevrank(session, &args[1], &args[2])
        }
        b"zscore" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'zscore' command");
            }
            zset::zscore(session, &args[1], &args[2])
        }
        b"zdiff" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'zdiff' command");
            }
            let Some(num_keys) = parse_i64(&args[1]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if num_keys < 0 {
                return error_op(session, "ERR value is not an integer or out of range");
            }
            let num_keys = num_keys as usize;
            if args.len() < 2 + num_keys {
                return error_op(session, "ERR wrong number of arguments for 'zdiff' command");
            }
            let keys = args[2..2 + num_keys].to_vec();
            let has_with_scores = args
                .get(2 + num_keys)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
            zset::zdiff(session, has_with_scores, &keys)
        }
        b"zdiffstore" => {
            if args.len() < 4 {
                return error_op(session, "ERR wrong number of arguments for 'zdiffstore' command");
            }
            let Some(num_keys) = parse_i64(&args[2]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if num_keys < 0 {
                return error_op(session, "ERR value is not an integer or out of range");
            }
            let num_keys = num_keys as usize;
            if args.len() < 3 + num_keys {
                return error_op(session, "ERR wrong number of arguments for 'zdiffstore' command");
            }
            let keys = args[3..3 + num_keys].to_vec();
            zset::zdiffstore(session, &args[1], &keys)
        }
        b"zmscore" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'zmscore' command");
            }
            zset::zmscore(session, &args[1], &args[2..])
        }
        b"zrandmember" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'zrandmember' command");
            }
            let mut count = 1i64;
            if args.len() >= 3 {
                let Some(parsed) = parse_i64(&args[2]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                count = parsed;
            }
            zset::zrandmember(session, &args[1], count)
        }
        b"zunion" | b"zunionstore" => {
            let is_store = name == b"zunionstore";
            if args.len() < 4 {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let arg_start = if is_store { 2 } else { 1 };
            let Some(num_keys) = parse_i64(&args[arg_start]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            if num_keys < 0 {
                return error_op(session, "ERR value is not an integer or out of range");
            }
            let num_keys = num_keys as usize;
            if args.len() < arg_start + 1 + num_keys {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let keys = args[arg_start + 1..arg_start + 1 + num_keys].to_vec();
            let mut i = arg_start + 1 + num_keys;
            let mut weights: Vec<f64> = Vec::new();
            let mut aggregate = String::from("SUM");
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"weights") {
                    i += 1;
                    for _ in 0..num_keys {
                        if i >= args.len() {
                            break;
                        }
                        let Some(w) = parse_f64(&args[i]) else {
                            return error_op(session, "ERR value is not a float");
                        };
                        weights.push(w);
                        i += 1;
                    }
                    if weights.len() != num_keys {
                        return error_op(session, "ERR weight count does not match number of keys");
                    }
                } else if args[i].eq_ignore_ascii_case(b"aggregate") {
                    i += 1;
                    if i >= args.len() {
                        return error_op(session, "ERR syntax error");
                    }
                    aggregate = String::from_utf8_lossy(&args[i]).to_string();
                    if !args[i].eq_ignore_ascii_case(b"sum")
                        && !args[i].eq_ignore_ascii_case(b"min")
                        && !args[i].eq_ignore_ascii_case(b"max")
                    {
                        return error_op(session, "ERR syntax error");
                    }
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"withscores") && !is_store {
                    i += 1;
                } else {
                    return error_op(session, "ERR syntax error");
                }
            }
            if is_store {
                zset::zunionstore(session, &args[1], &aggregate, &weights, &keys)
            } else {
                let has_with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"withscores"));
                zset::zunion(
                    session,
                    &aggregate,
                    &weights,
                    has_with_scores,
                    &keys,
                )
            }
        }
        b"zrangestore" => {
            if args.len() != 5 {
                return error_op(session, "ERR wrong number of arguments for 'zrangestore' command");
            }
            let Some(start) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(&args[4]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            zset::zrangestore(session, &args[1], &args[2], start, stop)
        }
        b"hset" => {
            // HSET key field value [field value ...]
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return error_op(session, "ERR wrong number of arguments for 'hset' command");
            }
            hash::hset(session, &args[1], &args[2..])
        }
        b"hsetnx" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'hsetnx' command");
            }
            hash::hsetnx(session, &args[1], &args[2], &args[3])
        }
        b"hget" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'hget' command");
            }
            hash::hget(session, &args[1], &args[2])
        }
        b"hmget" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'hmget' command");
            }
            hash::hmget(session, &args[1], &args[2..])
        }
        b"hdel" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'hdel' command");
            }
            hash::hdel(session, &args[1], &args[2..])
        }
        b"hexists" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'hexists' command");
            }
            hash::hexists(session, &args[1], &args[2])
        }
        b"hlen" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'hlen' command");
            }
            hash::hlen(session, &args[1])
        }
        b"hkeys" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'hkeys' command");
            }
            hash::hkeys(session, &args[1])
        }
        b"hvals" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'hvals' command");
            }
            hash::hvals(session, &args[1])
        }
        b"hgetall" => {
            if args.len() != 2 {
                return error_op(session, "ERR wrong number of arguments for 'hgetall' command");
            }
            hash::hgetall(session, &args[1])
        }
        b"hmset" => {
            // HMSET key field value [field value ...]
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return error_op(session, "ERR wrong number of arguments for 'hmset' command");
            }
            hash::hmset(session, &args[1], &args[2..])
        }
        b"hincrby" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'hincrby' command");
            }
            let Some(amount) = parse_i64(&args[3]) else {
                return error_op(session, "ERR value is not an integer or out of range");
            };
            hash::hincrby(session, &args[1], &args[2], amount)
        }
        b"hincrbyfloat" => {
            if args.len() != 4 {
                return error_op(session, "ERR wrong number of arguments for 'hincrbyfloat' command");
            }
            let Some(amount) = parse_f64(&args[3]) else {
                return error_op(session, "ERR value is not a float");
            };
            hash::hincrbyfloat(session, &args[1], &args[2], amount)
        }
        b"hrandfield" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'hrandfield' command");
            }
            let mut count = 1i64;
            let mut with_values = false;
            if args.len() >= 3 {
                let Some(parsed) = parse_i64(&args[2]) else {
                    return error_op(session, "ERR value is not an integer or out of range");
                };
                count = parsed;
            }
            if args.len() >= 4 {
                if args[3].eq_ignore_ascii_case(b"withvalues") {
                    with_values = true;
                } else {
                    return error_op(session, "ERR syntax error");
                }
            }
            hash::hrandfield(session, &args[1], count, with_values)
        }
        b"hstrlen" => {
            if args.len() != 3 {
                return error_op(session, "ERR wrong number of arguments for 'hstrlen' command");
            }
            hash::hstrlen(session, &args[1], &args[2])
        }
        b"hscan" => {
            if args.len() < 3 {
                return error_op(session, "ERR wrong number of arguments for 'hscan' command");
            }
            // HSCAN key cursor [MATCH pattern] [COUNT count]
            // Cursor argument is accepted but ignored (always full scan, cursor "0").
            let mut pattern: Vec<u8> = Vec::new();
            let mut count = 0i64;
            let mut i = 3usize;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"match") && i + 1 < args.len() {
                    i += 1;
                    pattern = args[i].to_vec();
                } else if args[i].eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
                    i += 1;
                    match parse_i64(&args[i]) {
                        Some(v) => count = v,
                        None => {
                            return error_op(session, "ERR value is not an integer or out of range");
                        }
                    }
                } else {
                    return error_op(session, "ERR syntax error");
                }
                i += 1;
            }
            hash::hscan(session, &args[1], pattern, count)
        }
        // --- Pub/Sub commands ---
        //
        // SUBSCRIBE / PSUBSCRIBE / SSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE /
        // SUNSUBSCRIBE are handled directly in the listener's connection loop
        // (they switch the connection into subscribe mode and need access to
        // the per-connection StreamMap state).
        //
        // Inside a MULTI block they are not allowed at all — queuing them must
        // mark the transaction dirty and immediately return an error, so EXEC
        // aborts rather than executing a partial batch.
        b"subscribe" | b"psubscribe" | b"ssubscribe"
        | b"unsubscribe" | b"punsubscribe" | b"sunsubscribe" => {
            // Reject inside MULTI: dirty the transaction and return an error.
            // Outside MULTI these commands are intercepted before dispatch_command
            // is called (in listener.rs), so if we reach here outside a MULTI
            // context that is also an error (belt-and-suspenders).
            return error_op(session, "ERR Command not allowed inside a transaction");
        }
        b"publish" | b"spublish" => {
            // SPUBLISH is a thin alias for PUBLISH in single-node mode.
            // TODO: distinguish SPUBLISH properly if horizontal scaling is added.
            if args.len() != 3 {
                return error_op(session, format!( "ERR wrong number of arguments for '{}' command", String::from_utf8_lossy(&name) ),);
            }
            let channel = args[1].clone();
            let payload = args[2].clone();
            session.pubsub().publish_op(channel, payload)
        }
        b"pubsub" => {
            if args.len() < 2 {
                return error_op(session, "ERR wrong number of arguments for 'pubsub' command");
            }
            let sub_cmd: Vec<u8> = args[1].iter().map(u8::to_ascii_lowercase).collect();
            match sub_cmd.as_slice() {
                b"channels" => {
                    let pat = args.get(2).map(|b| b.as_ref());
                    session.pubsub().active_channels(pat)
                }
                b"numsub" => {
                    let channel_args: Vec<Bytes> = args[2..].to_vec();
                    session.pubsub().numsub(channel_args)
                }
                b"numpat" => {
                    session.pubsub().numpat()
                }
                b"help" => {
                    pubsub::help()
                }
                _ => {
                    return error_op(session, format!( "ERR unknown subcommand '{}'. Try PUBSUB HELP.", String::from_utf8_lossy(&sub_cmd)));
                }
            }
        }
        _ => {
            return error_op(session, format!("ERR unknown command '{}'", String::from_utf8_lossy(&name)));
        }
    }
}

/// `+OK`.
struct OkOp;

impl WireOp for OkOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        ok_resp()
    }
}

fn ok() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(OkOp),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct ErrorOp {
    msg: Bytes,
}

impl WireOp for ErrorOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        RespValue::Error(self.msg.clone())
    }
}

fn error_op(session: &mut Session, msg: impl Into<Bytes>) -> QueuedOp {
    if session.in_multi() {
        session.mark_dirty();
    }
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(ErrorOp { msg: msg.into() }),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: true,
    }
}

/// Pushes an error reply, flagging the current MULTI transaction as dirty
/// (matching Redis's CLIENT_DIRTY_EXEC) whenever one is in progress.
fn error(session: &mut Session, msg: impl Into<Bytes>) -> RespValue {
    if session.in_multi() {
        session.mark_dirty();
    }
    RespValue::Error(msg.into())
}

/// Parses a command's arguments as exactly one `(key, value)` pair.
fn parse_pair(args: &[Bytes]) -> Option<(&Bytes, &Bytes)> {
    match args {
        [key, value] => Some((key, value)),
        _ => None,
    }
}

/// Parses a base-10 signed 64-bit integer, stripping trailing garbage.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Parses a base-10 64-bit float, stripping trailing garbage.
fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Calls `dispatch_command` with string args; panics on protocol errors.
    async fn dispatch(session: &mut Session, args: &[&str]) -> Vec<RespValue> {
        let args: Vec<Bytes> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        enqueue_command(session, &args).await
    }

    #[tokio::test]
    async fn ping_and_echo() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["ping"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"PONG"))]
        );
        assert_eq!(
            dispatch(&mut session, &["ping", "hi"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"hi")))]
        );
        assert_eq!(
            dispatch(&mut session, &["echo", "hello"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"hello")))]
        );
        assert!(matches!(
            dispatch(&mut session, &["echo"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn set_then_get_roundtrip() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"bar")))]
        );
        // Overwrite with a binary-ish value.
        dispatch(&mut session, &["set", "foo", "baz!"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"baz!")))]
        );
    }

    #[tokio::test]
    async fn get_missing_key_is_null() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["get", "nope"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn wrong_arity_is_an_error() {
        let mut session = test_session();
        assert!(matches!(
            dispatch(&mut session, &["get"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["set", "a"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn multi_exec_batches_commands() {
        let mut session = test_session();
        assert_eq!(dispatch(&mut session, &["multi"]).await, vec![ok_resp()]);
        assert_eq!(
            dispatch(&mut session, &["set", "foo", "bar"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![
                ok_resp(),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))]
        );
        // The writes must actually be persisted.
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"bar")))]
        );
    }

    #[tokio::test]
    async fn discard_drops_the_queue() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        assert_eq!(dispatch(&mut session, &["discard"]).await, vec![ok_resp()]);
        assert!(matches!(
            dispatch(&mut session, &["discard"]).await[0],
            RespValue::Error(_)
        ));
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn exec_without_multi_is_an_error() {
        let mut session = test_session();
        assert!(matches!(
            dispatch(&mut session, &["exec"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn unknown_command_aborts_exec() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        let err = dispatch(&mut session, &["notacommand"]).await;
        assert!(matches!(err[0], RespValue::Error(_)));
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors."
            ))]
        );
    }

    #[tokio::test]
    async fn nested_multi_errors_do_not_abort() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert!(matches!(
            dispatch(&mut session, &["multi"]).await[0],
            RespValue::Error(_)
        ));
        assert_eq!(
            dispatch(&mut session, &["set", "foo", "bar"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![ok_resp()]))]
        );
    }

    #[tokio::test]
    async fn select_switches_database() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "foo", "one"]).await;
        assert_eq!(dispatch(&mut session, &["select", "1"]).await, vec![ok_resp()]);
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(None)]
        );
        dispatch(&mut session, &["select", "0"]).await;
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"one")))]
        );
    }

    #[tokio::test]
    async fn select_rejects_bad_indices() {
        let mut session = test_session();
        for bad in ["abc", "-1", "1.5"] {
            assert!(matches!(
                dispatch(&mut session, &["select", bad]).await[0],
                RespValue::Error(_)
            ));
        }
    }

    fn expect_int_array(replies: &[RespValue]) -> Vec<i64> {
        match &replies[0] {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::Integer(n) => *n,
                    other => panic!("expected integer element, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bf_add_and_exists_through_dispatch() {
        let mut session = test_session();
        dispatch(&mut session, &["bf.add", "bf", "hello"]).await;
        assert_eq!(
            dispatch(&mut session, &["bf.add", "bf", "hello"]).await,
            vec![RespValue::Integer(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["bf.exists", "bf", "hello"]).await,
            vec![RespValue::Integer(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["bf.exists", "bf", "nope"]).await,
            vec![RespValue::Integer(0)]
        );
    }

    #[tokio::test]
    async fn bf_reserve_rejects_existing_and_bad_capacity() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["bf.reserve", "bf", "0.01", "100"]).await,
            vec![ok_resp()]
        );
        assert!(matches!(
            dispatch(&mut session, &["bf.reserve", "bf", "0.01", "100"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["bf.reserve", "other", "0.01", "0"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["bf.reserve", "other", "notafloat", "100"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["bf.reserve"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn bf_reserve_accepts_options() {
        let mut session = test_session();
        assert_eq!(
            dispatch(
                &mut session,
                &["bf.reserve", "bf", "0.001", "500", "EXPANSION", "4"]
            )
            .await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(
                &mut session,
                &["bf.reserve", "ns", "0.01", "100", "NONSCALING"]
            )
            .await,
            vec![ok_resp()]
        );
    }

    #[tokio::test]
    async fn bf_madd_and_mexists_through_dispatch() {
        let mut session = test_session();
        assert_eq!(
            expect_int_array(&dispatch(&mut session, &["bf.madd", "bf", "a", "b", "c"]).await),
            vec![1, 1, 1]
        );
        assert_eq!(
            expect_int_array(&dispatch(&mut session, &["bf.mexists", "bf", "a", "x", "c"]).await),
            vec![1, 0, 1]
        );
    }

    #[tokio::test]
    async fn bf_insert_through_dispatch() {
        let mut session = test_session();
        assert_eq!(
            expect_int_array(
                &dispatch(
                    &mut session,
                    &[
                        "bf.insert",
                        "bf",
                        "CAPACITY",
                        "500",
                        "ERROR",
                        "0.001",
                        "ITEMS",
                        "x",
                        "y"
                    ]
                )
                .await
            ),
            vec![1, 1]
        );
        assert_eq!(
            expect_int_array(&dispatch(&mut session, &["bf.insert", "bf", "ITEMS", "x"]).await),
            vec![0]
        );
        // NOCREATE on a missing key errors.
        assert!(matches!(
            dispatch(
                &mut session,
                &["bf.insert", "nokey", "NOCREATE", "ITEMS", "z"]
            )
            .await[0],
            RespValue::Error(_)
        ));
        // Missing ITEMS is a syntax error.
        assert!(matches!(
            dispatch(&mut session, &["bf.insert", "bf", "CAPACITY", "10"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn bf_info_through_dispatch() {
        let mut session = test_session();
        dispatch(&mut session, &["bf.add", "bf", "a"]).await;
        let replies = dispatch(&mut session, &["bf.info", "bf"]).await;
        match &replies[0] {
            RespValue::Array(Some(items)) => {
                let keys: Vec<String> = items
                    .iter()
                    .step_by(2)
                    .filter_map(|r| match r {
                        RespValue::BulkString(Some(b)) => {
                            Some(String::from_utf8_lossy(b).to_string())
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    keys,
                    vec![
                        "Capacity",
                        "Size",
                        "Number of filters",
                        "Number of items inserted",
                        "Expansion rate"
                    ]
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert!(matches!(
            dispatch(&mut session, &["bf.info", "nokey"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn list_commands_through_dispatch() {
        let mut session = test_session();

        // LPUSH + RPUSH build the list in the right order.
        assert_eq!(
            dispatch(&mut session, &["rpush", "mylist", "a", "b", "c"]).await,
            vec![RespValue::Integer(3)]
        );
        assert_eq!(
            dispatch(&mut session, &["lpush", "mylist", "x"]).await,
            vec![RespValue::Integer(4)]
        );
        assert_eq!(
            dispatch(&mut session, &["lrange", "mylist", "0", "-1"]).await,
            vec![RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"x"))),
                RespValue::BulkString(Some(Bytes::from_static(b"a"))),
                RespValue::BulkString(Some(Bytes::from_static(b"b"))),
                RespValue::BulkString(Some(Bytes::from_static(b"c"))),
            ]))]
        );

        assert_eq!(
            dispatch(&mut session, &["llen", "mylist"]).await,
            vec![RespValue::Integer(4)]
        );
        assert_eq!(
            dispatch(&mut session, &["lindex", "mylist", "1"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"a")))]
        );
        assert_eq!(
            dispatch(&mut session, &["lset", "mylist", "1", "A"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["lpop", "mylist"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"x")))]
        );
        assert_eq!(
            dispatch(&mut session, &["rpop", "mylist"]).await,
            vec![RespValue::BulkString(Some(Bytes::from_static(b"c")))]
        );

        // LREM and LINSERT mutate the surviving chain.
        dispatch(&mut session, &["rpush", "mylist", "c", "b", "b"]).await;
        assert_eq!(
            dispatch(&mut session, &["lrem", "mylist", "0", "b"]).await,
            vec![RespValue::Integer(3)]
        );
        dispatch(&mut session, &["rpush", "mylist", "d"]).await;
        assert_eq!(
            dispatch(&mut session, &["linsert", "mylist", "BEFORE", "d", "z"]).await,
            vec![RespValue::Integer(4)]
        );
        assert_eq!(
            dispatch(&mut session, &["ltrim", "mylist", "1", "2"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["lrange", "mylist", "0", "-1"]).await,
            vec![RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"c"))),
                RespValue::BulkString(Some(Bytes::from_static(b"z"))),
            ]))]
        );

        // LPUSHX / RPUSHX no-op on a missing key.
        assert_eq!(
            dispatch(&mut session, &["rpushx", "nokey", "v"]).await,
            vec![RespValue::Integer(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["lpushx", "nokey", "v"]).await,
            vec![RespValue::Integer(0)]
        );

        // Wrong arity and bad integers are errors.
        assert!(matches!(
            dispatch(&mut session, &["lpush", "k"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["lrange", "k", "x", "1"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["linsert", "k", "BEFORE", "a"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn list_commands_work_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["rpush", "mylist", "a", "b"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["llen", "mylist"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![
                RespValue::Integer(2),
                RespValue::Integer(2),
            ]))]
        );
    }

    #[tokio::test]
    async fn set_commands_through_dispatch() {
        let mut session = test_session();

        // SADD + SCARD report cardinality.
        assert_eq!(
            dispatch(&mut session, &["sadd", "myset", "a", "b", "c"]).await,
            vec![RespValue::Integer(3)]
        );
        assert_eq!(
            dispatch(&mut session, &["sadd", "myset", "a"]).await,
            vec![RespValue::Integer(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["scard", "myset"]).await,
            vec![RespValue::Integer(3)]
        );

        // SISMEMBER and SMEMBERS read the members back.
        assert_eq!(
            dispatch(&mut session, &["sismember", "myset", "b"]).await,
            vec![RespValue::Integer(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["sismember", "myset", "x"]).await,
            vec![RespValue::Integer(0)]
        );

        // SREM removes, SPOP random-pops, SRANDMEMBER samples.
        assert_eq!(
            dispatch(&mut session, &["srem", "myset", "a", "x"]).await,
            vec![RespValue::Integer(1)]
        );
        let replies = dispatch(&mut session, &["spop", "myset"]).await;
        match &replies[0] {
            RespValue::BulkString(Some(m)) => {
                assert!(matches!(m.as_ref(), b"b" | b"c"));
            }
            other => panic!("expected bulk string, got {other:?}"),
        }
        let replies = dispatch(&mut session, &["srandmember", "myset"]).await;
        match &replies[0] {
            RespValue::BulkString(Some(_)) => {}
            other => panic!("expected bulk string, got {other:?}"),
        }

        // SDIFF / SINTER / SUNION combine sets.
        dispatch(&mut session, &["sadd", "s1", "a", "b", "c"]).await;
        dispatch(&mut session, &["sadd", "s2", "b", "c", "d"]).await;
        assert_eq!(
            dispatch(&mut session, &["sdiff", "s1", "s2"]).await,
            vec![RespValue::Array(Some(vec![RespValue::BulkString(Some(
                Bytes::from_static(b"a"),
            ))]))]
        );
        // Store commands write into the destination key.
        assert_eq!(
            dispatch(&mut session, &["sinterstore", "dest", "s1", "s2"]).await,
            vec![RespValue::Integer(2)]
        );
        let members = dispatch(&mut session, &["smembers", "dest"]).await;
        match &members[0] {
            RespValue::Array(Some(items)) => {
                let mut got: Vec<Bytes> = items
                    .iter()
                    .map(|r| match r {
                        RespValue::BulkString(Some(b)) => b.clone(),
                        other => panic!("expected bulk string, got {other:?}"),
                    })
                    .collect();
                got.sort(); // SMEMBERS order is unspecified.
                assert_eq!(
                    got,
                    vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]
                );
            }
            other => panic!("expected array, got {other:?}"),
        }

        // Missing keys behave per Redis semantics.
        assert_eq!(
            dispatch(&mut session, &["scard", "nope"]).await,
            vec![RespValue::Integer(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["smembers", "nope"]).await,
            vec![RespValue::Array(Some(Vec::new()))]
        );
        assert_eq!(
            dispatch(&mut session, &["spop", "nope"]).await,
            vec![RespValue::BulkString(None)]
        );

        // Wrong arity and unknown subcommands are errors.
        assert!(matches!(
            dispatch(&mut session, &["sadd", "k"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["sdiffstore", "k"]).await[0],
            RespValue::Error(_)
        ));
        assert!(matches!(
            dispatch(&mut session, &["smove", "a", "b"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn set_commands_work_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["sadd", "myset", "a", "b"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["scard", "myset"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![
                RespValue::Integer(2),
                RespValue::Integer(2),
            ]))]
        );
    }

    #[tokio::test]
    async fn bf_commands_work_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["bf.add", "bf", "a"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["bf.exists", "bf", "a"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(1),
            ]))]
        );
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())))
    }

    async fn json_bulk(session: &mut Session, cmd: &[&str]) -> String {
        match &dispatch(session, cmd).await[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn json_set_get_roundtrip() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", r#"{"name":"Alice","age":30}"#])
                .await,
            vec![ok_resp()]
        );
        assert_eq!(
            json_bulk(&mut session, &["json.get", "doc"]).await,
            r#"{"age":30,"name":"Alice"}"#
        );
        // Single path.
        assert_eq!(
            json_bulk(&mut session, &["json.get", "doc", "$.name"]).await,
            r#""Alice""#
        );
        // Missing key -> RESP null.
        assert_eq!(
            dispatch(&mut session, &["json.get", "nokey", "$.name"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn json_get_multi_path() {
        let mut session = test_session();
        dispatch(
            &mut session,
            &["json.set", "doc", "$", r#"{"a":1,"b":2}"#],
        )
        .await;
        assert_eq!(
            json_bulk(&mut session, &["json.get", "doc", "$.a", "$.b"]).await,
            r#"{"$.a":1,"$.b":2}"#
        );
    }

    #[tokio::test]
    async fn json_set_nx_xx() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", "1", "NX"]).await,
            vec![ok_resp()]
        );
        // NX on an existing key -> null.
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", "2", "NX"]).await,
            vec![RespValue::BulkString(None)]
        );
        // XX on a missing key -> null.
        assert_eq!(
            dispatch(&mut session, &["json.set", "other", "$", "2", "XX"]).await,
            vec![RespValue::BulkString(None)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", "2", "XX"]).await,
            vec![ok_resp()]
        );
        // NX and XX together are rejected at parse time.
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", "2", "NX", "XX"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR NX and XX are mutually exclusive"
            ))]
        );
    }

    #[tokio::test]
    async fn json_set_rejects_bad_input() {
        let mut session = test_session();
        // Invalid JSON value.
        assert!(matches!(
            dispatch(&mut session, &["json.set", "doc", "$", "notjson"]).await[0],
            RespValue::Error(_)
        ));
        // Unknown flag -> syntax error.
        assert!(matches!(
            dispatch(&mut session, &["json.set", "doc", "$", "1", "BOGUS"]).await[0],
            RespValue::Error(_)
        ));
        // Bad FPHA type -> syntax error.
        assert!(matches!(
            dispatch(&mut session, &["json.set", "doc", "$", "1", "FPHA", "X"]).await[0],
            RespValue::Error(_)
        ));
        // FPHA with a value out of range.
        assert!(matches!(
            dispatch(&mut session, &["json.set", "doc", "$", "1e40", "FPHA", "FP16"]).await[0],
            RespValue::Error(_)
        ));
        // Wrong arity.
        assert!(matches!(
            dispatch(&mut session, &["json.set", "doc"]).await[0],
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn json_type_del_len() {
        let mut session = test_session();
        dispatch(
            &mut session,
            &[
                "json.set",
                "doc",
                "$",
                r#"{"s":"x","n":1,"arr":[1],"o":{"k":1},"b":true,"nil":null}"#,
            ],
        )
        .await;

        for (path, want) in [
            ("$.s", "string"),
            ("$.n", "number"),
            ("$.arr", "array"),
            ("$.o", "object"),
            ("$.b", "boolean"),
            ("$.nil", "null"),
            ("$", "object"),
        ] {
            assert_eq!(
                json_bulk(&mut session, &["json.type", "doc", path]).await,
                want.to_string()
            );
        }
        // Missing key -> null.
        assert_eq!(
            dispatch(&mut session, &["json.type", "nokey", "$"]).await,
            vec![RespValue::BulkString(None)]
        );

        // JSON.DEL removes a path and reports the count.
        assert_eq!(
            dispatch(&mut session, &["json.del", "doc", "$.s"]).await,
            vec![RespValue::Integer(1)]
        );
        // A deleted path resolves to RESP null (not the JSON value null).
        assert_eq!(
            dispatch(&mut session, &["json.get", "doc", "$.s"]).await,
            vec![RespValue::BulkString(None)]
        );
        // Deleting the whole key reports 1 even when missing.
        assert_eq!(
            dispatch(&mut session, &["json.del", "doc"]).await,
            vec![RespValue::Integer(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.del", "nokey"]).await,
            vec![RespValue::Integer(1)]
        );
    }

    #[tokio::test]
    async fn json_arr_commands_through_dispatch() {
        let mut session = test_session();
        dispatch(
            &mut session,
            &["json.set", "doc", "$", r#"{"arr":[1,2,3]}"#],
        )
        .await;

        assert_eq!(
            dispatch(&mut session, &["json.arrappend", "doc", "$.arr", "4", "5"]).await,
            vec![RespValue::Integer(5)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.arrindex", "doc", "$.arr", "4"]).await,
            vec![RespValue::Integer(3)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.arrindex", "doc", "$.arr", "99"]).await,
            vec![RespValue::Integer(-1)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.arrlen", "doc", "$.arr"]).await,
            vec![RespValue::Integer(5)]
        );
        // Missing path -> null.
        assert_eq!(
            dispatch(&mut session, &["json.arrlen", "doc", "$.nope"]).await,
            vec![RespValue::BulkString(None)]
        );
        // ARRPOP / ARRTRIM / ARRINSERT mutate in place.
        assert_eq!(
            dispatch(&mut session, &["json.arrpop", "doc", "$.arr", "1"]).await,
            vec![bulk("2")]
        );
        assert_eq!(
            dispatch(&mut session, &["json.arrtrim", "doc", "$.arr", "0", "1"]).await,
            vec![RespValue::Integer(2)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.arrinsert", "doc", "$.arr", "1", "99"]).await,
            vec![RespValue::Integer(3)]
        );
        assert_eq!(
            json_bulk(&mut session, &["json.get", "doc", "$.arr"]).await,
            "[1,99,3]".to_string()
        );
    }

    #[tokio::test]
    async fn json_number_obj_string_commands() {
        let mut session = test_session();
        dispatch(
            &mut session,
            &["json.set", "doc", "$", r#"{"n":10,"o":{"a":1,"b":2},"s":"hello"}"#],
        )
        .await;

        assert_eq!(
            dispatch(&mut session, &["json.numincrby", "doc", "$.n", "5"]).await,
            vec![bulk("15")]
        );
        assert_eq!(
            dispatch(&mut session, &["json.nummultby", "doc", "$.n", "3"]).await,
            vec![bulk("45")]
        );

        // OBJKEYS returns sorted keys.
        assert_eq!(
            dispatch(&mut session, &["json.objkeys", "doc", "$.o"]).await,
            vec![RespValue::Array(Some(vec![bulk("a"), bulk("b")]))]
        );
        assert_eq!(
            dispatch(&mut session, &["json.objlen", "doc", "$.o"]).await,
            vec![RespValue::Integer(2)]
        );

        assert_eq!(
            dispatch(&mut session, &["json.strappend", "doc", "$.s", r#"" world""#]).await,
            vec![RespValue::Integer(11)]
        );
        assert_eq!(
            dispatch(&mut session, &["json.strlen", "doc", "$.s"]).await,
            vec![RespValue::Integer(11)]
        );
        // STRAPPEND with no path appends to the root string.
        dispatch(&mut session, &["json.set", "sdoc", "$", r#""foo""#]).await;
        assert_eq!(
            dispatch(&mut session, &["json.strappend", "sdoc", r#""bar""#]).await,
            vec![RespValue::Integer(6)]
        );
    }

    #[tokio::test]
    async fn json_mget_resp_clear() {
        let mut session = test_session();
        dispatch(&mut session, &["json.set", "k1", "$", r#"{"v":1}"#]).await;
        dispatch(&mut session, &["json.set", "k2", "$", r#"{"v":2}"#]).await;

        assert_eq!(
            dispatch(&mut session, &["json.mget", "k1", "k2", "k3", "$.v"]).await,
            vec![RespValue::Array(Some(vec![
                bulk("1"),
                bulk("2"),
                RespValue::BulkString(None),
            ]))]
        );

        dispatch(
            &mut session,
            &["json.set", "doc", "$", r#"{"n":1,"b":true}"#],
        )
        .await;
        // JSON.RESP numbers are bulk strings, booleans become 1/0 integers.
        assert_eq!(
            dispatch(&mut session, &["json.resp", "doc", "$.n"]).await,
            vec![bulk("1")]
        );
        assert_eq!(
            dispatch(&mut session, &["json.resp", "doc", "$.b"]).await,
            vec![RespValue::Integer(1)]
        );

        dispatch(&mut session, &["json.set", "doc", "$", r#"{"a":{"x":1}}"#]).await;
        assert_eq!(
            dispatch(&mut session, &["json.clear", "doc", "$.a"]).await,
            vec![RespValue::Integer(1)]
        );
        assert_eq!(
            json_bulk(&mut session, &["json.get", "doc", "$.a"]).await,
            "{}".to_string()
        );
    }

    #[tokio::test]
    async fn json_command_arity_and_type_errors() {
        let mut session = test_session();
        for bad in [
            &["json.get"][..],
            &["json.del"][..],
            &["json.arrlen"][..],
            &["json.mget", "k1"][..],
            &["json.arrtrim", "doc", "$.arr"][..],
            &["json.arrinsert", "doc", "$.arr"][..],
        ] {
            assert!(
                matches!(dispatch(&mut session, bad).await[0], RespValue::Error(_)),
                "expected error for {bad:?}"
            );
        }
        // ARRPOP with a non-integer index (no ERR prefix, matching Go).
        assert_eq!(
            dispatch(&mut session, &["json.arrpop", "doc", "$", "x"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"value is not an integer or out of range"
            ))]
        );
        // STRAPPEND with a non-string JSON value.
        assert!(matches!(
            dispatch(&mut session, &["json.strappend", "doc", "$", "42"]).await[0],
            RespValue::Error(_)
        ));
        // Commands against a non-JSON key report WRONGTYPE.
        dispatch(&mut session, &["set", "strkey", "plain"]).await;
        assert!(matches!(
            &dispatch(&mut session, &["json.get", "strkey"]).await[0],
            RespValue::Error(b) if b.starts_with(b"WRONGTYPE")
        ));
    }

    #[tokio::test]
    async fn json_commands_work_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["json.set", "doc", "$", "1"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["json.get", "doc"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![ok_resp(), bulk("1")]))]
        );
    }

    fn int(n: i64) -> RespValue {
        RespValue::Integer(n)
    }

    #[tokio::test]
    async fn pfadd_pfcount_pfmerge_through_dispatch() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["pfadd", "hll_a", "hello", "world"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["pfadd", "hll_a", "hello"]).await,
            vec![int(0)]
        );
        // Single-key count of a populated sketch.
        assert_eq!(
            dispatch(&mut session, &["pfcount", "hll_a"]).await,
            vec![int(2)]
        );
        // Empty/nonexistent sketches count 0.
        assert_eq!(
            dispatch(&mut session, &["pfcount", "hll_empty"]).await,
            vec![int(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["pfcount", "hll_nonexist"]).await,
            vec![int(0)]
        );
        // Merge into a fresh key, then cross-check counts.
        assert_eq!(
            dispatch(&mut session, &["pfadd", "hll_b", "hello"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["pfmerge", "hll_m", "hll_a", "hll_b"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["pfcount", "hll_m"]).await,
            vec![int(2)]
        );
        // A multi-key count must be >= each single-key count.
        let multi = match &dispatch(&mut session, &["pfcount", "hll_a", "hll_b"]).await[0] {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        };
        assert!(multi >= 2);
    }

    #[tokio::test]
    async fn pf_commands_wrong_arity() {
        let mut session = test_session();
        for cmd in [&["pfadd", "k"][..], &["pfcount"][..]] {
            assert_eq!(
                dispatch(&mut session, cmd).await,
                vec![RespValue::Error(Bytes::from(format!(
                    "ERR wrong number of arguments for '{}' command",
                    cmd[0]
                )))]
            );
        }
        // PFMERGE with only a dest key is legal: it unions zero sources into
        // an empty sketch (Go's checkMinArgs(..., 2)).
        assert_eq!(
            dispatch(&mut session, &["pfmerge", "empty_dest"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["pfcount", "empty_dest"]).await,
            vec![int(0)]
        );
    }

    #[tokio::test]
    async fn pf_commands_work_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["pfadd", "hll", "a"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["pfcount", "hll"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![int(1), int(1)]))]
        );
    }

    #[tokio::test]
    async fn conn_commands_through_dispatch() {
        let mut session = test_session();
        // CLIENT ID echoes the session id.
        assert_eq!(
            dispatch(&mut session, &["client", "id"]).await,
            vec![int(session.id() as i64)]
        );
        // CLIENT INFO is a bulk string starting with id and db.
        match &dispatch(&mut session, &["client", "info"]).await[0] {
            RespValue::BulkString(Some(info)) => {
                let info = String::from_utf8_lossy(info).to_string();
                assert!(info.starts_with(&format!("id={} addr=", session.id())), "got {info:?}");
                assert!(info.contains("db=0"), "got {info:?}");
            }
            other => panic!("expected bulk string, got {other:?}"),
        }
        // Unknown CLIENT subcommand.
        assert_eq!(
            dispatch(&mut session, &["client", "setname"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'client|setname' command"
            ))]
        );
        // SYNC / PSYNC / WAIT reply +OK.
        assert_eq!(dispatch(&mut session, &["sync"]).await, vec![ok_resp()]);
        assert_eq!(dispatch(&mut session, &["psync"]).await, vec![ok_resp()]);
        assert_eq!(dispatch(&mut session, &["wait"]).await, vec![ok_resp()]);
        // LOLWUT is the Invar banner.
        assert!(matches!(
            &dispatch(&mut session, &["lolwut"]).await[0],
            RespValue::BulkString(Some(b)) if String::from_utf8_lossy(b).contains("Invar version:")
        ));
        // TIME is [sec, micro].
        match &dispatch(&mut session, &["time"]).await[0] {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                for item in items {
                    assert!(matches!(item, RespValue::BulkString(Some(_))));
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
        // MODULE LIST is an empty array.
        assert_eq!(
            dispatch(&mut session, &["module", "list"]).await,
            vec![RespValue::Array(Some(Vec::new()))]
        );
        // SAVE / BGSAVE reply +OK.
        assert_eq!(dispatch(&mut session, &["save"]).await, vec![ok_resp()]);
        assert_eq!(dispatch(&mut session, &["bgsave"]).await, vec![ok_resp()]);
        // DBSIZE counts the current DB.
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        dispatch(&mut session, &["set", "baz", "qux"]).await;
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(2)]);
        // SELECT then DBSIZE is scoped to the new DB.
        assert_eq!(dispatch(&mut session, &["select", "3"]).await, vec![ok_resp()]);
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        // Select back to DB 0: still two keys.
        assert_eq!(dispatch(&mut session, &["select", "0"]).await, vec![ok_resp()]);
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(2)]);
    }

    #[tokio::test]
    async fn conn_commands_wrong_arity_and_bad_subcommand() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["client"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'client' command"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["module"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'module' command"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["module", "load", "x.so"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR unknown subcommand"
            ))]
        );
    }

    #[tokio::test]
    async fn save_is_rejected_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["save"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"Command not allowed inside a transaction"
            ))]
        );
        // The rejection dirties the transaction (Go writes the error through a
        // tracked connection), so EXEC aborts.
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors."
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "foo"]).await,
            vec![RespValue::BulkString(None)]
        );
    }

    #[tokio::test]
    async fn bgsave_works_inside_multi() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        assert_eq!(
            dispatch(&mut session, &["bgsave"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![ok_resp()]))]
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn quit_replies_ok_and_requests_close() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["quit"]).await,
            vec![ok_resp()]
        );
        assert!(session.should_close());
    }

    #[tokio::test]
    async fn flushall_and_flushdb_through_dispatch() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "keep", "1"]).await;
        assert_eq!(
            dispatch(&mut session, &["flushdb"]).await,
            vec![ok_resp()]
        );
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        dispatch(&mut session, &["set", "other", "2"]).await;
        assert_eq!(
            dispatch(&mut session, &["flushall"]).await,
            vec![ok_resp()]
        );
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
    }

    #[tokio::test]
    async fn flush_is_rejected_inside_multi_and_dirties() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        for cmd in [&["flushall"][..], &["flushdb"][..]] {
            assert_eq!(
                dispatch(&mut session, cmd).await,
                vec![RespValue::Error(
                    Bytes::from("Command not allowed inside a transaction".to_string()))]
            );
        }
        // The rejection dirties the transaction, so EXEC aborts.
        assert_eq!(
            dispatch(&mut session, &["set", "foo", "bar"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors."
            ))]
        );
    }

    #[tokio::test]
    async fn select_inside_multi_takes_effect_atomically() {
        let mut session = test_session();
        dispatch(&mut session, &["multi"]).await;
        // SELECT is queued (QUEUED), not applied with an immediate reply.
        assert_eq!(
            dispatch(&mut session, &["select", "1"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["set", "multi_select_key", "v"]).await,
            vec![RespValue::SimpleString(Bytes::from_static(b"QUEUED"))]
        );
        assert_eq!(
            dispatch(&mut session, &["exec"]).await,
            vec![RespValue::Array(Some(vec![ok_resp(), ok_resp()]))]
        );
        // The SELECT-scoped write landed in db1, not db0.
        assert_eq!(
            dispatch(&mut session, &["select", "0"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "multi_select_key"]).await,
            vec![RespValue::BulkString(None)]
        );
        assert_eq!(
            dispatch(&mut session, &["select", "1"]).await,
            vec![ok_resp()]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "multi_select_key"]).await,
            vec![bulk("v")]
        );
    }

    #[tokio::test]
    async fn bitmap_commands_through_dispatch() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["setbit", "bm", "0", "1"]).await,
            vec![int(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "bm", "0"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["setbit", "bm", "0", "1"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "bm", "7"]).await,
            vec![int(0)]
        );
        // Extends past first byte.
        assert_eq!(
            dispatch(&mut session, &["setbit", "bm", "8", "1"]).await,
            vec![int(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "bm", "8"]).await,
            vec![int(1)]
        );
        // BITCOUNT: byte 0 has bits 0 set only → 1; with byte range [0,0] → 1.
        assert_eq!(
            dispatch(&mut session, &["bitcount", "bm"]).await,
            vec![int(2)]
        );
        assert_eq!(
            dispatch(&mut session, &["bitcount", "bm", "0", "0"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["bitcount", "bm", "0", "0", "bit"]).await,
            vec![int(1)]
        );
        // BITPOS: first 1 is bit 0.
        assert_eq!(
            dispatch(&mut session, &["bitpos", "bm", "1"]).await,
            vec![int(0)]
        );
        // BITPOS for 0 on a key with a zero byte: first zero is bit 1 within byte 0.
        assert_eq!(
            dispatch(&mut session, &["bitpos", "bm", "0"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["bitpos", "bm_nonexist", "0"]).await,
            vec![int(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["bitpos", "bm_nonexist", "1"]).await,
            vec![int(-1)]
        );
        // BITOP AND/OR/XOR returns the length of the longest source.
        dispatch(&mut session, &["setbit", "src_a", "0", "1"]).await;
        dispatch(&mut session, &["setbit", "src_b", "1", "1"]).await;
        assert_eq!(
            dispatch(&mut session, &["bitop", "AND", "dest", "src_a", "src_b"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "dest", "0"]).await,
            vec![int(0)]
        );
        assert_eq!(
            dispatch(&mut session, &["bitop", "OR", "dest", "src_a", "src_b"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "dest", "0"]).await,
            vec![int(1)]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "dest", "1"]).await,
            vec![int(1)]
        );
    }

    #[tokio::test]
    async fn bitmap_commands_error_handling() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["setbit", "k", "0"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'setbit' command"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["setbit", "k", "-1", "1"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR bit offset is not an integer or out of range"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["setbit", "k", "0", "2"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR bit is not an integer or out of range"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["setbit", "k", "x", "1"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "k"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'getbit' command"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["getbit", "k", "-5"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR bit offset is not an integer or out of range"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["bitpos", "k", "2"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR bit is not an integer or out of range"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["bitcount"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bitcount' command"
            ))]
        );
        assert_eq!(
            dispatch(&mut session, &["bitcount", "k", "0", "0", "words"]).await,
            vec![RespValue::Error(Bytes::from_static(b"ERR syntax error"))]
        );
        // Start without end is a syntax error for BITCOUNT.
        assert_eq!(
            dispatch(&mut session, &["bitcount", "k", "0"]).await,
            vec![RespValue::Error(Bytes::from_static(b"ERR syntax error"))]
        );
        assert_eq!(
            dispatch(&mut session, &["bitop", "BAD", "dest", "op"]).await,
            vec![RespValue::Error(Bytes::from_static(b"ERR syntax error"))]
        );
        assert_eq!(
            dispatch(&mut session, &["bitop", "AND", "dest"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bitop' command"
            ))]
        );
        // NOT takes exactly one source key.
        assert_eq!(
            dispatch(&mut session, &["bitop", "NOT", "dest", "a", "b"]).await,
            vec![RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bitop' command"
            ))]
        );
    }
}
