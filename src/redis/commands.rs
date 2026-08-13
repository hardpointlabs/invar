//! Redis command dispatch: routes a decoded command array to its handler,
//! enforcing the `MULTI`/`EXEC`/`DISCARD` lifecycle, and flushes any queued
//! operations through the session's transaction dispatcher.

use bytes::Bytes;

use crate::bloom;
use crate::common::op::WireOp;
use crate::common::session::Session;
use crate::keys;
use crate::list;
use crate::resp::RespValue;
use crate::set;
use crate::strings;
use crate::zset;

/// Dispatches a single decoded command. Returns the RESP replies to write
/// back to the client (possibly multiple, e.g. pipelined queue replies).
pub async fn dispatch_command(session: &mut Session, args: &[Bytes]) -> Vec<RespValue> {
    let mut replies = Vec::new();

    let Some(name) = args.first() else {
        error(session, &mut replies, "ERR empty command");
        return replies;
    };
    let name: Vec<u8> = name.iter().map(u8::to_ascii_lowercase).collect();

    match name.as_slice() {
        b"ping" => match &args[1..] {
            [] => queue_wire(session, &mut replies, PingOp::new(None)),
            [msg] => queue_wire(session, &mut replies, PingOp::new(Some(msg.clone()))),
            _ => error(
                session,
                &mut replies,
                "ERR wrong number of arguments for 'ping' command",
            ),
        },
        b"echo" => match &args[1..] {
            [msg] => queue_wire(session, &mut replies, EchoOp { msg: msg.clone() }),
            _ => error(
                session,
                &mut replies,
                "ERR wrong number of arguments for 'echo' command",
            ),
        },
        b"select" => {
            match &args[1..] {
                [db] => match parse_i64(db) {
                    Some(db) if db >= 0 => {
                        session.switch_db(db as i32);
                        replies.push(ok());
                    }
                    _ => error(session, &mut replies, "ERR invalid DB index"),
                },
                _ => error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'select' command",
                ),
            }
            return replies;
        }
        b"multi" => {
            if session.in_multi() {
                // Nested MULTI is an error but does NOT abort the outer
                // transaction, so it bypasses dirty tracking.
                replies.push(RespValue::Error(Bytes::from_static(
                    b"MULTI calls can not be nested",
                )));
            } else {
                session.enter_multi();
                replies.push(ok());
            }
            return replies;
        }
        b"exec" => match session.exit_multi(false) {
            Ok(()) => replies.extend(session.dispatch_pending_ops(true).await),
            Err(_) => error(session, &mut replies, "ERR EXEC without MULTI"),
        },
        b"discard" => match session.exit_multi(true) {
            Ok(()) => replies.push(ok()),
            Err(_) => error(session, &mut replies, "DISCARD without MULTI"),
        },
        b"set" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'set' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::set(session, key, value)) {
                replies.push(queued);
            }
        }
        b"get" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'get' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::get(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"setex" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'setex' command",
                );
                return replies;
            }
            let Some(seconds) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(strings::set_ex(session, &args[1], &args[3], seconds))
            {
                replies.push(queued);
            }
        }
        b"psetex" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'psetex' command",
                );
                return replies;
            }
            let Some(ms) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(strings::pset_ex(session, &args[1], &args[3], ms))
            {
                replies.push(queued);
            }
        }
        b"getset" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'getset' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::get_set(session, key, value)) {
                replies.push(queued);
            }
        }
        b"getdel" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'getdel' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::get_del(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"strlen" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'strlen' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::strlen(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"substr" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'substr' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(end) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::substr(session, &args[1], start, end))
            {
                replies.push(queued);
            }
        }
        b"getrange" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'getrange' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(end) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::substr(session, &args[1], start, end))
            {
                replies.push(queued);
            }
        }
        b"setnx" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'setnx' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::set_nx(session, key, value)) {
                replies.push(queued);
            }
        }
        b"append" => {
            let Some((key, value)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'append' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::append(session, key, value)) {
                replies.push(queued);
            }
        }
        b"getex" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'getex' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::get_ex(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"incrbyfloat" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'incrbyfloat' command",
                );
                return replies;
            };
            let Some(amount) = parse_f64(amount) else {
                error(session, &mut replies, "ERR value is not a float");
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::incr_by_float(session, key, amount)) {
                replies.push(queued);
            }
        }
        b"mset" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'mset' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::mset(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"msetnx" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'msetnx' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::mset_nx(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"setrange" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'setrange' command",
                );
                return replies;
            }
            let Some(offset) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if offset < 0 {
                error(session, &mut replies, "ERR offset is out of range");
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(strings::set_range(session, &args[1], offset, &args[3]))
            {
                replies.push(queued);
            }
        }
        b"incr" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'incr' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::increment(session, &args[1], 1)) {
                replies.push(queued);
            }
        }
        b"incrby" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'incrby' command",
                );
                return replies;
            };
            let Some(amount) = parse_i64(amount) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::increment(session, key, amount)) {
                replies.push(queued);
            }
        }
        b"decr" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'decr' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(strings::increment(session, &args[1], -1)) {
                replies.push(queued);
            }
        }
        b"decrby" => {
            let Some((key, amount)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'decrby' command",
                );
                return replies;
            };
            let Some(amount) = parse_i64(amount) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(strings::increment(session, key, -amount)) {
                replies.push(queued);
            }
        }
        b"bf.reserve" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.reserve' command",
                );
                return replies;
            }
            let Some(err_rate) = parse_f64(&args[2]) else {
                error(session, &mut replies, "ERR value is not a float");
                return replies;
            };
            let Some(capacity) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if capacity < 1 {
                error(session, &mut replies, "ERR capacity must be positive");
                return replies;
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
                            error(
                                session,
                                &mut replies,
                                "ERR value is not an integer or out of range",
                            );
                            return replies;
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"nonscaling") {
                    non_scaling = true;
                }
                i += 1;
            }
            if let Some(queued) = session.enqueue_op(bloom::reserve(
                session,
                &args[1],
                err_rate,
                capacity as u64,
                expansion,
                non_scaling,
            )) {
                replies.push(queued);
            }
        }
        b"bf.add" => {
            let Some((key, item)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.add' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(bloom::add(session, key, item)) {
                replies.push(queued);
            }
        }
        b"bf.exists" => {
            let Some((key, item)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.exists' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(bloom::exists(session, key, item)) {
                replies.push(queued);
            }
        }
        b"bf.madd" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.madd' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bloom::madd(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"bf.mexists" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.mexists' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bloom::mexists(session, &args[1], &args[2..]))
            {
                replies.push(queued);
            }
        }
        b"bf.insert" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.insert' command",
                );
                return replies;
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
                            error(
                                session,
                                &mut replies,
                                "ERR value is not an integer or out of range",
                            );
                            return replies;
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"error") && i + 1 < args.len() {
                    i += 1;
                    match parse_f64(&args[i]) {
                        Some(v) => error_rate = v,
                        None => {
                            error(session, &mut replies, "ERR value is not a float");
                            return replies;
                        }
                    }
                } else if args[i].eq_ignore_ascii_case(b"expansion") && i + 1 < args.len() {
                    i += 1;
                    match parse_i64(&args[i]) {
                        Some(v) => expansion = v,
                        None => {
                            error(
                                session,
                                &mut replies,
                                "ERR value is not an integer or out of range",
                            );
                            return replies;
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
                    error(
                        session,
                        &mut replies,
                        format!("ERR syntax error at {}", String::from_utf8_lossy(&args[i])),
                    );
                    return replies;
                }
                i += 1;
            }
            if items.is_empty() {
                error(session, &mut replies, "ERR ITEMS argument required");
                return replies;
            }
            let info = bloom::InsertInfo {
                capacity,
                error: error_rate,
                expansion,
                no_create,
                non_scaling,
                items,
            };
            if let Some(queued) = session.enqueue_op(bloom::insert(session, &args[1], info)) {
                replies.push(queued);
            }
        }
        b"bf.info" => {
            let Some(key) = args.get(1) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bf.info' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(bloom::info(session, key)) {
                replies.push(queued);
            }
        }
        b"lpush" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lpush' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::lpush(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"rpush" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'rpush' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::rpush(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"lpop" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lpop' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::lpop(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"rpop" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'rpop' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::rpop(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"llen" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'llen' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::llen(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"lrange" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lrange' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(list::lrange(session, &args[1], start, stop)) {
                replies.push(queued);
            }
        }
        b"lindex" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lindex' command",
                );
                return replies;
            }
            let Some(index) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(list::lindex(session, &args[1], index)) {
                replies.push(queued);
            }
        }
        b"lset" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lset' command",
                );
                return replies;
            }
            let Some(index) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(list::lset(session, &args[1], index, &args[3]))
            {
                replies.push(queued);
            }
        }
        b"lrem" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lrem' command",
                );
                return replies;
            }
            let Some(count) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(list::lrem(session, &args[1], count, &args[3]))
            {
                replies.push(queued);
            }
        }
        b"ltrim" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'ltrim' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(list::ltrim(session, &args[1], start, stop)) {
                replies.push(queued);
            }
        }
        b"linsert" => {
            if args.len() != 5 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'linsert' command",
                );
                return replies;
            }
            let before = args[2].eq_ignore_ascii_case(b"before");
            if let Some(queued) =
                session.enqueue_op(list::linsert(session, &args[1], before, &args[3], &args[4]))
            {
                replies.push(queued);
            }
        }
        b"lpushx" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'lpushx' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::lpushx(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"rpushx" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'rpushx' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(list::rpushx(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"sadd" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sadd' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sadd(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"srem" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'srem' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::srem(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"scard" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'scard' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::scard(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"smembers" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'smembers' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::smembers(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"sismember" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sismember' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sismember(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"spop" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'spop' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::spop(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"srandmember" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'srandmember' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::srandmember(session, &args[1], 1)) {
                replies.push(queued);
            }
        }
        b"smove" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'smove' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(set::smove(session, &args[1], &args[2], &args[3]))
            {
                replies.push(queued);
            }
        }
        b"sdiff" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sdiff' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sdiff(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"sinter" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sinter' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sinter(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"sunion" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sunion' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sunion(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"sdiffstore" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sdiffstore' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(set::sdiffstore(session, &args[1], &args[2..]))
            {
                replies.push(queued);
            }
        }
        b"sinterstore" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sinterstore' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(set::sinterstore(session, &args[1], &args[2..]))
            {
                replies.push(queued);
            }
        }
        b"sunionstore" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'sunionstore' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(set::sunionstore(session, &args[1], &args[2..]))
            {
                replies.push(queued);
            }
        }
        b"zadd" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zadd' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zadd(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"zcard" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zcard' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zcard(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"zcount" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zcount' command",
                );
                return replies;
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            if let Some(queued) = session.enqueue_op(zset::zcount(session, &args[1], &min, &max)) {
                replies.push(queued);
            }
        }
        b"zincrby" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zincrby' command",
                );
                return replies;
            }
            let Some(incr) = parse_f64(&args[2]) else {
                error(session, &mut replies, "ERR value is not a float");
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(zset::zincrby(session, &args[1], incr, &args[3]))
            {
                replies.push(queued);
            }
        }
        b"zinter" | b"zinterstore" => {
            let is_store = name == b"zinterstore";
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
            }
            let arg_start = if is_store { 2 } else { 1 };
            let Some(num_keys) = parse_i64(&args[arg_start]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if num_keys < 0 {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            }
            let num_keys = num_keys as usize;
            if args.len() < arg_start + 1 + num_keys {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
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
                            error(session, &mut replies, "ERR value is not a float");
                            return replies;
                        };
                        weights.push(w);
                        i += 1;
                    }
                    if weights.len() != num_keys {
                        error(
                            session,
                            &mut replies,
                            "ERR weight count does not match number of keys",
                        );
                        return replies;
                    }
                } else if args[i].eq_ignore_ascii_case(b"aggregate") {
                    i += 1;
                    if i >= args.len() {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    aggregate = String::from_utf8_lossy(&args[i]).to_string();
                    if !args[i].eq_ignore_ascii_case(b"sum")
                        && !args[i].eq_ignore_ascii_case(b"min")
                        && !args[i].eq_ignore_ascii_case(b"max")
                    {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"withscores") && !is_store {
                    i += 1;
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
            }
            if is_store {
                if let Some(queued) = session.enqueue_op(zset::zinterstore(
                    session, &args[1], &aggregate, &weights, &keys,
                )) {
                    replies.push(queued);
                }
            } else {
                let has_with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"withscores"));
                if let Some(queued) = session.enqueue_op(zset::zinter(
                    session,
                    &aggregate,
                    &weights,
                    has_with_scores,
                    &keys,
                )) {
                    replies.push(queued);
                }
            }
        }
        b"zlexcount" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zlexcount' command",
                );
                return replies;
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            if let Some(queued) = session.enqueue_op(zset::zlexcount(session, &args[1], &min, &max))
            {
                replies.push(queued);
            }
        }
        b"zpopmax" | b"zpopmin" => {
            let want_min = name == b"zpopmin";
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
            }
            let mut count = 1usize;
            if args.len() >= 3 {
                match parse_i64(&args[2]) {
                    Some(v) if v >= 0 => count = v as usize,
                    _ => {
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
                    }
                }
            }
            if let Some(queued) = session.enqueue_op(if want_min {
                zset::zpopmin(session, &args[1], count)
            } else {
                zset::zpopmax(session, &args[1], count)
            }) {
                replies.push(queued);
            }
        }
        b"exists" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'exists' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::exists(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"mget" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'mget' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::mget(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"move" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'move' command",
                );
                return replies;
            }
            let Some(target_db) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if target_db < 0 {
                error(session, &mut replies, "ERR invalid DB index");
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(keys::move_op(session, &args[1], target_db as i32))
            {
                replies.push(queued);
            }
        }
        b"rename" => {
            let Some((old_key, new_key)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'rename' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(keys::rename(session, old_key, new_key)) {
                replies.push(queued);
            }
        }
        b"renamenx" => {
            let Some((old_key, new_key)) = parse_pair(&args[1..]) else {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'renamenx' command",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(keys::rename_nx(session, old_key, new_key)) {
                replies.push(queued);
            }
        }
        b"pttl" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'pttl' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::pttl(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"ttl" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'ttl' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::ttl(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"expire" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'expire' command",
                );
                return replies;
            }
            let Some(seconds) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) = session.enqueue_op(keys::expire(session, &args[1], seconds)) {
                replies.push(queued);
            }
        }
        b"type" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'type' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::key_type(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"del" | b"unlink" => {
            if args.len() < 2 {
                let name = if name.as_slice() == b"del" {
                    "del"
                } else {
                    "unlink"
                };
                error(
                    session,
                    &mut replies,
                    format!("ERR wrong number of arguments for '{name}' command"),
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::del(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"zrange" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrange' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let with_scores = args
                .get(4)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
            if let Some(queued) =
                session.enqueue_op(zset::zrange(session, &args[1], start, stop, with_scores))
            {
                replies.push(queued);
            }
        }
        b"zrangebylex" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrangebylex' command",
                );
                return replies;
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
                let Some(offset) = parse_i64(&args[5]) else {
                    error(
                        session,
                        &mut replies,
                        "ERR value is not an integer or out of range",
                    );
                    return replies;
                };
                let Some(count) = parse_i64(&args[6]) else {
                    error(
                        session,
                        &mut replies,
                        "ERR value is not an integer or out of range",
                    );
                    return replies;
                };
                limit_offset = offset;
                limit_count = count;
                has_limit = true;
            }
            if let Some(queued) = session.enqueue_op(zset::zrangebylex(
                session,
                &args[1],
                &min,
                &max,
                limit_offset,
                limit_count,
                has_limit,
            )) {
                replies.push(queued);
            }
        }
        b"zrangebyscore" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrangebyscore' command",
                );
                return replies;
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
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
                    };
                    let Some(count) = parse_i64(&args[i + 2]) else {
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
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
            if let Some(queued) = session.enqueue_op(zset::zrangebyscore(
                session,
                &args[1],
                &min,
                &max,
                with_scores,
                limit,
            )) {
                replies.push(queued);
            }
        }
        b"zrank" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrank' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zrank(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"zrem" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrem' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zrem(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"zremrangebylex" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zremrangebylex' command",
                );
                return replies;
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            if let Some(queued) =
                session.enqueue_op(zset::zremrangebylex(session, &args[1], &min, &max))
            {
                replies.push(queued);
            }
        }
        b"zremrangebyrank" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zremrangebyrank' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(zset::zremrangebyrank(session, &args[1], start, stop))
            {
                replies.push(queued);
            }
        }
        b"zremrangebyscore" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zremrangebyscore' command",
                );
                return replies;
            }
            let min = String::from_utf8_lossy(&args[2]).into_owned();
            let max = String::from_utf8_lossy(&args[3]).into_owned();
            if let Some(queued) =
                session.enqueue_op(zset::zremrangebyscore(session, &args[1], &min, &max))
            {
                replies.push(queued);
            }
        }
        b"zrevrange" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrevrange' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let with_scores = args
                .get(4)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
            if let Some(queued) =
                session.enqueue_op(zset::zrevrange(session, &args[1], start, stop, with_scores))
            {
                replies.push(queued);
            }
        }
        b"zrevrangebylex" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrevrangebylex' command",
                );
                return replies;
            }
            let max = String::from_utf8_lossy(&args[2]).into_owned();
            let min = String::from_utf8_lossy(&args[3]).into_owned();
            let (mut limit_offset, mut limit_count, mut has_limit) = (0i64, 0i64, false);
            if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
                let Some(offset) = parse_i64(&args[5]) else {
                    error(
                        session,
                        &mut replies,
                        "ERR value is not an integer or out of range",
                    );
                    return replies;
                };
                let Some(count) = parse_i64(&args[6]) else {
                    error(
                        session,
                        &mut replies,
                        "ERR value is not an integer or out of range",
                    );
                    return replies;
                };
                limit_offset = offset;
                limit_count = count;
                has_limit = true;
            }
            if let Some(queued) = session.enqueue_op(zset::zrevrangebylex(
                session,
                &args[1],
                &max,
                &min,
                limit_offset,
                limit_count,
                has_limit,
            )) {
                replies.push(queued);
            }
        }
        b"zrevrangebyscore" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrevrangebyscore' command",
                );
                return replies;
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
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
                    };
                    let Some(count) = parse_i64(&args[i + 2]) else {
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
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
            if let Some(queued) = session.enqueue_op(zset::zrevrangebyscore(
                session,
                &args[1],
                &max,
                &min,
                with_scores,
                limit,
            )) {
                replies.push(queued);
            }
        }
        b"zrevrank" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrevrank' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zrevrank(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"zscore" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zscore' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zscore(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"zdiff" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zdiff' command",
                );
                return replies;
            }
            let Some(num_keys) = parse_i64(&args[1]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if num_keys < 0 {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            }
            let num_keys = num_keys as usize;
            if args.len() < 2 + num_keys {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zdiff' command",
                );
                return replies;
            }
            let keys = args[2..2 + num_keys].to_vec();
            let has_with_scores = args
                .get(2 + num_keys)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"withscores"));
            if let Some(queued) = session.enqueue_op(zset::zdiff(session, has_with_scores, &keys)) {
                replies.push(queued);
            }
        }
        b"zdiffstore" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zdiffstore' command",
                );
                return replies;
            }
            let Some(num_keys) = parse_i64(&args[2]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if num_keys < 0 {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            }
            let num_keys = num_keys as usize;
            if args.len() < 3 + num_keys {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zdiffstore' command",
                );
                return replies;
            }
            let keys = args[3..3 + num_keys].to_vec();
            if let Some(queued) = session.enqueue_op(zset::zdiffstore(session, &args[1], &keys)) {
                replies.push(queued);
            }
        }
        b"zmscore" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zmscore' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(zset::zmscore(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"zrandmember" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrandmember' command",
                );
                return replies;
            }
            let mut count = 1i64;
            if args.len() >= 3 {
                let Some(parsed) = parse_i64(&args[2]) else {
                    error(
                        session,
                        &mut replies,
                        "ERR value is not an integer or out of range",
                    );
                    return replies;
                };
                count = parsed;
            }
            if let Some(queued) = session.enqueue_op(zset::zrandmember(session, &args[1], count)) {
                replies.push(queued);
            }
        }
        b"zunion" | b"zunionstore" => {
            let is_store = name == b"zunionstore";
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
            }
            let arg_start = if is_store { 2 } else { 1 };
            let Some(num_keys) = parse_i64(&args[arg_start]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if num_keys < 0 {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            }
            let num_keys = num_keys as usize;
            if args.len() < arg_start + 1 + num_keys {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
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
                            error(session, &mut replies, "ERR value is not a float");
                            return replies;
                        };
                        weights.push(w);
                        i += 1;
                    }
                    if weights.len() != num_keys {
                        error(
                            session,
                            &mut replies,
                            "ERR weight count does not match number of keys",
                        );
                        return replies;
                    }
                } else if args[i].eq_ignore_ascii_case(b"aggregate") {
                    i += 1;
                    if i >= args.len() {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    aggregate = String::from_utf8_lossy(&args[i]).to_string();
                    if !args[i].eq_ignore_ascii_case(b"sum")
                        && !args[i].eq_ignore_ascii_case(b"min")
                        && !args[i].eq_ignore_ascii_case(b"max")
                    {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"withscores") && !is_store {
                    i += 1;
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
            }
            if is_store {
                if let Some(queued) = session.enqueue_op(zset::zunionstore(
                    session, &args[1], &aggregate, &weights, &keys,
                )) {
                    replies.push(queued);
                }
            } else {
                let has_with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"withscores"));
                if let Some(queued) = session.enqueue_op(zset::zunion(
                    session,
                    &aggregate,
                    &weights,
                    has_with_scores,
                    &keys,
                )) {
                    replies.push(queued);
                }
            }
        }
        b"bzpopmin" | b"bzpopmax" => {
            let want_min = name == b"bzpopmin";
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&name)
                    ),
                );
                return replies;
            }
            // Syntax: BZPOPMIN key [key ...] timeout. The last argument is
            // the timeout in (fractional) seconds; 0 blocks indefinitely.
            let timeout_arg = &args[args.len() - 1];
            let Some(timeout) = parse_f64(timeout_arg) else {
                error(
                    session,
                    &mut replies,
                    "ERR timeout is not a float or out of range",
                );
                return replies;
            };
            if timeout < 0.0 {
                error(
                    session,
                    &mut replies,
                    "ERR timeout is not a float or out of range",
                );
                return replies;
            }
            let keys: Vec<&[u8]> = args[1..args.len() - 1].iter().map(|b| b.as_ref()).collect();
            replies.push(zset::bzpop_reply(session, &keys, timeout, want_min).await);
            return replies;
        }
        b"zrangestore" => {
            if args.len() != 5 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'zrangestore' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            let Some(stop) = parse_i64(&args[4]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(zset::zrangestore(session, &args[1], &args[2], start, stop))
            {
                replies.push(queued);
            }
        }
        _ => {
            error(
                session,
                &mut replies,
                format!("ERR unknown command '{}'", String::from_utf8_lossy(&name)),
            );
            return replies;
        }
    }

    replies.extend(session.dispatch_pending_ops(false).await);
    replies
}

/// `+OK`.
fn ok() -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

/// Pushes an error reply, flagging the current MULTI transaction as dirty
/// (matching Redis's CLIENT_DIRTY_EXEC) whenever one is in progress.
fn error(session: &mut Session, replies: &mut Vec<RespValue>, msg: impl Into<Bytes>) {
    if session.in_multi() {
        session.mark_dirty();
    }
    replies.push(RespValue::Error(msg.into()));
}

/// Enqueues a wire-only op, recording the `+QUEUED` reply if in MULTI.
fn queue_wire(session: &mut Session, replies: &mut Vec<RespValue>, wire_op: impl WireOp + 'static) {
    if let Some(queued) = session.enqueue_wire_op(Box::new(wire_op)) {
        replies.push(queued);
    }
}

/// Parses a command's arguments as exactly one `(key, value)` pair.
fn parse_pair(args: &[Bytes]) -> Option<(&Bytes, &Bytes)> {
    match args {
        [key, value] => Some((key, value)),
        _ => None,
    }
}

/// Parses a base-10 signed 64-bit integer, rejecting trailing garbage.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Parses a base-10 64-bit float, rejecting trailing garbage.
fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

/// `PING [message]` — replies `+PONG`, or the message as a bulk string.
pub struct PingOp {
    msg: Option<Bytes>,
}

impl PingOp {
    fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }
}

impl WireOp for PingOp {
    fn reply(
        &self,
        _result: Result<crate::common::op::DbResult, crate::common::op::DbError>,
    ) -> RespValue {
        match &self.msg {
            Some(msg) => RespValue::BulkString(Some(msg.clone())),
            None => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        }
    }
}

/// `ECHO message` — replies with the message as a bulk string.
pub struct EchoOp {
    msg: Bytes,
}

impl WireOp for EchoOp {
    fn reply(
        &self,
        _result: Result<crate::common::op::DbResult, crate::common::op::DbError>,
    ) -> RespValue {
        RespValue::BulkString(Some(self.msg.clone()))
    }
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
        dispatch_command(session, &args).await
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
        assert_eq!(dispatch(&mut session, &["multi"]).await, vec![ok()]);
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
                ok(),
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
        assert_eq!(dispatch(&mut session, &["discard"]).await, vec![ok()]);
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
            vec![RespValue::Array(Some(vec![ok()]))]
        );
    }

    #[tokio::test]
    async fn select_switches_database() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "foo", "one"]).await;
        assert_eq!(dispatch(&mut session, &["select", "1"]).await, vec![ok()]);
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
            vec![ok()]
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
            vec![ok()]
        );
        assert_eq!(
            dispatch(
                &mut session,
                &["bf.reserve", "ns", "0.01", "100", "NONSCALING"]
            )
            .await,
            vec![ok()]
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
            vec![ok()]
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
            vec![ok()]
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
}
