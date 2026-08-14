//! Redis command dispatch: routes a decoded command array to its handler,
//! enforcing the `MULTI`/`EXEC`/`DISCARD` lifecycle, and flushes any queued
//! operations through the session's transaction dispatcher.

use bytes::Bytes;

use crate::bitmap;
use crate::bloom;
use crate::common::op::WireOp;
use crate::common::session::Session;
use crate::conn;
use crate::hash;
use crate::hll;
use crate::json;
use crate::keys;
use crate::list;
use crate::resp::RespValue;
use crate::server;
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
                        if session.in_multi() {
                            // Deferred: reply `+OK` at EXEC, matching the
                            // transactional semantics where each queued
                            // command yields one array element.
                            if let Some(queued) = session.enqueue_op(conn::ok_op()) {
                                replies.push(queued);
                            }
                        } else {
                            replies.push(ok());
                        }
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
        b"client" => {
            let op = server::client(session, args);
            if let Some(queued) = session.enqueue_op(op) {
                replies.push(queued);
            }
        }
        b"info" => {
            if let Some(queued) = session.enqueue_op(server::info()) {
                replies.push(queued);
            }
        }
        b"hello" => {
            let op = server::hello(session, args);
            if let Some(queued) = session.enqueue_op(op) {
                replies.push(queued);
            }
        }
        b"sync" | b"psync" => {
            if let Some(queued) = session.enqueue_op(conn::sync()) {
                replies.push(queued);
            }
        }
        b"wait" => {
            if let Some(queued) = session.enqueue_op(conn::wait()) {
                replies.push(queued);
            }
        }
        b"lolwut" => {
            if let Some(queued) = session.enqueue_op(conn::lolwut(conn::VERSION, conn::COMMIT)) {
                replies.push(queued);
            }
        }
        b"time" => {
            if let Some(queued) = session.enqueue_op(conn::time()) {
                replies.push(queued);
            }
        }
        b"module" => {
            if let Some(queued) = session.enqueue_op(conn::module(args)) {
                replies.push(queued);
            }
        }
        b"bgsave" => {
            if let Some(queued) = session.enqueue_op(conn::bgsave(session)) {
                replies.push(queued);
            }
        }
        b"save" => {
            if session.in_multi() {
                // Written via the tracked connection in Go, so it dirties the
                // transaction and aborts EXEC.
                error(session, &mut replies, "Command not allowed inside a transaction");
            } else {
                match session.store().sync().await {
                    Ok(()) => replies.push(ok()),
                    Err(e) => replies.push(RespValue::Error(format!("ERR {e}").into())),
                }
            }
            return replies;
        }
        b"dbsize" => {
            if let Some(queued) = session.enqueue_op(conn::dbsize(session)) {
                replies.push(queued);
            }
        }
        b"quit" => {
            replies.push(ok());
            session.request_close();
            return replies;
        }
        b"flushall" => {
            if session.in_multi() {
                error(
                    session,
                    &mut replies,
                    "This server does not support FLUSHALL execution inside MULTI",
                );
            } else {
                match session.store().destroy().await {
                    Ok(()) => replies.push(ok()),
                    Err(e) => replies.push(RespValue::Error(format!("ERR {e}").into())),
                }
            }
            return replies;
        }
        b"flushdb" => {
            if session.in_multi() {
                error(
                    session,
                    &mut replies,
                    "This server does not support FLUSHDB execution inside MULTI",
                );
            } else {
                match session.store().drop_prefix(&session.prefix()).await {
                    Ok(()) => replies.push(ok()),
                    Err(e) => replies.push(RespValue::Error(format!("ERR {e}").into())),
                }
            }
            return replies;
        }
        b"setbit" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'setbit' command",
                );
                return replies;
            }
            let Some(offset) = parse_i64(&args[2]) else {
                error(session, &mut replies, "ERR value is not an integer or out of range");
                return replies;
            };
            if offset < 0 {
                error(session, &mut replies, "ERR bit offset is not an integer or out of range");
                return replies;
            }
            let Some(value) = parse_i64(&args[3]) else {
                error(session, &mut replies, "ERR value is not an integer or out of range");
                return replies;
            };
            if value != 0 && value != 1 {
                error(session, &mut replies, "ERR bit is not an integer or out of range");
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bitmap::set_bit(session, &args[1], offset, value))
            {
                replies.push(queued);
            }
        }
        b"getbit" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'getbit' command",
                );
                return replies;
            }
            let Some(offset) = parse_i64(&args[2]) else {
                error(session, &mut replies, "ERR value is not an integer or out of range");
                return replies;
            };
            if offset < 0 {
                error(session, &mut replies, "ERR bit offset is not an integer or out of range");
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bitmap::get_bit(session, &args[1], offset)) {
                replies.push(queued);
            }
        }
        b"bitcount" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bitcount' command",
                );
                return replies;
            }
            let mut start_given = false;
            let mut end_given = false;
            let mut start_val = 0i64;
            let mut end_val = 0i64;
            let mut use_bit = false;
            let mut i = 2usize;
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    error(session, &mut replies, "ERR value is not an integer or out of range");
                    return replies;
                };
                start_val = v;
                start_given = true;
                i += 1;
            }
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    error(session, &mut replies, "ERR value is not an integer or out of range");
                    return replies;
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
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
                i += 1;
            }
            if i < args.len() {
                error(session, &mut replies, "ERR syntax error");
                return replies;
            }
            if start_given != end_given {
                error(session, &mut replies, "ERR syntax error");
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bitmap::bit_count(
                session,
                &args[1],
                start_given,
                end_given,
                start_val,
                end_val,
                use_bit,
            )) {
                replies.push(queued);
            }
        }
        b"bitpos" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bitpos' command",
                );
                return replies;
            }
            let Some(bit) = parse_i64(&args[2]) else {
                error(session, &mut replies, "ERR value is not an integer or out of range");
                return replies;
            };
            if bit != 0 && bit != 1 {
                error(session, &mut replies, "ERR bit is not an integer or out of range");
                return replies;
            }
            let mut start_given = false;
            let mut start_val = 0i64;
            let mut end_val = 0i64;
            let mut use_bit = false;
            let mut i = 3usize;
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    error(session, &mut replies, "ERR value is not an integer or out of range");
                    return replies;
                };
                start_val = v;
                start_given = true;
                i += 1;
            }
            if i < args.len() {
                let Some(v) = parse_i64(&args[i]) else {
                    error(session, &mut replies, "ERR value is not an integer or out of range");
                    return replies;
                };
                end_val = v;
                i += 1;
            }
            if i < args.len() {
                let unit = args[i].to_ascii_lowercase();
                if unit == *b"bit" {
                    use_bit = true;
                } else if unit != *b"byte" {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
                i += 1;
            }
            if i < args.len() {
                error(session, &mut replies, "ERR syntax error");
                return replies;
            }
            if let Some(queued) = session.enqueue_op(bitmap::bit_pos(
                session,
                &args[1],
                bit,
                start_given,
                start_val,
                end_val,
                use_bit,
            )) {
                replies.push(queued);
            }
        }
        b"bitop" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bitop' command",
                );
                return replies;
            }
            let Some(op) = bitmap::parse_bit_op(&args[1]) else {
                error(session, &mut replies, "ERR syntax error");
                return replies;
            };
            if op == bitmap::BitOpType::Not && args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'bitop' command",
                );
                return replies;
            }
            let src_keys: Vec<&[u8]> = args[3..].iter().map(|b| b.as_ref()).collect();
            if let Some(queued) = session.enqueue_op(bitmap::bit_op(session, &args[2], op, &src_keys))
            {
                replies.push(queued);
            }
        }
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
        b"json.set" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.set' command",
                );
                return replies;
            }
            let Some(value) = json::parse_json(&args[3]) else {
                error(session, &mut replies, "ERR invalid JSON");
                return replies;
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
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    i += 1;
                    match json::parse_fpha(&args[i]) {
                        Some(parsed) => ft = parsed,
                        None => {
                            error(session, &mut replies, "ERR syntax error");
                            return replies;
                        }
                    }
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
                i += 1;
            }
            if nx && xx {
                error(session, &mut replies, "ERR NX and XX are mutually exclusive");
                return replies;
            }
            if ft != json::FphaType::None {
                if let Err(e) = json::validate_fpha(&value, ft) {
                    error(session, &mut replies, e);
                    return replies;
                }
            }
            if let Some(queued) = session.enqueue_op(json::set(session, &args[1], &args[2], value, nx, xx)) {
                replies.push(queued);
            }
        }
        b"json.get" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.get' command",
                );
                return replies;
            }
            let paths: Vec<String> = args[2..]
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            if let Some(queued) = session.enqueue_op(json::get(session, &args[1], paths)) {
                replies.push(queued);
            }
        }
        b"json.del" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.del' command",
                );
                return replies;
            }
            let paths: Vec<String> = args[2..]
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            if let Some(queued) = session.enqueue_op(json::del(session, &args[1], paths)) {
                replies.push(queued);
            }
        }
        b"json.type" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.type' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::json_type(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.arrappend" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrappend' command",
                );
                return replies;
            }
            let mut values = Vec::with_capacity(args.len() - 3);
            for v in &args[3..] {
                match json::parse_json(v) {
                    Some(jv) => values.push(jv),
                    None => {
                        error(session, &mut replies, "ERR invalid JSON");
                        return replies;
                    }
                }
            }
            if let Some(queued) = session.enqueue_op(json::arr_append(session, &args[1], &args[2], values)) {
                replies.push(queued);
            }
        }
        b"json.arrindex" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrindex' command",
                );
                return replies;
            }
            let Some(value) = json::parse_json(&args[3]) else {
                error(session, &mut replies, "ERR invalid JSON");
                return replies;
            };
            if let Some(queued) = session.enqueue_op(json::arr_index(session, &args[1], &args[2], value)) {
                replies.push(queued);
            }
        }
        b"json.arrlen" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrlen' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::arr_len(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.numincrby" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.numincrby' command",
                );
                return replies;
            }
            let Some(delta) = parse_f64(&args[3]) else {
                error(session, &mut replies, "ERR value is not a number");
                return replies;
            };
            if let Some(queued) = session.enqueue_op(json::num_incr_by(session, &args[1], &args[2], delta)) {
                replies.push(queued);
            }
        }
        b"json.nummultby" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.nummultby' command",
                );
                return replies;
            }
            let Some(factor) = parse_f64(&args[3]) else {
                error(session, &mut replies, "ERR value is not a number");
                return replies;
            };
            if let Some(queued) = session.enqueue_op(json::num_mult_by(session, &args[1], &args[2], factor)) {
                replies.push(queued);
            }
        }
        b"json.objkeys" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.objkeys' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::obj_keys(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.objlen" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.objlen' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::obj_len(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.strappend" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.strappend' command",
                );
                return replies;
            }
            let (path, value_idx) = if args.len() == 4 {
                (args[2].clone(), 3)
            } else if args.len() == 3 {
                (Bytes::from_static(b"$"), 2)
            } else {
                (Bytes::from_static(b"$"), 3)
            };
            let Some(suffix) = json::parse_json_string(&args[value_idx]) else {
                error(session, &mut replies, "ERR invalid JSON string");
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(json::str_append(session, &args[1], &path, suffix))
            {
                replies.push(queued);
            }
        }
        b"json.strlen" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.strlen' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::str_len(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.mget" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.mget' command",
                );
                return replies;
            }
            let last = args.len() - 1;
            let path = String::from_utf8_lossy(&args[last]).into_owned();
            let keys: Vec<Vec<u8>> = args[1..last].iter().map(|k| k.to_vec()).collect();
            if let Some(queued) = session.enqueue_op(json::mget(session, keys, path)) {
                replies.push(queued);
            }
        }
        b"json.resp" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.resp' command",
                );
                return replies;
            }
            let path = if args.len() >= 3 {
                String::from_utf8_lossy(&args[2]).into_owned()
            } else {
                String::new()
            };
            if let Some(queued) = session.enqueue_op(json::resp(session, &args[1], path)) {
                replies.push(queued);
            }
        }
        b"json.clear" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.clear' command",
                );
                return replies;
            }
            let path: Vec<u8> = if args.len() >= 3 {
                args[2].to_vec()
            } else {
                b"$".to_vec()
            };
            if let Some(queued) = session.enqueue_op(json::clear(session, &args[1], &path)) {
                replies.push(queued);
            }
        }
        b"json.arrpop" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrpop' command",
                );
                return replies;
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
                        error(session, &mut replies, "value is not an integer or out of range");
                        return replies;
                    }
                }
            }
            if let Some(queued) = session.enqueue_op(json::arr_pop(session, &args[1], &path, idx)) {
                replies.push(queued);
            }
        }
        b"json.arrtrim" => {
            if args.len() < 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrtrim' command",
                );
                return replies;
            }
            let Some(start) = parse_i64(&args[3]) else {
                error(session, &mut replies, "value is not an integer or out of range");
                return replies;
            };
            let mut stop = -1i64;
            if args.len() >= 5 {
                match parse_i64(&args[4]) {
                    Some(v) => stop = v,
                    None => {
                        error(session, &mut replies, "value is not an integer or out of range");
                        return replies;
                    }
                }
            }
            if let Some(queued) =
                session.enqueue_op(json::arr_trim(session, &args[1], &args[2], start, stop))
            {
                replies.push(queued);
            }
        }
        b"json.arrinsert" => {
            if args.len() < 5 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'json.arrinsert' command",
                );
                return replies;
            }
            let Some(index) = parse_i64(&args[3]) else {
                error(session, &mut replies, "ERR value is not an integer or out of range");
                return replies;
            };
            let mut values = Vec::with_capacity(args.len() - 4);
            for v in &args[4..] {
                match json::parse_json(v) {
                    Some(jv) => values.push(jv),
                    None => {
                        error(session, &mut replies, "ERR invalid JSON");
                        return replies;
                    }
                }
            }
            if let Some(queued) = session.enqueue_op(json::arr_insert(session, &args[1], &args[2], index, values)) {
                replies.push(queued);
            }
        }
        b"pfadd" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'pfadd' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hll::pfadd(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"pfcount" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'pfcount' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hll::pfcount(session, &args[1..])) {
                replies.push(queued);
            }
        }
        b"pfmerge" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'pfmerge' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(hll::pfmerge(session, &args[1], &args[2..]))
            {
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
        b"object" => {
            // TODO this is hard-wired to only implement the IDLETIME subcommand stub
            // Replace if/when we decide to implement more OBJECT subcommands
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'object' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(keys::idle_time(session)) {
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
        b"scan" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'scan' command",
                );
                return replies;
            }
            let mut count = 10usize;
            let mut pattern: Option<Vec<u8>> = None;
            let mut type_filter: Option<u8> = None;
            let mut i = 2;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"match") {
                    if i + 1 >= args.len() {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    pattern = Some(args[i + 1].to_vec());
                    i += 2;
                } else if args[i].eq_ignore_ascii_case(b"count") {
                    if i + 1 >= args.len() {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    let Some(n) = parse_i64(&args[i + 1]) else {
                        error(
                            session,
                            &mut replies,
                            "ERR value is not an integer or out of range",
                        );
                        return replies;
                    };
                    if n < 1 {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    count = n as usize;
                    i += 2;
                } else if args[i].eq_ignore_ascii_case(b"type") {
                    if i + 1 >= args.len() {
                        error(session, &mut replies, "ERR syntax error");
                        return replies;
                    }
                    // Unknown type names match nothing (Redis 7.x behaviour).
                    type_filter = keys::type_byte(&args[i + 1]);
                    i += 2;
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
            }
            let pattern = match pattern {
                Some(p) if p == b"*" => None,
                other => other,
            };
            if let Some(queued) = session.enqueue_op(keys::scan(
                session,
                &args[1],
                count,
                pattern,
                type_filter,
            )) {
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
        b"hset" => {
            // HSET key field value [field value ...]
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hset' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hset(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"hsetnx" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hsetnx' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(hash::hsetnx(session, &args[1], &args[2], &args[3]))
            {
                replies.push(queued);
            }
        }
        b"hget" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hget' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hget(session, &args[1], &args[2])) {
                replies.push(queued);
            }
        }
        b"hmget" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hmget' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(hash::hmget(session, &args[1], &args[2..]))
            {
                replies.push(queued);
            }
        }
        b"hdel" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hdel' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hdel(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"hexists" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hexists' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(hash::hexists(session, &args[1], &args[2]))
            {
                replies.push(queued);
            }
        }
        b"hlen" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hlen' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hlen(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"hkeys" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hkeys' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hkeys(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"hvals" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hvals' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hvals(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"hgetall" => {
            if args.len() != 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hgetall' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hgetall(session, &args[1])) {
                replies.push(queued);
            }
        }
        b"hmset" => {
            // HMSET key field value [field value ...]
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hmset' command",
                );
                return replies;
            }
            if let Some(queued) = session.enqueue_op(hash::hmset(session, &args[1], &args[2..])) {
                replies.push(queued);
            }
        }
        b"hincrby" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hincrby' command",
                );
                return replies;
            }
            let Some(amount) = parse_i64(&args[3]) else {
                error(
                    session,
                    &mut replies,
                    "ERR value is not an integer or out of range",
                );
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(hash::hincrby(session, &args[1], &args[2], amount))
            {
                replies.push(queued);
            }
        }
        b"hincrbyfloat" => {
            if args.len() != 4 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hincrbyfloat' command",
                );
                return replies;
            }
            let Some(amount) = parse_f64(&args[3]) else {
                error(session, &mut replies, "ERR value is not a float");
                return replies;
            };
            if let Some(queued) =
                session.enqueue_op(hash::hincrbyfloat(session, &args[1], &args[2], amount))
            {
                replies.push(queued);
            }
        }
        b"hrandfield" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hrandfield' command",
                );
                return replies;
            }
            let mut count = 1i64;
            let mut with_values = false;
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
            if args.len() >= 4 {
                if args[3].eq_ignore_ascii_case(b"withvalues") {
                    with_values = true;
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
            }
            if let Some(queued) =
                session.enqueue_op(hash::hrandfield(session, &args[1], count, with_values))
            {
                replies.push(queued);
            }
        }
        b"hstrlen" => {
            if args.len() != 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hstrlen' command",
                );
                return replies;
            }
            if let Some(queued) =
                session.enqueue_op(hash::hstrlen(session, &args[1], &args[2]))
            {
                replies.push(queued);
            }
        }
        b"hscan" => {
            if args.len() < 3 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'hscan' command",
                );
                return replies;
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
                            error(
                                session,
                                &mut replies,
                                "ERR value is not an integer or out of range",
                            );
                            return replies;
                        }
                    }
                } else {
                    error(session, &mut replies, "ERR syntax error");
                    return replies;
                }
                i += 1;
            }
            if let Some(queued) =
                session.enqueue_op(hash::hscan(session, &args[1], pattern, count))
            {
                replies.push(queued);
            }
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
            error(
                session,
                &mut replies,
                "ERR Command not allowed inside a transaction",
            );
            return replies;
        }
        b"publish" | b"spublish" => {
            // SPUBLISH is a thin alias for PUBLISH in single-node mode.
            // TODO: distinguish SPUBLISH properly if horizontal scaling is added.
            if args.len() != 3 {
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
            // PUBLISH / SPUBLISH are allowed inside MULTI (they queue and
            // execute normally — no special-casing).
            let channel = args[1].clone();
            let payload = args[2].clone();
            let pubsub_reg = session.pubsub();
            // Execute immediately (bypass QueuedOp machinery for PUBLISH
            // since it is pure in-memory state with no DB side effects).
            if session.in_multi() {
                // Inside MULTI: return +QUEUED and defer actual publish to EXEC.
                // We implement this as a wire-only op that executes the publish
                // at commit time.
                let reply = session.enqueue_op(crate::pubsub_cmds::publish_op(
                    pubsub_reg, channel, payload,
                ));
                if let Some(q) = reply {
                    replies.push(q);
                }
            } else {
                let count = pubsub_reg.publish(&channel, payload);
                replies.push(RespValue::Integer(count));
                return replies;
            }
        }
        b"pubsub" => {
            if args.len() < 2 {
                error(
                    session,
                    &mut replies,
                    "ERR wrong number of arguments for 'pubsub' command",
                );
                return replies;
            }
            let sub_cmd: Vec<u8> = args[1].iter().map(u8::to_ascii_lowercase).collect();
            match sub_cmd.as_slice() {
                b"channels" => {
                    let pat = args.get(2).map(|b| b.as_ref());
                    let pubsub_reg = session.pubsub();
                    let mut channels = pubsub_reg.active_channels(pat);
                    channels.sort();
                    replies.push(RespValue::Array(Some(
                        channels
                            .into_iter()
                            .map(|c| RespValue::BulkString(Some(c)))
                            .collect(),
                    )));
                    return replies;
                }
                b"numsub" => {
                    let pubsub_reg = session.pubsub();
                    let channel_args: Vec<Bytes> = args[2..].to_vec();
                    let counts = pubsub_reg.numsub(&channel_args);
                    let mut flat = Vec::with_capacity(counts.len() * 2);
                    for (ch, count) in counts {
                        flat.push(RespValue::BulkString(Some(ch)));
                        flat.push(RespValue::Integer(count));
                    }
                    replies.push(RespValue::Array(Some(flat)));
                    return replies;
                }
                b"numpat" => {
                    let pubsub_reg = session.pubsub();
                    replies.push(RespValue::Integer(pubsub_reg.numpat()));
                    return replies;
                }
                b"help" => {
                    replies.push(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:"))),
                        RespValue::BulkString(Some(Bytes::from_static(b"CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all)."))),
                        RespValue::BulkString(Some(Bytes::from_static(b"NUMSUB [<channel> ...] -- Return listen count for channels."))),
                        RespValue::BulkString(Some(Bytes::from_static(b"NUMPAT -- Return the number of active patterns."))),
                    ])));
                    return replies;
                }
                _ => {
                    error(
                        session,
                        &mut replies,
                        format!(
                            "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                            String::from_utf8_lossy(&sub_cmd)
                        ),
                    );
                    return replies;
                }
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
            vec![ok()]
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
            vec![ok()]
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
            vec![ok()]
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
            vec![RespValue::Array(Some(vec![ok(), bulk("1")]))]
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
            vec![ok()]
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
            vec![ok()]
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
        assert_eq!(dispatch(&mut session, &["sync"]).await, vec![ok()]);
        assert_eq!(dispatch(&mut session, &["psync"]).await, vec![ok()]);
        assert_eq!(dispatch(&mut session, &["wait"]).await, vec![ok()]);
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
        assert_eq!(dispatch(&mut session, &["save"]).await, vec![ok()]);
        assert_eq!(dispatch(&mut session, &["bgsave"]).await, vec![ok()]);
        // DBSIZE counts the current DB.
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        dispatch(&mut session, &["set", "foo", "bar"]).await;
        dispatch(&mut session, &["set", "baz", "qux"]).await;
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(2)]);
        // SELECT then DBSIZE is scoped to the new DB.
        assert_eq!(dispatch(&mut session, &["select", "3"]).await, vec![ok()]);
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        // Select back to DB 0: still two keys.
        assert_eq!(dispatch(&mut session, &["select", "0"]).await, vec![ok()]);
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
            vec![RespValue::Array(Some(vec![ok()]))]
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn quit_replies_ok_and_requests_close() {
        let mut session = test_session();
        assert_eq!(
            dispatch(&mut session, &["quit"]).await,
            vec![ok()]
        );
        assert!(session.should_close());
    }

    #[tokio::test]
    async fn flushall_and_flushdb_through_dispatch() {
        let mut session = test_session();
        dispatch(&mut session, &["set", "keep", "1"]).await;
        assert_eq!(
            dispatch(&mut session, &["flushdb"]).await,
            vec![ok()]
        );
        assert_eq!(dispatch(&mut session, &["dbsize"]).await, vec![int(0)]);
        dispatch(&mut session, &["set", "other", "2"]).await;
        assert_eq!(
            dispatch(&mut session, &["flushall"]).await,
            vec![ok()]
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
                vec![RespValue::Error(Bytes::from(format!(
                    "This server does not support {} execution inside MULTI",
                    cmd[0].to_ascii_uppercase()
                )))]
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
            vec![RespValue::Array(Some(vec![ok(), ok()]))]
        );
        // The SELECT-scoped write landed in db1, not db0.
        assert_eq!(
            dispatch(&mut session, &["select", "0"]).await,
            vec![ok()]
        );
        assert_eq!(
            dispatch(&mut session, &["get", "multi_select_key"]).await,
            vec![RespValue::BulkString(None)]
        );
        assert_eq!(
            dispatch(&mut session, &["select", "1"]).await,
            vec![ok()]
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
