//! Server-level commands that answer "server level" queries without touching
//! the key-value store: `INFO`, `HELLO`, `CLIENT` and friends.
//!
//! Port of the Go `redis/server` package. Their purpose is to let third-party
//! clients (and libraries such as BullMQ) probe this daemon's capabilities, so
//! the responses favour compatibility over exhaustiveness.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use bytes::Bytes;
use kv::kv::{BoxFuture, Tx};
use crate::common::DbOp;
use crate::common::op::{DbError, DbResult, DefaultWire, NoOp, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::{conn, RedisStore};
use crate::resp::RespValue;

/// The Redis wire-protocol version Invar claims compatibility with. Reported
/// in INFO so client libraries gate their behaviour on it; must be a valid
/// semver string.
pub const REDIS_VERSION: &str = "6.2.0";

/// Default TCP port reported by INFO until the listener records its real
/// bind address.
const DEFAULT_TCP_PORT: i64 = 6379;

static SERVER_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// 20 random bytes, hex-encoded, generated once at startup.
static RUN_ID: LazyLock<String> = LazyLock::new(|| {
    let mut b = [0u8; 20];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut b);
    hex_encode(&b)
});

static TCP_PORT: AtomicI64 = AtomicI64::new(DEFAULT_TCP_PORT);
static CONNECTED_CLIENTS: AtomicI64 = AtomicI64::new(0);
static TOTAL_CONNECTIONS_RECEIVED: AtomicI64 = AtomicI64::new(0);

/// Records the listen address so INFO can report the real TCP port.
pub fn set_addr(addr: SocketAddr) {
    TCP_PORT.store(addr.port() as i64, Ordering::Relaxed);
}

/// Must be called for every accepted connection.
pub fn conn_opened() {
    TOTAL_CONNECTIONS_RECEIVED.fetch_add(1, Ordering::Relaxed);
    CONNECTED_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

/// Must be called for every closed connection.
pub fn conn_closed() {
    CONNECTED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
}

/// Answers the `INFO` command. Values that would require global
/// instrumentation are reported as plausible constants; the fields that
/// third-party libraries actually rely on (redis_version, maxmemory_policy,
/// loading) are accurate.
pub fn info() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(FixedReply {
            reply: RespValue::BulkString(Some(Bytes::from(info_string()))),
        }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct SaveOp {
    store: Arc<dyn RedisStore>,
}

impl DbOp for SaveOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            self.store.sync().await?;
            Ok(Box::new(()) as DbResult)
        })
    }
}

pub fn save(session: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SaveOp { store: session.store() }),
        wire_op: Box::new(DefaultWire),
        is_mutating: false,
        allowed_in_tx: false,
    }
}

struct FlushAllOp {
    store: Arc<dyn RedisStore>,
}

impl DbOp for FlushAllOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            self.store.destroy().await?;
            Ok(Box::new(()) as DbResult)
        })
    }
}

pub fn flushall(session: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(FlushAllOp { store: session.store() }),
        wire_op: Box::new(DefaultWire),
        is_mutating: false,
        allowed_in_tx: false,
    }
}

struct FlushDbOp {
    store: Arc<dyn RedisStore>,
    prefix: Bytes,
}

impl DbOp for FlushDbOp {
    fn run<'a>(&'a self, _tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        Box::pin(async move {
            self.store.drop_prefix(self.prefix.as_ref()).await?;
            Ok(Box::new(()) as DbResult)
        })
    }
}

pub fn flushdb(session: &Session) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(FlushDbOp { store: session.store(), prefix: Bytes::from(session.prefix()) }),
        wire_op: Box::new(DefaultWire),
        is_mutating: false,
        allowed_in_tx: false,
    }
}

/// Builds the raw INFO payload.
fn info_string() -> String {
    let mut b = String::new();

    b.push_str("# Server\r\n");
    b.push_str(&format!("redis_version:{REDIS_VERSION}\r\n"));
    b.push_str(&format!("invar_version:{}\r\n", conn::VERSION));
    b.push_str("redis_git_sha1:00000000\r\n");
    b.push_str("redis_git_dirty:0\r\n");
    b.push_str("redis_build_id:0000000000000000\r\n");
    b.push_str("redis_mode:standalone\r\n");
    b.push_str(&format!(
        "os:{} {}\r\n",
        std::env::consts::OS,
        std::env::consts::ARCH
    ));
    b.push_str("arch_bits:64\r\n");
    b.push_str("multiplexing_api:epoll\r\n");
    b.push_str(&format!("process_id:{}\r\n", std::process::id()));
    b.push_str(&format!("run_id:{}\r\n", RUN_ID.as_str()));
    b.push_str(&format!(
        "tcp_port:{}\r\n",
        TCP_PORT.load(Ordering::Relaxed)
    ));
    b.push_str(&format!(
        "uptime_in_seconds:{}\r\n",
        SERVER_START.elapsed().as_secs()
    ));
    b.push_str(&format!(
        "uptime_in_days:{}\r\n",
        SERVER_START.elapsed().as_secs() / 86400
    ));
    b.push_str("hz:10\r\n");
    b.push_str("configured_hz:10\r\n");
    b.push_str("lru_clock:0\r\n");
    b.push_str("executable:invar\r\n");
    b.push_str("config_file:\r\n");
    b.push_str("io_threads_active:0\r\n");

    b.push_str("\r\n# Clients\r\n");
    b.push_str(&format!(
        "connected_clients:{}\r\n",
        CONNECTED_CLIENTS.load(Ordering::Relaxed)
    ));
    b.push_str("cluster_connections:0\r\n");
    b.push_str("maxclients:10000\r\n");
    b.push_str("client_recent_max_input_buffer:0\r\n");
    b.push_str("client_recent_max_output_buffer:0\r\n");
    b.push_str("blocked_clients:0\r\n");
    b.push_str("tracking_clients:0\r\n");
    b.push_str("pubsub_clients:0\r\n");
    b.push_str("watching_clients:0\r\n");
    b.push_str("clients_in_timeout_table:0\r\n");
    b.push_str("total_watched_keys:0\r\n");
    b.push_str("total_blocking_keys:0\r\n");
    b.push_str("total_blocking_keys_on_nokey:0\r\n");

    b.push_str("\r\n# Memory\r\n");
    b.push_str("used_memory:0\r\n");
    b.push_str("used_memory_human:0B\r\n");
    b.push_str("used_memory_rss:0\r\n");
    b.push_str("used_memory_peak:0\r\n");
    b.push_str("used_memory_peak_human:0B\r\n");
    b.push_str("used_memory_lua:0\r\n");
    b.push_str("maxmemory:0\r\n");
    b.push_str("maxmemory_human:0B\r\n");
    b.push_str("maxmemory_policy:noeviction\r\n");
    b.push_str("mem_fragmentation_ratio:1.00\r\n");
    b.push_str("mem_allocator:libc\r\n");

    b.push_str("\r\n# Persistence\r\n");
    b.push_str("loading:0\r\n");
    b.push_str("async_loading:0\r\n");
    b.push_str("rdb_changes_since_last_save:0\r\n");
    b.push_str("rdb_bgsave_in_progress:0\r\n");
    b.push_str(&format!(
        "rdb_last_save_time:{}\r\n",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    ));
    b.push_str("rdb_last_bgsave_status:ok\r\n");
    b.push_str("rdb_last_bgsave_time_sec:0\r\n");
    b.push_str("rdb_current_bgsave_time_sec:-1\r\n");
    b.push_str("rdb_saves:0\r\n");
    b.push_str("aof_enabled:0\r\n");
    b.push_str("aof_rewrite_in_progress:0\r\n");
    b.push_str("aof_rewrite_scheduled:0\r\n");
    b.push_str("aof_last_rewrite_time_sec:-1\r\n");
    b.push_str("aof_current_rewrite_time_sec:-1\r\n");
    b.push_str("aof_last_bgrewrite_status:ok\r\n");
    b.push_str("aof_rewrites:0\r\n");

    b.push_str("\r\n# Stats\r\n");
    b.push_str(&format!(
        "total_connections_received:{}\r\n",
        TOTAL_CONNECTIONS_RECEIVED.load(Ordering::Relaxed)
    ));
    b.push_str("total_commands_processed:0\r\n");
    b.push_str("instantaneous_ops_per_sec:0\r\n");
    b.push_str("total_net_input_bytes:0\r\n");
    b.push_str("total_net_output_bytes:0\r\n");
    b.push_str("rejected_connections:0\r\n");
    b.push_str("sync_full:0\r\n");
    b.push_str("sync_partial_ok:0\r\n");
    b.push_str("sync_partial_err:0\r\n");
    b.push_str("expired_keys:0\r\n");
    b.push_str("evicted_keys:0\r\n");
    b.push_str("keyspace_hits:0\r\n");
    b.push_str("keyspace_misses:0\r\n");
    b.push_str("pubsub_channels:0\r\n");
    b.push_str("pubsub_patterns:0\r\n");
    b.push_str("latest_fork_usec:0\r\n");
    b.push_str("total_forks:0\r\n");

    b.push_str("\r\n# Replication\r\n");
    b.push_str("role:master\r\n");
    b.push_str("connected_slaves:0\r\n");
    b.push_str("master_failover_state:no-failover\r\n");
    b.push_str(&format!("master_replid:{}\r\n", RUN_ID.as_str()));
    b.push_str("master_replid2:0000000000000000000000000000000000000000\r\n");
    b.push_str("master_repl_offset:0\r\n");
    b.push_str("second_repl_offset:-1\r\n");
    b.push_str("repl_backlog_active:0\r\n");
    b.push_str("repl_backlog_size:1048576\r\n");
    b.push_str("repl_backlog_first_byte_offset:0\r\n");
    b.push_str("repl_backlog_histlen:0\r\n");

    b.push_str("\r\n# CPU\r\n");
    b.push_str("used_cpu_sys:0.000000\r\n");
    b.push_str("used_cpu_user:0.000000\r\n");
    b.push_str("used_cpu_sys_children:0.000000\r\n");
    b.push_str("used_cpu_user_children:0.000000\r\n");
    b.push_str("used_cpu_sys_main_thread:0.000000\r\n");
    b.push_str("used_cpu_user_main_thread:0.000000\r\n");

    b.push_str("\r\n# Modules\r\n");

    b.push_str("\r\n# Cluster\r\n");
    b.push_str("cluster_enabled:0\r\n");

    b.push_str("\r\n# Keyspace\r\n");

    b
}

/// Negotiates the RESP protocol version. Invar only speaks RESP2, so a
/// request for any other version (including RESP3) is refused with a NOPROTO
/// error, which is what a RESP2-only server is expected to do and lets clients
/// such as ioredis fall back cleanly to RESP2.
pub fn hello(session: &mut Session, args: &[Bytes]) -> QueuedOp {
    let reply = hello_reply(session, args);
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(FixedReply { reply }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

fn hello_reply(session: &mut Session, args: &[Bytes]) -> RespValue {
    let mut proto: i64 = 2;
    let mut rest = &args[1..];
    while let Some(token) = rest.first() {
        let token_upper: Vec<u8> = token.iter().map(u8::to_ascii_uppercase).collect();
        match token_upper.as_slice() {
            b"AUTH" => {
                // Invar has no authentication configured; accept and ignore
                // the supplied credentials so client handshakes do not fail.
                if rest.len() < 3 {
                    return err("ERR wrong number of arguments for 'hello' command");
                }
                rest = &rest[3..];
            }
            b"SETNAME" => {
                if rest.len() < 2 {
                    return err("ERR wrong number of arguments for 'hello' command");
                }
                session.set_client_name(String::from_utf8_lossy(&rest[1]).into_owned());
                rest = &rest[2..];
            }
            _ => {
                let Ok(v) = std::str::from_utf8(token).unwrap_or("").parse::<i64>() else {
                    return err("ERR syntax error in 'hello' command");
                };
                proto = v;
                rest = &rest[1..];
            }
        }
    }
    if proto != 2 {
        return RespValue::Error(Bytes::from_static(b"NOPROTO unsupported protocol version"));
    }

    let id = session.id().to_string();
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"server"))),
        RespValue::BulkString(Some(Bytes::from_static(b"redis"))),
        RespValue::BulkString(Some(Bytes::from_static(b"version"))),
        RespValue::BulkString(Some(Bytes::from(conn::VERSION))),
        RespValue::BulkString(Some(Bytes::from_static(b"proto"))),
        RespValue::Integer(2),
        RespValue::BulkString(Some(Bytes::from_static(b"id"))),
        RespValue::BulkString(Some(Bytes::from(id))),
        RespValue::BulkString(Some(Bytes::from_static(b"mode"))),
        RespValue::BulkString(Some(Bytes::from_static(b"standalone"))),
        RespValue::BulkString(Some(Bytes::from_static(b"role"))),
        RespValue::BulkString(Some(Bytes::from_static(b"master"))),
        RespValue::BulkString(Some(Bytes::from_static(b"modules"))),
        RespValue::Array(Some(Vec::new())),
    ]))
}

/// Implements the `CLIENT` subcommand family: per-connection bookkeeping that
/// does not touch the key-value store.
pub fn client(session: &mut Session, args: &[Bytes]) -> QueuedOp {
    let reply = client_reply(session, args);
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(FixedReply { reply }),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

fn client_reply(session: &mut Session, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'client' command");
    }
    let sub: Vec<u8> = args[1].iter().map(u8::to_ascii_lowercase).collect();
    match sub.as_slice() {
        b"id" => RespValue::Integer(session.id() as i64),
        b"info" => RespValue::BulkString(Some(Bytes::from(client_info_string(session)))),
        b"list" => RespValue::BulkString(Some(Bytes::from(client_info_string(session)))),
        b"setname" => {
            if args.len() != 3 {
                return err("ERR wrong number of arguments for 'client|setname' command");
            }
            let name = String::from_utf8_lossy(&args[2]).into_owned();
            if name.contains([' ', '\n', '\r']) {
                return RespValue::Error(Bytes::from_static(
                    b"ERR Client names cannot contain spaces, newlines or special characters.",
                ));
            }
            session.set_client_name(name);
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        }
        b"getname" => {
            let name = session.client_name();
            if name.is_empty() {
                RespValue::BulkString(None)
            } else {
                RespValue::BulkString(Some(Bytes::copy_from_slice(name.as_bytes())))
            }
        }
        b"setinfo" => {
            if args.len() != 4 {
                return err("ERR wrong number of arguments for 'client|setinfo' command");
            }
            let attr: Vec<u8> = args[2].iter().map(u8::to_ascii_uppercase).collect();
            match attr.as_slice() {
                b"LIB-NAME" => {
                    session.set_lib_name(String::from_utf8_lossy(&args[3]).into_owned());
                    RespValue::SimpleString(Bytes::from_static(b"OK"))
                }
                b"LIB-VER" => {
                    session.set_lib_ver(String::from_utf8_lossy(&args[3]).into_owned());
                    RespValue::SimpleString(Bytes::from_static(b"OK"))
                }
                _ => RespValue::Error(Bytes::from(format!(
                    "ERR Unrecognized option '{}'",
                    String::from_utf8_lossy(&args[2])
                ))),
            }
        }
        b"getinfo" => {
            if args.len() != 3 {
                return err("ERR wrong number of arguments for 'client|getinfo' command");
            }
            let attr: Vec<u8> = args[2].iter().map(u8::to_ascii_uppercase).collect();
            match attr.as_slice() {
                b"LIB-NAME" => RespValue::BulkString(Some(Bytes::copy_from_slice(
                    session.lib_name().as_bytes(),
                ))),
                b"LIB-VER" => RespValue::BulkString(Some(Bytes::copy_from_slice(
                    session.lib_ver().as_bytes(),
                ))),
                _ => RespValue::Error(Bytes::from(format!(
                    "ERR Unrecognized option '{}'",
                    String::from_utf8_lossy(&args[2])
                ))),
            }
        }
        _ => RespValue::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try CLIENT HELP.",
            String::from_utf8_lossy(&args[1])
        ))),
    }
}

/// Builds the `CLIENT INFO`/`CLIENT LIST` payload for the current connection.
fn client_info_string(session: &Session) -> String {
    let addr = session
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    format!(
        "id={} addr={addr} laddr={addr} fd=-1 name={} \
         age=0 idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 \
         qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 redir=-1 resp=2 user=default \
         lib-name={} lib-ver={} tot-net-in=0 tot-net-out=0 events=r cmd=client",
        session.id(),
        session.client_name(),
        session.current_db(),
        session.lib_name(),
        session.lib_ver(),
    )
}

/// A wire-only op that replies a value computed at enqueue time.
struct FixedReply {
    reply: RespValue,
}

impl WireOp for FixedReply {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        self.reply.clone()
    }
}

fn err(msg: &str) -> RespValue {
    RespValue::Error(Bytes::copy_from_slice(msg.as_bytes()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    fn cmd(args: &[&'static [u8]]) -> Vec<Bytes> {
        args.iter().map(|a| Bytes::from_static(a)).collect()
    }

    fn exec(op: QueuedOp) -> RespValue {
        let result: Result<DbResult, DbError> = Ok(Box::new(()));
        op.wire_op.reply(result)
    }

    #[test]
    fn info_reports_key_fields() {
        let reply = info_string();
        for want in [
            "# Server",
            "redis_version:6.2.0",
            "redis_mode:standalone",
            "maxmemory_policy:noeviction",
            "loading:0",
            "role:master",
            "# Keyspace",
        ] {
            assert!(reply.contains(want), "INFO output missing {want:?}");
        }
        assert!(reply.contains("invar_version:"), "missing invar_version");
        assert!(reply.starts_with("# Server\r\n"));
    }

    #[test]
    fn hello_no_args_replies_resp2_map() {
        let mut session = test_session();
        let reply = exec(hello(&mut session, &cmd(&[b"HELLO"])));
        match &reply {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 14);
                assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from_static(b"server"))));
                assert_eq!(items[1], RespValue::BulkString(Some(Bytes::from_static(b"redis"))));
                assert_eq!(items[4], RespValue::BulkString(Some(Bytes::from_static(b"proto"))));
                assert_eq!(items[5], RespValue::Integer(2));
                assert_eq!(items[6], RespValue::BulkString(Some(Bytes::from_static(b"id"))));
                assert_eq!(
                    items[7],
                    RespValue::BulkString(Some(Bytes::from(session.id().to_string())))
                );
                assert_eq!(items[12], RespValue::BulkString(Some(Bytes::from_static(b"modules"))));
                assert_eq!(items[13], RespValue::Array(Some(Vec::new())));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn hello_rejects_resp3() {
        let mut session = test_session();
        let reply = exec(hello(&mut session, &cmd(&[b"HELLO", b"3"])));
        assert_eq!(
            reply,
            RespValue::Error(Bytes::from_static(b"NOPROTO unsupported protocol version"))
        );
    }

    #[test]
    fn hello_sets_name_and_ignores_auth() {
        let mut session = test_session();
        let reply = exec(hello(
            &mut session,
            &cmd(&[b"HELLO", b"2", b"AUTH", b"default", b"pw", b"SETNAME", b"bull:conn"]),
        ));
        assert_eq!(session.client_name(), "bull:conn");
        match &reply {
            RespValue::Array(Some(items)) => assert_eq!(items[5], RespValue::Integer(2)),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn client_setname_getname() {
        let mut session = test_session();
        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"SETNAME", b"worker-1"])));
        assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"OK")));

        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"GETNAME"])));
        assert_eq!(
            reply,
            RespValue::BulkString(Some(Bytes::from_static(b"worker-1")))
        );
    }

    #[test]
    fn client_getname_unset_is_null() {
        let mut session = test_session();
        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"GETNAME"])));
        assert_eq!(reply, RespValue::BulkString(None));
    }

    #[test]
    fn client_setinfo_getinfo() {
        let mut session = test_session();
        let reply = exec(client(
            &mut session,
            &cmd(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"ioredis"]),
        ));
        assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"OK")));
        let reply = exec(client(
            &mut session,
            &cmd(&[b"CLIENT", b"SETINFO", b"LIB-VER", b"6.0.0"]),
        ));
        assert_eq!(reply, RespValue::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(session.lib_name(), "ioredis");
        assert_eq!(session.lib_ver(), "6.0.0");

        let reply = exec(client(
            &mut session,
            &cmd(&[b"CLIENT", b"GETINFO", b"LIB-VER"]),
        ));
        assert_eq!(
            reply,
            RespValue::BulkString(Some(Bytes::from_static(b"6.0.0")))
        );
    }

    #[test]
    fn client_id() {
        let mut session = test_session();
        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"ID"])));
        assert_eq!(reply, RespValue::Integer(session.id() as i64));
    }

    #[test]
    fn client_info_contains_bookkeeping_fields() {
        let mut session = test_session();
        session.set_client_name("conn-1".into());
        session.set_lib_name("ioredis".into());
        session.set_lib_ver("6.0.0".into());
        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"INFO"])));
        match &reply {
            RespValue::BulkString(Some(info)) => {
                let info = String::from_utf8_lossy(info).to_string();
                assert!(info.starts_with("id="), "got {info:?}");
                for want in ["name=conn-1", "lib-name=ioredis", "lib-ver=6.0.0", "db=0"] {
                    assert!(info.contains(want), "CLIENT INFO missing {want:?} in {info:?}");
                }
            }
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn client_setname_rejects_spaces() {
        let mut session = test_session();
        let reply = exec(client(
            &mut session,
            &cmd(&[b"CLIENT", b"SETNAME", b"bad name"]),
        ));
        assert!(
            matches!(&reply, RespValue::Error(_)),
            "SETNAME with spaces should error, got {reply:?}"
        );
    }

    #[test]
    fn client_wrong_arity_and_unknown_subcommand() {
        let mut session = test_session();
        let reply = exec(client(&mut session, &cmd(&[b"CLIENT"])));
        assert_eq!(
            reply,
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'client' command"
            ))
        );

        let reply = exec(client(&mut session, &cmd(&[b"CLIENT", b"bogus"])));
        assert!(
            matches!(&reply, RespValue::Error(e) if e.starts_with(b"ERR unknown subcommand 'bogus'")),
            "got {reply:?}"
        );
    }
}
