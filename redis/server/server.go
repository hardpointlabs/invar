// Package server implements commands that answer "server level" queries
// without touching the key-value store: INFO, HELLO, CLIENT and friends.
// Their purpose is to let third-party clients (and libraries such as BullMQ)
// probe this daemon's capabilities, so the responses favour compatibility
// over exhaustiveness.
package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hardpointlabs/invar/config"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

var serverStart = time.Now()

var runID = func() string {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}()

var tcpPort int64 = 6379

// redisVersion is the Redis wire-protocol version Invar claims compatibility
// with. It is reported in INFO so client libraries gate their behaviour on it;
// it must be a valid semver string.
const redisVersion = "6.2.0"

var (
	connectedClients      atomic.Int64
	totalConnectionsCount atomic.Int64
)

// SetAddr records the listen address so INFO can report the real TCP port.
func SetAddr(addr string) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	if p, err := strconv.Atoi(port); err == nil {
		tcpPort = int64(p)
	}
}

// ConnOpened must be called for every accepted connection.
func ConnOpened() {
	totalConnectionsCount.Add(1)
	connectedClients.Add(1)
}

// ConnClosed must be called for every closed connection.
func ConnClosed() {
	connectedClients.Add(-1)
}

// Info answers the INFO command. Values that would require global
// instrumentation are reported as plausible constants; the fields that
// third-party libraries actually rely on (redis_version, maxmemory_policy,
// loading) are accurate.
func Info(session *common.Session, conn redcon.Conn) {
	var b strings.Builder

	b.WriteString("# Server\r\n")
	fmt.Fprintf(&b, "redis_version:%s\r\n", redisVersion)
	fmt.Fprintf(&b, "invar_version:%s\r\n", config.Version)
	b.WriteString("redis_git_sha1:00000000\r\n")
	b.WriteString("redis_git_dirty:0\r\n")
	b.WriteString("redis_build_id:0000000000000000\r\n")
	b.WriteString("redis_mode:standalone\r\n")
	fmt.Fprintf(&b, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
	b.WriteString("arch_bits:64\r\n")
	b.WriteString("multiplexing_api:epoll\r\n")
	fmt.Fprintf(&b, "process_id:%d\r\n", os.Getpid())
	fmt.Fprintf(&b, "run_id:%s\r\n", runID)
	fmt.Fprintf(&b, "tcp_port:%d\r\n", tcpPort)
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int(time.Since(serverStart).Seconds()))
	fmt.Fprintf(&b, "uptime_in_days:%d\r\n", int(time.Since(serverStart).Hours()/24))
	b.WriteString("hz:10\r\n")
	b.WriteString("configured_hz:10\r\n")
	b.WriteString("lru_clock:0\r\n")
	b.WriteString("executable:invar\r\n")
	b.WriteString("config_file:\r\n")
	b.WriteString("io_threads_active:0\r\n")

	b.WriteString("\r\n# Clients\r\n")
	fmt.Fprintf(&b, "connected_clients:%d\r\n", connectedClients.Load())
	b.WriteString("cluster_connections:0\r\n")
	b.WriteString("maxclients:10000\r\n")
	b.WriteString("client_recent_max_input_buffer:0\r\n")
	b.WriteString("client_recent_max_output_buffer:0\r\n")
	b.WriteString("blocked_clients:0\r\n")
	b.WriteString("tracking_clients:0\r\n")
	b.WriteString("pubsub_clients:0\r\n")
	b.WriteString("watching_clients:0\r\n")
	b.WriteString("clients_in_timeout_table:0\r\n")
	b.WriteString("total_watched_keys:0\r\n")
	b.WriteString("total_blocking_keys:0\r\n")
	b.WriteString("total_blocking_keys_on_nokey:0\r\n")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b.WriteString("\r\n# Memory\r\n")
	fmt.Fprintf(&b, "used_memory:%d\r\n", m.Alloc)
	fmt.Fprintf(&b, "used_memory_human:%s\r\n", humanBytes(m.Alloc))
	b.WriteString("used_memory_rss:0\r\n")
	b.WriteString("used_memory_peak:0\r\n")
	b.WriteString("used_memory_peak_human:0B\r\n")
	b.WriteString("used_memory_lua:0\r\n")
	b.WriteString("maxmemory:0\r\n")
	b.WriteString("maxmemory_human:0B\r\n")
	b.WriteString("maxmemory_policy:noeviction\r\n")
	b.WriteString("mem_fragmentation_ratio:1.00\r\n")
	b.WriteString("mem_allocator:libc\r\n")

	b.WriteString("\r\n# Persistence\r\n")
	b.WriteString("loading:0\r\n")
	b.WriteString("async_loading:0\r\n")
	b.WriteString("rdb_changes_since_last_save:0\r\n")
	b.WriteString("rdb_bgsave_in_progress:0\r\n")
	fmt.Fprintf(&b, "rdb_last_save_time:%d\r\n", time.Now().Unix())
	b.WriteString("rdb_last_bgsave_status:ok\r\n")
	b.WriteString("rdb_last_bgsave_time_sec:0\r\n")
	b.WriteString("rdb_current_bgsave_time_sec:-1\r\n")
	b.WriteString("rdb_saves:0\r\n")
	b.WriteString("aof_enabled:0\r\n")
	b.WriteString("aof_rewrite_in_progress:0\r\n")
	b.WriteString("aof_rewrite_scheduled:0\r\n")
	b.WriteString("aof_last_rewrite_time_sec:-1\r\n")
	b.WriteString("aof_current_rewrite_time_sec:-1\r\n")
	b.WriteString("aof_last_bgrewrite_status:ok\r\n")
	b.WriteString("aof_rewrites:0\r\n")

	b.WriteString("\r\n# Stats\r\n")
	fmt.Fprintf(&b, "total_connections_received:%d\r\n", totalConnectionsCount.Load())
	b.WriteString("total_commands_processed:0\r\n")
	b.WriteString("instantaneous_ops_per_sec:0\r\n")
	b.WriteString("total_net_input_bytes:0\r\n")
	b.WriteString("total_net_output_bytes:0\r\n")
	b.WriteString("rejected_connections:0\r\n")
	b.WriteString("sync_full:0\r\n")
	b.WriteString("sync_partial_ok:0\r\n")
	b.WriteString("sync_partial_err:0\r\n")
	b.WriteString("expired_keys:0\r\n")
	b.WriteString("evicted_keys:0\r\n")
	b.WriteString("keyspace_hits:0\r\n")
	b.WriteString("keyspace_misses:0\r\n")
	b.WriteString("pubsub_channels:0\r\n")
	b.WriteString("pubsub_patterns:0\r\n")
	b.WriteString("latest_fork_usec:0\r\n")
	b.WriteString("total_forks:0\r\n")

	b.WriteString("\r\n# Replication\r\n")
	b.WriteString("role:master\r\n")
	b.WriteString("connected_slaves:0\r\n")
	b.WriteString("master_failover_state:no-failover\r\n")
	fmt.Fprintf(&b, "master_replid:%s\r\n", runID)
	b.WriteString("master_replid2:0000000000000000000000000000000000000000\r\n")
	b.WriteString("master_repl_offset:0\r\n")
	b.WriteString("second_repl_offset:-1\r\n")
	b.WriteString("repl_backlog_active:0\r\n")
	b.WriteString("repl_backlog_size:1048576\r\n")
	b.WriteString("repl_backlog_first_byte_offset:0\r\n")
	b.WriteString("repl_backlog_histlen:0\r\n")

	b.WriteString("\r\n# CPU\r\n")
	b.WriteString("used_cpu_sys:0.000000\r\n")
	b.WriteString("used_cpu_user:0.000000\r\n")
	b.WriteString("used_cpu_sys_children:0.000000\r\n")
	b.WriteString("used_cpu_user_children:0.000000\r\n")
	b.WriteString("used_cpu_sys_main_thread:0.000000\r\n")
	b.WriteString("used_cpu_user_main_thread:0.000000\r\n")

	b.WriteString("\r\n# Modules\r\n")

	b.WriteString("\r\n# Cluster\r\n")
	b.WriteString("cluster_enabled:0\r\n")

	b.WriteString("\r\n# Keyspace\r\n")

	conn.WriteBulkString(b.String())
}

func humanBytes(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	units := "KMGTPE"
	i := -1
	v := float64(n)
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.2f%c", v, units[i])
}

// Hello negotiates the RESP protocol version. Invar only speaks RESP2, so a
// request for any other version (including RESP3) is refused with a NOPROTO
// error, which is what a RESP2-only server is expected to do and lets clients
// such as ioredis fall back cleanly to RESP2.
func Hello(session *common.Session, conn redcon.Conn, cmd redcon.Command) {
	proto := 2
	args := cmd.Args[1:]
	for len(args) > 0 {
		token := strings.ToUpper(string(args[0]))
		switch token {
		case "AUTH":
			// Invar has no authentication configured; accept and ignore the
			// supplied credentials so client handshakes do not fail.
			if len(args) < 3 {
				conn.WriteError("ERR wrong number of arguments for 'hello' command")
				return
			}
			args = args[3:]
		case "SETNAME":
			if len(args) < 2 {
				conn.WriteError("ERR wrong number of arguments for 'hello' command")
				return
			}
			session.ClientName = string(args[1])
			args = args[2:]
		default:
			v, err := strconv.Atoi(string(args[0]))
			if err != nil {
				conn.WriteError("ERR syntax error in 'hello' command")
				return
			}
			proto = v
			args = args[1:]
		}
	}
	if proto != 2 {
		conn.WriteError("NOPROTO unsupported protocol version")
		return
	}
	conn.WriteArray(14)
	writeField(conn, "server", "redis")
	writeField(conn, "version", config.Version)
	conn.WriteBulkString("proto")
	conn.WriteInt(proto)
	writeField(conn, "id", strconv.FormatUint(session.Id, 10))
	writeField(conn, "mode", "standalone")
	writeField(conn, "role", "master")
	conn.WriteBulkString("modules")
	conn.WriteArray(0)
}

func writeField(conn redcon.Conn, name, value string) {
	conn.WriteBulkString(name)
	conn.WriteBulkString(value)
}

// Client implements the CLIENT subcommand family: per-connection bookkeeping
// that does not touch the key-value store.
func Client(session *common.Session, conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'client' command")
		return
	}
	switch strings.ToLower(string(cmd.Args[1])) {
	default:
		conn.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", string(cmd.Args[1])))
	case "id":
		conn.WriteUint64(session.Id)
	case "info":
		conn.WriteBulkString(clientInfoString(session, conn))
	case "list":
		conn.WriteBulkString(clientInfoString(session, conn))
	case "setname":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		name := string(cmd.Args[2])
		if strings.ContainsAny(name, " \n\r") {
			conn.WriteError("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		session.ClientName = name
		conn.WriteString("OK")
	case "getname":
		if session.ClientName == "" {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(session.ClientName)
		}
	case "setinfo":
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for 'client|setinfo' command")
			return
		}
		attr := strings.ToUpper(string(cmd.Args[2]))
		switch attr {
		case "LIB-NAME":
			session.LibName = string(cmd.Args[3])
			conn.WriteString("OK")
		case "LIB-VER":
			session.LibVer = string(cmd.Args[3])
			conn.WriteString("OK")
		default:
			conn.WriteError("ERR Unrecognized option '" + string(cmd.Args[2]) + "'")
		}
	case "getinfo":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'client|getinfo' command")
			return
		}
		switch strings.ToUpper(string(cmd.Args[2])) {
		case "LIB-NAME":
			conn.WriteBulkString(session.LibName)
		case "LIB-VER":
			conn.WriteBulkString(session.LibVer)
		default:
			conn.WriteError("ERR Unrecognized option '" + string(cmd.Args[2]) + "'")
		}
	}
}

func clientInfoString(session *common.Session, conn redcon.Conn) string {
	var b strings.Builder
	fmt.Fprintf(&b, "id=%d addr=%s laddr=%s fd=-1 name=%s", session.Id, conn.RemoteAddr(), conn.RemoteAddr(), session.ClientName)
	fmt.Fprintf(&b, " age=0 idle=0 flags=N db=%d sub=0 psub=0 ssub=0 multi=-1 watch=0", session.CurrentDB())
	fmt.Fprintf(&b, " qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 redir=-1 resp=2 user=default")
	fmt.Fprintf(&b, " lib-name=%s lib-ver=%s tot-net-in=0 tot-net-out=0 events=r cmd=client", session.LibName, session.LibVer)
	return b.String()
}
