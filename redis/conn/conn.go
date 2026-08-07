// Redis commands related to connection & server management implemented here
// https://redis.io/docs/latest/operate/rs/references/compatibility/commands/connection/
// https://redis.io/docs/latest/operate/rs/references/compatibility/commands/server/
package conn

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// Many connection & server are stubbed for client compatibility as
// documented in /COMPATIBILITY.md
var noOpCmd = func(conn redcon.Conn, _ any, _ error) {
	conn.WriteString("OK")
}

func connectionId(conn redcon.Conn) uint64 {
	return (conn.Context()).(*common.Session).Id
}

func setCurrentDb(conn redcon.Conn, dbIndex int) {
	(conn.Context()).(*common.Session).SwitchDB(dbIndex)
}

// TODO this should live somewhere common but the priority for now is
// to move all connection/server mgmt commands to queued ops
func checkExactArgs(conn redcon.Conn, args [][]byte, n int) bool {
	if len(args) != n {
		conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return false
	}
	return true
}

func currentDb(conn redcon.Conn) int {
	if conn == nil {
		return 0 // default for testing
	}
	return (conn.Context()).(*common.Session).CurrentDB()
}

func Echo(args [][]byte) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		if len(args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'echo' command")
		} else {
			conn.WriteBulkString(string(args[1]))
		}
	}
}

func Ping(args [][]byte) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		if len(args) > 1 {
			conn.WriteBulkString(string(args[1]))
		} else {
			conn.WriteString("PONG")
		}
	}
}

func Client(args [][]byte, session *common.Session) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		if len(args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
			return
		}
		subCmd := strings.ToLower(string(args[1]))
		switch subCmd {
		default:
			conn.WriteError("subcommand not supported")
		case "id":
			conn.WriteUint64(session.Id)
		case "info":
			infoString := "id=" + strconv.FormatUint(connectionId(conn), 10) + " db=" + strconv.Itoa(currentDb(conn)) + "\r\n"
			conn.WriteBulkString(infoString)
			return
		}
	}
}

func Select(args [][]byte, conn redcon.Conn) common.QueuedOp {
	return common.QueuedOp{
		IsMutating: true,
		DbOp: func(tx kv.Tx) (any, error) {
			if len(args) != 2 {
				return nil, errors.New("Invalid arguments")
			}
			dbIndex, err := strconv.Atoi(string(args[1]))
			if err != nil || dbIndex < 0 {
				return nil, errors.New("Invalid DB index")
			}

			setCurrentDb(conn, dbIndex)
			return nil, nil
		},
		WireOp: func(conn redcon.Conn, _ any, err error) {
			if err != nil {
				conn.WriteError("")
			}
			conn.WriteString("OK")
		},
	}
}

// since Invar only runs in single-writer mode, 'replication' is meaningless,
// however we're implementing to keep client compatibility
func Sync() common.WireOp {
	return noOpCmd
}

func Wait() common.WireOp {
	return noOpCmd
}

func Lolwut(version string, commit string) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		conn.WriteBulkString(fmt.Sprintf("Invar version: %s, commit: %s\n", version, commit))
	}
}

func Time() common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		now := time.Now()
		sec := now.Unix()
		micro := now.Nanosecond() / 1000
		conn.WriteArray(2)
		conn.WriteBulkString(strconv.FormatInt(sec, 10))
		conn.WriteBulkString(strconv.FormatInt(int64(micro), 10))
	}
}

func Module(args [][]byte) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		if len(args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'module' command")
			return
		}
		if strings.ToLower(string(args[1])) == "list" {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR unknown subcommand")
		}
	}
}

// NOTE: this is O(n) as opposed to O(1) in Redis!
// Do not use this routinely in production!
func DbSize(session *common.Session) common.QueuedOp {
	return common.QueuedOp{
		IsMutating: false,
		DbOp: func(tx kv.Tx) (any, error) {
			it := tx.NewIterator(session.Prefix())
			defer it.Close()
			var count int64
			for it.Next() {
				count++
			}
			return count, it.Err()
		},
		WireOp: func(conn redcon.Conn, value any, err error) {
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR: %v", err))
			} else {
				count := value.(int64)
				conn.WriteInt64(count)
			}
		},
	}
}

func Save(session *common.Session) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		if err := session.KVS().Sync(); err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteString("OK")
		}
	}
}

func BgSave(session *common.Session) common.WireOp {
	return func(conn redcon.Conn, _ any, _ error) {
		go session.KVS().Sync()
		conn.WriteString("OK")
	}
}
