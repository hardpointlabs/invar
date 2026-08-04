package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/config"
	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/bitmap"
	"github.com/hardpointlabs/invar/redis/bloom"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/hash"
	"github.com/hardpointlabs/invar/redis/hll"
	"github.com/hardpointlabs/invar/redis/keys"
	"github.com/hardpointlabs/invar/redis/list"
	"github.com/hardpointlabs/invar/redis/set"
	redisstrings "github.com/hardpointlabs/invar/redis/strings"
	"github.com/hardpointlabs/invar/redis/zset"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

var addr = ":6379"

// key delimeters
const prefixSeparator = ":"

// public redis types for LSM tree entries (not private/internal types)
const (
	RedisString byte = iota
	RedisList
	RedisSet
	RedisSortedSet
	RedisHash
	RedisStream
	RedisVectorSet
	RedisBloom
	RedisJSON
)

func currentDbPrefix(conn redcon.Conn) []byte {
	return []byte(strconv.Itoa(currentDb(conn)) + prefixSeparator)
}

// rawKeyPrefix builds the public key prefix "{dbSlot}:{keyName}" for user-accessible keys.
func rawKeyPrefix(keyName []byte, dbSlot int) []byte {
	return append([]byte(strconv.Itoa(dbSlot)+prefixSeparator), keyName...)
}

// copyItemValue safely copies a Badger item's value into a new []byte.
func copyItemValue(item *badger.Item) ([]byte, error) {
	var out []byte
	err := item.Value(func(val []byte) error {
		out = append([]byte{}, val...)
		return nil
	})
	return out, err
}

// readUint32Sentinel reads a 4-byte big-endian uint32 from a public sentinel key.

func upsertSession(conn redcon.Conn, kvs kv.KeyValueStore) *common.Session {
	if ctx := conn.Context(); ctx != nil {
		return ctx.(*common.Session)
	}
	session := common.NewSession(kvs)
	conn.SetContext(session)
	return session
}

func connectionId(conn redcon.Conn) uint64 {
	return (conn.Context()).(*common.Session).Id
}

func currentDb(conn redcon.Conn) int {
	if conn == nil {
		return 0 // default for testing
	}
	return (conn.Context()).(*common.Session).CurrentDB()
}

func setCurrentDb(conn redcon.Conn, dbIndex int) {
	(conn.Context()).(*common.Session).SwitchDB(dbIndex)
}

// RedisListener implements the main.Listener interface for the Redis wire protocol.
type RedisListener struct {
	Ln net.Listener
}

func (l *RedisListener) Serve(ctx context.Context, db *badger.DB) error {
	go func() {
		<-ctx.Done()
		l.Ln.Close()
	}()
	return serve(l.Ln, db)
}

func dispatchCommand(session *common.Session, conn redcon.Conn, cmd redcon.Command, db *badger.DB, ps *redcon.PubSub) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "select":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		dbIndex, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil || dbIndex < 0 {
			conn.WriteError("ERR invalid DB index")
			return
		}
		setCurrentDb(conn, dbIndex)
		conn.WriteString("OK")
	case "echo":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'echo' command")
		} else {
			conn.WriteBulkString(string(cmd.Args[1]))
		}
	case "ping":
		if len(cmd.Args) > 1 {
			conn.WriteBulkString(string(cmd.Args[1]))
		} else {
			conn.WriteString("PONG")
		}
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "client":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		subCmd := strings.ToLower(string(cmd.Args[1]))
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
	case "bitcount":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		key := cmd.Args[1]
		useBit := false
		var startGiven, endGiven bool
		var startVal, endVal int
		i := 2
		if i < len(cmd.Args) {
			v, ok := parseIntArg(conn, cmd.Args[i])
			if ok {
				startVal = v
				startGiven = true
				i++
			}
		}
		if i < len(cmd.Args) {
			v, ok := parseIntArg(conn, cmd.Args[i])
			if ok {
				endVal = v
				endGiven = true
				i++
			}
		}
		if i < len(cmd.Args) {
			unit := strings.ToLower(string(cmd.Args[i]))
			if unit == "bit" {
				useBit = true
			} else if unit != "byte" {
				conn.WriteError("ERR syntax error")
				return
			}
			i++
		}
		if i < len(cmd.Args) {
			conn.WriteError("ERR syntax error")
			return
		}
		if startGiven != endGiven {
			conn.WriteError("ERR syntax error")
			return
		}
		session.EnqueueOp(bitmap.BitCount(session, key, startGiven, endGiven, startVal, endVal, useBit))
	case "bitop":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for 'bitop' command")
			return
		}
		op, ok := bitmap.ParseBitOp(string(cmd.Args[1]))
		if !ok {
			conn.WriteError("ERR syntax error")
			return
		}
		if op == bitmap.BitOpNOT && len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for 'bitop' command")
			return
		}
		destKey := cmd.Args[2]
		srcKeys := cmd.Args[3:]
		session.EnqueueOp(bitmap.BitOp(session, destKey, op, srcKeys))
	case "bitpos":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		bit, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		if bit != 0 && bit != 1 {
			conn.WriteError("ERR bit is not an integer or out of range")
			return
		}
		useBit := false
		var startGiven bool
		var startVal, endVal int
		i := 3
		if i < len(cmd.Args) {
			v, ok := parseIntArg(conn, cmd.Args[i])
			if ok {
				startVal = v
				startGiven = true
				i++
			}
		}
		if i < len(cmd.Args) {
			v, ok := parseIntArg(conn, cmd.Args[i])
			if ok {
				endVal = v
				i++
			}
		}
		if i < len(cmd.Args) {
			unit := strings.ToLower(string(cmd.Args[i]))
			if unit == "bit" {
				useBit = true
			} else if unit != "byte" {
				conn.WriteError("ERR syntax error")
				return
			}
			i++
		}
		if i < len(cmd.Args) {
			conn.WriteError("ERR syntax error")
			return
		}
		session.EnqueueOp(bitmap.BitPos(session, cmd.Args[1], bit, startGiven, startVal, endVal, useBit))
	case "bgsave":
		go db.Sync()
		conn.WriteString("OK")
	case "module":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'module' command")
			return
		}
		if strings.ToLower(string(cmd.Args[1])) == "list" {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR unknown subcommand")
		}
	case "save":
		if err := db.Sync(); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "sync", "psync":
		// since Invar only runs in a single process, 'replication' is meaningless, however
		// we're implementing to avoid breaking callers who expect these commands
		conn.WriteString("OK")
	case "wait":
		conn.WriteString("OK")
	case "lolwut":
		conn.WriteBulkString(fmt.Sprintf("Invar version: %s, commit: %s\n", config.Version, config.Commit))
	case "time":
		now := time.Now()
		sec := now.Unix()
		micro := now.Nanosecond() / 1000
		conn.WriteArray(2)
		conn.WriteBulkString(strconv.FormatInt(sec, 10))
		conn.WriteBulkString(strconv.FormatInt(int64(micro), 10))
	case "flushall":
		err := db.DropAll()
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "flushdb":
		err := db.DropPrefix(currentDbPrefix(conn))
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "dbsize":
		// NOTE: this is O(n) as opposed to O(1) in redis!
		// Do not use this routinely in production!
		db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.PrefetchSize = 100
			opts.Prefix = currentDbPrefix(conn)
			it := txn.NewIterator(opts)
			defer it.Close()
			var count int64 = 0
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			conn.WriteInt64(count)
			return nil
		})
	case "exists":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.Exists(session, cmd.Args[1:]...))
	case "set":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(redisstrings.Set(session, cmd.Args[1], cmd.Args[2]))
	case "setbit":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		offset, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		if offset < 0 {
			conn.WriteError("ERR bit offset is not an integer or out of range")
			return
		}
		value, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		if value != 0 && value != 1 {
			conn.WriteError("ERR bit is not an integer or out of range")
			return
		}
		session.EnqueueOp(bitmap.SetBit(session, cmd.Args[1], offset, value))
	case "setex":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		sec, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.SetEx(session, cmd.Args[1], cmd.Args[3], sec))
	case "strlen":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.Strlen(session, cmd.Args[1]))
	case "substr":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		end, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.Substr(session, cmd.Args[1], start, end))
	case "getbit":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		offset, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		if offset < 0 {
			conn.WriteError("ERR bit offset is not an integer or out of range")
			return
		}
		session.EnqueueOp(bitmap.GetBit(session, cmd.Args[1], offset))
	case "get":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.Get(session, cmd.Args[1]))
	case "mget":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.MGet(session, cmd.Args[1:]...))
	case "getset":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(redisstrings.GetSet(session, cmd.Args[1], cmd.Args[2]))
	case "getdel":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.GetDel(session, cmd.Args[1]))
	case "move":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		targetDb, ok := parseIntArg(conn, cmd.Args[2])
		if !ok || targetDb < 0 {
			conn.WriteError("ERR invalid DB index")
			return
		}
		session.EnqueueOp(keys.Move(session, cmd.Args[1], targetDb))
	case "rename":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(keys.Rename(session, cmd.Args[1], cmd.Args[2]))
	case "renamenx":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(keys.RenameNX(session, cmd.Args[1], cmd.Args[2]))
	case "object":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		switch strings.ToLower(string(cmd.Args[1])) {
		case "idletime":
			conn.WriteNull()
		default:
			conn.WriteError("ERR unknown subcommand")
		}
	case "setnx":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(redisstrings.SetNX(session, cmd.Args[1], cmd.Args[2]))
	case "pttl":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.PTTL(session, cmd.Args[1]))
	case "ttl":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.TTL(session, cmd.Args[1]))
	case "expire":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		seconds, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(keys.Expire(session, cmd.Args[1], seconds))
	case "incr":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.Increment(session, cmd.Args[1], 1))
	case "incrby":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		amount, ok := parseInt64Arg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.Increment(session, cmd.Args[1], amount))
	case "decr":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.Increment(session, cmd.Args[1], -1))
	case "decrby":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		amount, ok := parseInt64Arg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.Increment(session, cmd.Args[1], -amount))
	case "append":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(redisstrings.Append(session, cmd.Args[1], cmd.Args[2]))
	case "getex":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(redisstrings.GetEx(session, cmd.Args[1:]...))
	case "getrange":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		end, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.Substr(session, cmd.Args[1], start, end))
	case "incrbyfloat":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		amount, ok := parseFloatArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.IncrByFloat(session, cmd.Args[1], amount))
	case "mset":
		if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for 'mset' command")
			return
		}
		session.EnqueueOp(redisstrings.MSet(session, cmd.Args[1:]...))
	case "msetnx":
		if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for 'msetnx' command")
			return
		}
		session.EnqueueOp(redisstrings.MSetNX(session, cmd.Args[1:]...))
	case "psetex":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		ms, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(redisstrings.PSetEx(session, cmd.Args[1], cmd.Args[3], ms))
	case "setrange":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		offset, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		if offset < 0 {
			conn.WriteError("ERR offset is out of range")
			return
		}
		session.EnqueueOp(redisstrings.SetRange(session, cmd.Args[1], offset, cmd.Args[3]))
	case "type":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.Type(session, cmd.Args[1]))
	case "del", "unlink":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(keys.Del(session, cmd.Args[1:]...))
	case "lpush":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(list.LPush(session, cmd.Args[1], cmd.Args[2:]...))
	case "rpush":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(list.RPush(session, cmd.Args[1], cmd.Args[2:]...))
	case "lpop":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(list.LPop(session, cmd.Args[1]))
	case "rpop":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(list.RPop(session, cmd.Args[1]))
	case "llen":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(list.LLen(session, cmd.Args[1]))
	case "lrange":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		session.EnqueueOp(list.LRange(session, cmd.Args[1], start, stop))
	case "lindex":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		index, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(list.LIndex(session, cmd.Args[1], index))
	case "lset":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		index, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(list.LSet(session, cmd.Args[1], index, cmd.Args[3]))
	case "lrem":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		count, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(list.LRem(session, cmd.Args[1], count, cmd.Args[3]))
	case "ltrim":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		session.EnqueueOp(list.LTrim(session, cmd.Args[1], start, stop))
	case "linsert":
		if !checkExactArgs(conn, cmd, 5) {
			return
		}
		before := strings.ToLower(string(cmd.Args[2])) == "before"
		session.EnqueueOp(list.LInsert(session, cmd.Args[1], before, cmd.Args[3], cmd.Args[4]))
	case "lpushx":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(list.LPushX(session, cmd.Args[1], cmd.Args[2]))
	case "rpushx":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(list.RPushX(session, cmd.Args[1], cmd.Args[2]))
	case "hset":
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for 'hset' command")
			return
		}
		session.EnqueueOp(hash.HSet(session, cmd.Args[1], cmd.Args[2:]...))
	case "hsetnx":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(hash.HSetNX(session, cmd.Args[1], cmd.Args[2], cmd.Args[3]))
	case "hget":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hash.HGet(session, cmd.Args[1], cmd.Args[2]))
	case "hdel":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hash.HDel(session, cmd.Args[1], cmd.Args[2:]...))
	case "hexists":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hash.HExists(session, cmd.Args[1], cmd.Args[2]))
	case "hlen":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hash.HLen(session, cmd.Args[1]))
	case "hmget":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hash.HMGet(session, cmd.Args[1], cmd.Args[2:]...))
	case "hmset":
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for 'hmset' command")
			return
		}
		session.EnqueueOp(hash.HMSet(session, cmd.Args[1], cmd.Args[2:]...))
	case "hkeys":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hash.HKeys(session, cmd.Args[1]))
	case "hvals":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hash.HVals(session, cmd.Args[1]))
	case "hgetall":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hash.HGetAll(session, cmd.Args[1]))
	case "hincrby":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		incrAmount, incrOk := parseInt64Arg(conn, cmd.Args[3])
		if !incrOk {
			return
		}
		session.EnqueueOp(hash.HIncrBy(session, cmd.Args[1], cmd.Args[2], incrAmount))
	case "hincrbyfloat":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		floatAmount, floatOk := parseFloatArg(conn, cmd.Args[3])
		if !floatOk {
			return
		}
		session.EnqueueOp(hash.HIncrByFloat(session, cmd.Args[1], cmd.Args[2], floatAmount))
	case "hrandfield":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		var count int = 1
		var withValues bool
		if len(cmd.Args) >= 3 {
			var ok bool
			count, ok = parseIntArg(conn, cmd.Args[2])
			if !ok {
				return
			}
		}
		if len(cmd.Args) >= 4 {
			if strings.ToLower(string(cmd.Args[3])) == "withvalues" {
				withValues = true
			}
		}
		session.EnqueueOp(hash.HRandField(session, cmd.Args[1], count, withValues))
	case "hstrlen":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hash.HStrLen(session, cmd.Args[1], cmd.Args[2]))
	case "hscan":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		_, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		var pattern string
		var count int
		for i := 3; i < len(cmd.Args); i++ {
			switch strings.ToLower(string(cmd.Args[i])) {
			case "match":
				i++
				if i < len(cmd.Args) {
					pattern = string(cmd.Args[i])
				}
			case "count":
				i++
				if i < len(cmd.Args) {
					c, err := strconv.Atoi(string(cmd.Args[i]))
					if err == nil {
						count = c
					}
				}
			}
		}
		session.EnqueueOp(hash.HScan(session, cmd.Args[1], pattern, count))
	case "sadd":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SAdd(session, cmd.Args[1], cmd.Args[2:]...))
	case "srem":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SRem(session, cmd.Args[1], cmd.Args[2:]...))
	case "scard":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SCard(session, cmd.Args[1]))
	case "smembers":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SMembers(session, cmd.Args[1]))
	case "sismember":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SIsMember(session, cmd.Args[1], cmd.Args[2]))
	case "spop":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SPop(session, cmd.Args[1]))
	case "srandmember":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SRandMember(session, cmd.Args[1], 1))
	case "smove":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(set.SMove(session, cmd.Args[1], cmd.Args[2], cmd.Args[3]))
	case "sdiff":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SDiff(session, cmd.Args[1:]...))
	case "sinter":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SInter(session, cmd.Args[1:]...))
	case "sunion":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(set.SUnion(session, cmd.Args[1:]...))
	case "sdiffstore":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SDiffStore(session, cmd.Args[1], cmd.Args[2:]...))
	case "sinterstore":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SInterStore(session, cmd.Args[1], cmd.Args[2:]...))
	case "sunionstore":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(set.SUnionStore(session, cmd.Args[1], cmd.Args[2:]...))
	case "zadd":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(zset.ZAdd(session, cmd.Args[1], cmd.Args[2:]...))
	case "zcard":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(zset.ZCard(session, cmd.Args[1]))
	case "zcount":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(zset.ZCount(session, cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3])))
	case "zincrby":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		incr, ok := parseFloatArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		session.EnqueueOp(zset.ZIncrBy(session, cmd.Args[1], incr, cmd.Args[3]))
	case "zinter":
		fallthrough
	case "zinterstore":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		isStore := strings.ToLower(string(cmd.Args[0])) == "zinterstore"
		argStart := 1
		if isStore {
			argStart = 2
		}
		numKeys, ok := parseIntArg(conn, cmd.Args[argStart])
		if !ok {
			return
		}
		if len(cmd.Args) < argStart+1+numKeys {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		keys := cmd.Args[argStart+1 : argStart+1+numKeys]
		i := argStart + 1 + numKeys
		var weights []float64
		aggregate := "SUM"
		for i < len(cmd.Args) {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "weights" {
				i++
				for j := 0; j < numKeys && i < len(cmd.Args); j++ {
					w, ok := parseFloatArg(conn, cmd.Args[i])
					if !ok {
						return
					}
					weights = append(weights, w)
					i++
				}
				if len(weights) != numKeys {
					conn.WriteError("ERR weight count does not match number of keys")
					return
				}
			} else if arg == "aggregate" {
				i++
				if i >= len(cmd.Args) {
					conn.WriteError("ERR syntax error")
					return
				}
				aggregate = string(cmd.Args[i])
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					conn.WriteError("ERR syntax error")
					return
				}
				i++
			} else if arg == "withscores" && strings.ToLower(string(cmd.Args[0])) == "zinter" {
				i++
			} else {
				conn.WriteError("ERR syntax error")
				return
			}
		}
		if isStore {
			session.EnqueueOp(zset.ZInterStore(session, cmd.Args[1], aggregate, weights, keys...))
		} else {
			hasWithScores := false
			for _, arg := range cmd.Args {
				if strings.EqualFold(string(arg), "withscores") {
					hasWithScores = true
					break
				}
			}
			session.EnqueueOp(zset.ZInter(session, aggregate, weights, hasWithScores, keys...))
		}
	case "zlexcount":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(zset.ZLexCount(session, cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3])))
	case "zpopmax":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		popCount := 1
		if len(cmd.Args) >= 3 {
			var ok bool
			popCount, ok = parseIntArg(conn, cmd.Args[2])
			if !ok || popCount < 0 {
				if !ok {
					return
				}
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
		}
		session.EnqueueOp(zset.ZPopMax(session, cmd.Args[1], popCount))
	case "zpopmin":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		popCount := 1
		if len(cmd.Args) >= 3 {
			var ok bool
			popCount, ok = parseIntArg(conn, cmd.Args[2])
			if !ok || popCount < 0 {
				if !ok {
					return
				}
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
		}
		session.EnqueueOp(zset.ZPopMin(session, cmd.Args[1], popCount))
	case "zrange":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		withScores := false
		if len(cmd.Args) >= 5 && strings.EqualFold(string(cmd.Args[4]), "withscores") {
			withScores = true
		}
		session.EnqueueOp(zset.ZRange(session, cmd.Args[1], start, stop, withScores))
	case "zrangebylex":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		minStr := string(cmd.Args[2])
		maxStr := string(cmd.Args[3])
		limitOffset, limitCount := 0, 0
		hasLimit := false
		if len(cmd.Args) >= 7 && strings.EqualFold(string(cmd.Args[4]), "limit") {
			var ok bool
			limitOffset, ok = parseIntArg(conn, cmd.Args[5])
			if !ok {
				return
			}
			limitCount, ok = parseIntArg(conn, cmd.Args[6])
			if !ok {
				return
			}
			hasLimit = true
		}
		session.EnqueueOp(zset.ZRangeByLex(session, cmd.Args[1], minStr, maxStr, limitOffset, limitCount, hasLimit))
	case "zrangebyscore":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		minStr := string(cmd.Args[2])
		maxStr := string(cmd.Args[3])
		withScores := false
		limitOffset, limitCount := 0, 0
		hasLimit := false
		for i := 4; i < len(cmd.Args); i++ {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "withscores" {
				withScores = true
			} else if arg == "limit" && i+2 < len(cmd.Args) {
				var ok bool
				limitOffset, ok = parseIntArg(conn, cmd.Args[i+1])
				if !ok {
					return
				}
				limitCount, ok = parseIntArg(conn, cmd.Args[i+2])
				if !ok {
					return
				}
				hasLimit = true
				i += 2
			}
		}
		session.EnqueueOp(zset.ZRangeByScore(session, cmd.Args[1], minStr, maxStr, withScores, limitOffset, limitCount, hasLimit))
	case "zrank":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(zset.ZRank(session, cmd.Args[1], cmd.Args[2]))
	case "zrem":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(zset.ZRem(session, cmd.Args[1], cmd.Args[2:]...))
	case "zremrangebylex":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(zset.ZRemRangeByLex(session, cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3])))
	case "zremrangebyrank":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		session.EnqueueOp(zset.ZRemRangeByRank(session, cmd.Args[1], start, stop))
	case "zremrangebyscore":
		if !checkExactArgs(conn, cmd, 4) {
			return
		}
		session.EnqueueOp(zset.ZRemRangeByScore(session, cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3])))
	case "zrevrange":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		withScores := false
		if len(cmd.Args) >= 5 && strings.EqualFold(string(cmd.Args[4]), "withscores") {
			withScores = true
		}
		session.EnqueueOp(zset.ZRevRange(session, cmd.Args[1], start, stop, withScores))
	case "zrevrangebylex":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		maxStr := string(cmd.Args[2])
		minStr := string(cmd.Args[3])
		limitOffset, limitCount := 0, 0
		hasLimit := false
		if len(cmd.Args) >= 7 && strings.EqualFold(string(cmd.Args[4]), "limit") {
			var ok bool
			limitOffset, ok = parseIntArg(conn, cmd.Args[5])
			if !ok {
				return
			}
			limitCount, ok = parseIntArg(conn, cmd.Args[6])
			if !ok {
				return
			}
			hasLimit = true
		}
		session.EnqueueOp(zset.ZRevRangeByLex(session, cmd.Args[1], maxStr, minStr, limitOffset, limitCount, hasLimit))
	case "zrevrangebyscore":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		maxStr := string(cmd.Args[2])
		minStr := string(cmd.Args[3])
		withScores := false
		limitOffset, limitCount := 0, 0
		hasLimit := false
		for i := 4; i < len(cmd.Args); i++ {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "withscores" {
				withScores = true
			} else if arg == "limit" && i+2 < len(cmd.Args) {
				var ok bool
				limitOffset, ok = parseIntArg(conn, cmd.Args[i+1])
				if !ok {
					return
				}
				limitCount, ok = parseIntArg(conn, cmd.Args[i+2])
				if !ok {
					return
				}
				hasLimit = true
				i += 2
			}
		}
		session.EnqueueOp(zset.ZRevRangeByScore(session, cmd.Args[1], maxStr, minStr, withScores, limitOffset, limitCount, hasLimit))
	case "zrevrank":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(zset.ZRevRank(session, cmd.Args[1], cmd.Args[2]))
	case "zscore":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(zset.ZScore(session, cmd.Args[1], cmd.Args[2]))
	case "zdiff":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		numKeys, ok := parseIntArg(conn, cmd.Args[1])
		if !ok {
			return
		}
		if len(cmd.Args) < 2+numKeys {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		keys := cmd.Args[2 : 2+numKeys]
		hasWithScores := false
		if len(cmd.Args) > 2+numKeys && strings.EqualFold(string(cmd.Args[2+numKeys]), "withscores") {
			hasWithScores = true
		}
		session.EnqueueOp(zset.ZDiff(session, hasWithScores, keys...))
	case "zdiffstore":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		numKeys, ok := parseIntArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		if len(cmd.Args) < 3+numKeys {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		keys := cmd.Args[3 : 3+numKeys]
		session.EnqueueOp(zset.ZDiffStore(session, cmd.Args[1], keys...))
	case "zmscore":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(zset.ZMScore(session, cmd.Args[1], cmd.Args[2:]...))
	case "zrandmember":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		count := 1
		if len(cmd.Args) >= 3 {
			var ok bool
			count, ok = parseIntArg(conn, cmd.Args[2])
			if !ok {
				return
			}
		}
		session.EnqueueOp(zset.ZRandMember(session, cmd.Args[1], count))
	case "zunion":
		fallthrough
	case "zunionstore":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		isStore := strings.ToLower(string(cmd.Args[0])) == "zunionstore"
		argStart := 1
		if isStore {
			argStart = 2
		}
		numKeys, ok := parseIntArg(conn, cmd.Args[argStart])
		if !ok {
			return
		}
		if len(cmd.Args) < argStart+1+numKeys {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		keys := cmd.Args[argStart+1 : argStart+1+numKeys]
		i := argStart + 1 + numKeys
		var weights []float64
		aggregate := "SUM"
		for i < len(cmd.Args) {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "weights" {
				i++
				for j := 0; j < numKeys && i < len(cmd.Args); j++ {
					w, ok := parseFloatArg(conn, cmd.Args[i])
					if !ok {
						return
					}
					weights = append(weights, w)
					i++
				}
				if len(weights) != numKeys {
					conn.WriteError("ERR weight count does not match number of keys")
					return
				}
			} else if arg == "aggregate" {
				i++
				if i >= len(cmd.Args) {
					conn.WriteError("ERR syntax error")
					return
				}
				aggregate = string(cmd.Args[i])
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					conn.WriteError("ERR syntax error")
					return
				}
				i++
			} else if arg == "withscores" && strings.ToLower(string(cmd.Args[0])) == "zunion" {
				i++
			} else {
				conn.WriteError("ERR syntax error")
				return
			}
		}
		if isStore {
			session.EnqueueOp(zset.ZUnionStore(session, cmd.Args[1], aggregate, weights, keys...))
		} else {
			hasWithScores := false
			for _, arg := range cmd.Args {
				if strings.EqualFold(string(arg), "withscores") {
					hasWithScores = true
					break
				}
			}
			session.EnqueueOp(zset.ZUnion(session, aggregate, weights, hasWithScores, keys...))
		}
	case "zrangestore":
		if !checkExactArgs(conn, cmd, 5) {
			return
		}
		start, ok := parseIntArg(conn, cmd.Args[3])
		if !ok {
			return
		}
		stop, ok := parseIntArg(conn, cmd.Args[4])
		if !ok {
			return
		}
		session.EnqueueOp(zset.ZRangeStore(session, cmd.Args[1], cmd.Args[2], start, stop))
	case "pfadd":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(hll.Pfadd(session, cmd.Args[1], cmd.Args[2:]...))
	case "pfcount":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hll.Pfcount(session, cmd.Args[1:]...))
	case "pfmerge":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(hll.Pfmerge(session, cmd.Args[1], cmd.Args[2:]...))
	case "bf.reserve":
		if !checkMinArgs(conn, cmd, 4) {
			return
		}
		errRate, ok := parseFloatArg(conn, cmd.Args[2])
		if !ok {
			return
		}
		capacity, ok := parseIntArg(conn, cmd.Args[3])
		if !ok || capacity < 1 {
			conn.WriteError("ERR capacity must be positive")
			return
		}
		expansion := 2
		nonScaling := false
		for i := 4; i < len(cmd.Args); i++ {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "expansion" && i+1 < len(cmd.Args) {
				i++
				v, ok := parseIntArg(conn, cmd.Args[i])
				if !ok {
					return
				}
				expansion = v
			} else if arg == "nonscaling" {
				nonScaling = true
			}
		}
		session.EnqueueOp(bloom.Bfreserve(session, cmd.Args[1], errRate, uint64(capacity), expansion, nonScaling))
	case "bf.add":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(bloom.Bfadd(session, cmd.Args[1], cmd.Args[2]))
	case "bf.exists":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(bloom.Bfexists(session, cmd.Args[1], cmd.Args[2]))
	case "bf.madd":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(bloom.Bfmadd(session, cmd.Args[1], cmd.Args[2:]))
	case "bf.mexists":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		session.EnqueueOp(bloom.Bfmexists(session, cmd.Args[1], cmd.Args[2:]))
	case "bf.insert":
		if !checkMinArgs(conn, cmd, 3) {
			return
		}
		info := &bloom.InsertInfo{}
		i := 2
		for i < len(cmd.Args) {
			arg := strings.ToLower(string(cmd.Args[i]))
			if arg == "capacity" && i+1 < len(cmd.Args) {
				i++
				v, ok := parseIntArg(conn, cmd.Args[i])
				if !ok {
					return
				}
				info.Capacity = uint64(v)
			} else if arg == "error" && i+1 < len(cmd.Args) {
				i++
				v, ok := parseFloatArg(conn, cmd.Args[i])
				if !ok {
					return
				}
				info.Error = v
			} else if arg == "expansion" && i+1 < len(cmd.Args) {
				i++
				v, ok := parseIntArg(conn, cmd.Args[i])
				if !ok {
					return
				}
				info.Expansion = v
			} else if arg == "nocreate" {
				info.NoCreate = true
			} else if arg == "nonscaling" {
				info.NonScaling = true
			} else if arg == "items" {
				i++
				info.Items = cmd.Args[i:]
				break
			} else {
				conn.WriteError("ERR syntax error at " + string(cmd.Args[i]))
				return
			}
			i++
		}
		if len(info.Items) == 0 {
			conn.WriteError("ERR ITEMS argument required")
			return
		}
		session.EnqueueOp(bloom.Bfinsert(session, cmd.Args[1], info))
	case "bf.info":
		if !checkExactArgs(conn, cmd, 2) {
			return
		}
		session.EnqueueOp(bloom.Bfinfo(session, cmd.Args[1]))
	case "json.set":
		handleJSONSet(conn, db, cmd)
	case "json.get":
		handleJSONGet(conn, db, cmd)
	case "json.del":
		handleJSONDel(conn, db, cmd)
	case "json.type":
		handleJSONType(conn, db, cmd)
	case "json.arrappend":
		handleJSONArrAppend(conn, db, cmd)
	case "json.arrindex":
		handleJSONArrIndex(conn, db, cmd)
	case "json.arrlen":
		handleJSONArrLen(conn, db, cmd)
	case "json.numincrby":
		handleJSONNumIncrBy(conn, db, cmd)
	case "json.nummultby":
		handleJSONNumMultBy(conn, db, cmd)
	case "json.objkeys":
		handleJSONObjKeys(conn, db, cmd)
	case "json.objlen":
		handleJSONObjLen(conn, db, cmd)
	case "json.strappend":
		handleJSONStrAppend(conn, db, cmd)
	case "json.strlen":
		handleJSONStrLen(conn, db, cmd)
	case "json.mget":
		handleJSONMGet(conn, db, cmd)
	case "json.resp":
		handleJSONResp(conn, db, cmd)
	case "json.clear":
		handleJSONClear(conn, db, cmd)
	case "json.arrpop":
		handleJSONArrPop(conn, db, cmd)
	case "json.arrtrim":
		handleJSONArrTrim(conn, db, cmd)
	case "json.arrinsert":
		handleJSONArrInsert(conn, db, cmd)
	case "publish":
		if !checkExactArgs(conn, cmd, 3) {
			return
		}
		conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
	case "subscribe", "psubscribe":
		if !checkMinArgs(conn, cmd, 2) {
			return
		}
		command := strings.ToLower(string(cmd.Args[0]))
		for i := 1; i < len(cmd.Args); i++ {
			if command == "psubscribe" {
				ps.Psubscribe(conn, string(cmd.Args[i]))
			} else {
				ps.Subscribe(conn, string(cmd.Args[i]))
			}
		}
	}
	session.DispatchPendingOps(conn)
}

func serve(ln net.Listener, db *badger.DB) error {
	var ps redcon.PubSub
	kvs := kv.WrapBadger(db)
	log.Info().Msgf("started RESP protocol listener at %s", addr)
	err := redcon.Serve(ln,
		func(conn redcon.Conn, cmd redcon.Command) {
			session := upsertSession(conn, kvs)
			dispatchCommand(session, conn, cmd, db, &ps)
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	return err
}
