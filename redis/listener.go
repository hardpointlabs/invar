package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/config"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

var addr = ":6379"

// key delimeters
const internalPrefix = "-"
const prefixSeparator = ":"

// public redis types for LSM tree entries (not private/internal types)
type redisValueType byte

const (
	RedisString redisValueType = iota
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
func readUint32Sentinel(txn *badger.Txn, key []byte, dbSlot int) (uint32, error) {
	item, err := txn.Get(rawKeyPrefix(key, dbSlot))
	if err != nil {
		return 0, err
	}
	var count uint32
	if err := item.Value(func(val []byte) error {
		count = binary.BigEndian.Uint32(val)
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// writeUint32Sentinel writes a 4-byte big-endian uint32 to a public sentinel key with the given type meta.
func writeUint32Sentinel(txn *badger.Txn, key []byte, count uint32, typ redisValueType, dbSlot int) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	return txn.SetEntry(badger.NewEntry(rawKeyPrefix(key, dbSlot), buf).WithMeta(byte(typ)))
}

// clearPrefixedKeys deletes all internal keys under prefix, then deletes the sentinel key.
func clearPrefixedKeys(txn *badger.Txn, prefix, sentinelKey []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return txn.Delete(sentinelKey)
}

// writeBulkArray writes a RESP array of bulk strings to conn.
func writeBulkArray(conn redcon.Conn, items [][]byte) {
	conn.WriteArray(len(items))
	for _, item := range items {
		conn.WriteBulk(item)
	}
}

type ClientInfo struct {
	Id uint64
}

func setContext(conn redcon.Conn) {
	var ctx = conn.Context()
	if ctx == nil {
		clientInfo := &ClientInfo{Id: rand.Uint64N(1 << 63)}
		conn.SetContext(clientInfo)
	}
}

var syncMap sync.Map

func connectionId(conn redcon.Conn) uint64 {
	return (conn.Context()).(*ClientInfo).Id
}

func currentDb(conn redcon.Conn) int {
	if conn == nil {
		return 0 // default for testing
	}
	value, _ := syncMap.LoadOrStore(connectionId(conn), 0)
	return value.(int)
}

func setCurrentDb(conn redcon.Conn, dbIndex int) {
	syncMap.Store(connectionId(conn), dbIndex)
}

func getKeys(conn redcon.Conn, db *badger.DB, keys ...[]byte) {
	conn.WriteArray(len(keys))
	_ = db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
			if err != nil {
				conn.WriteNull()
				continue
			}
			valCopy, err := copyItemValue(item)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			conn.WriteBulk(valCopy)
		}
		return nil
	})
}

func moveKey(conn redcon.Conn, db *badger.DB, key []byte, targetDb int) {
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteInt(0)
			return nil
		}
		valCopy, err := copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		// Set the new key
		e := badger.NewEntry(rawKeyPrefix(key, targetDb), valCopy).WithMeta(item.UserMeta())
		err = txn.SetEntry(e)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		// Delete the old key
		err = txn.Delete(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		conn.WriteInt(1)
		return nil
	})
}

func Serve(ctx context.Context, db *badger.DB) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var ps redcon.PubSub
	go log.Info().Msgf("started redis listener at %s", addr)
	err = redcon.Serve(ln,
		func(conn redcon.Conn, cmd redcon.Command) {
			setContext(conn)

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
					conn.WriteUint64((conn.Context()).(*ClientInfo).Id)
				case "info":
					infoString := "id=" + strconv.FormatUint(connectionId(conn), 10) + " db=" + strconv.Itoa(currentDb(conn)) + "\r\n"
					conn.WriteBulkString(infoString)
					return
				}
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
				existsKeys(conn, db, cmd.Args[1:]...)
			case "set":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				setKey(conn, db, cmd.Args[1], cmd.Args[2])
			case "setex":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				sec, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				setKeyWithTTL(conn, db, cmd.Args[1], cmd.Args[3], sec)
			case "strlen":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				strlenKey(conn, db, cmd.Args[1])
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
				substrKey(conn, db, cmd.Args[1], start, end)
			case "get":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				getKey(conn, db, cmd.Args[1])
			case "mget":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				getKeys(conn, db, cmd.Args[1:]...)
			case "getset":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				getSet(conn, db, cmd.Args[1], cmd.Args[2])
			case "getdel":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				getDel(conn, db, cmd.Args[1])
			case "move":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				targetDb, ok := parseIntArg(conn, cmd.Args[2])
				if !ok || targetDb < 0 {
					conn.WriteError("ERR invalid DB index")
					return
				}
				moveKey(conn, db, cmd.Args[1], targetDb)
			case "rename":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				renameKey(conn, db, cmd.Args[1], cmd.Args[2])
			case "renamenx":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				renameNXKey(conn, db, cmd.Args[1], cmd.Args[2])
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
				setNX(conn, db, cmd.Args[1], cmd.Args[2])
			case "pttl":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				pttlKey(conn, db, cmd.Args[1])
			case "ttl":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				ttlKey(conn, db, cmd.Args[1])
			case "expire":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				seconds, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				expireKey(conn, db, cmd.Args[1], seconds)
			case "incr":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				incrementKey(conn, db, cmd.Args[1], 1)
			case "incrby":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				amount, ok := parseInt64Arg(conn, cmd.Args[2])
				if !ok {
					return
				}
				incrementKey(conn, db, cmd.Args[1], amount)
			case "decr":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				incrementKey(conn, db, cmd.Args[1], -1)
			case "decrby":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				amount, ok := parseInt64Arg(conn, cmd.Args[2])
				if !ok {
					return
				}
				incrementKey(conn, db, cmd.Args[1], -amount)
			case "append":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				appendKey(conn, db, cmd.Args[1], cmd.Args[2])
			case "getex":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				getEx(conn, db, cmd.Args[1:]...)
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
				substrKey(conn, db, cmd.Args[1], start, end)
			case "incrbyfloat":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				amount, ok := parseFloatArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				incrByFloat(conn, db, cmd.Args[1], amount)
			case "mset":
				if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
					conn.WriteError("ERR wrong number of arguments for 'mset' command")
					return
				}
				msetKeys(conn, db, cmd.Args[1:]...)
			case "msetnx":
				if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
					conn.WriteError("ERR wrong number of arguments for 'msetnx' command")
					return
				}
				msetnxKeys(conn, db, cmd.Args[1:]...)
			case "psetex":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				ms, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				setKeyWithTTLMs(conn, db, cmd.Args[1], cmd.Args[3], ms)
			case "setrange":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				offset, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				setRangeKey(conn, db, cmd.Args[1], offset, cmd.Args[3])
			case "type":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				typeOfKey(conn, db, cmd.Args[1])
			case "del", "unlink":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				delKeys(conn, db, cmd.Args[1:]...)
			case "lpush":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					_, err := lpush(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				db.View(func(txn *badger.Txn) error {
					size, err := llen(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteInt(size)
					return nil
				})
			case "rpush":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					_, err := rpush(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				db.View(func(txn *badger.Txn) error {
					size, err := llen(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteInt(size)
					return nil
				})
			case "lpop":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				var val []byte
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					var err error
					val, err = lpop(txn, currentDb(conn), cmd.Args[1])
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				if val == nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "rpop":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				var val []byte
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					var err error
					val, err = rpop(txn, currentDb(conn), cmd.Args[1])
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				if val == nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "llen":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					size, err := llen(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteInt(size)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					items, err := lrange(txn, currentDb(conn), cmd.Args[1], start, stop)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					writeBulkArray(conn, items)
					return nil
				})
			case "lindex":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				index, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				db.View(func(txn *badger.Txn) error {
					val, err := lindex(txn, currentDb(conn), cmd.Args[1], index)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					if val == nil {
						conn.WriteNull()
					} else {
						conn.WriteBulk(val)
					}
					return nil
				})
			case "lset":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				index, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					return lset(txn, currentDb(conn), cmd.Args[1], index, cmd.Args[3])
				})
				if err != nil {
					if err == badger.ErrKeyNotFound {
						conn.WriteError("ERR no such key")
					} else {
						conn.WriteError("ERR " + err.Error())
					}
					return
				}
				conn.WriteString("OK")
			case "lrem":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				count, ok := parseIntArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = lrem(txn, currentDb(conn), cmd.Args[1], count, cmd.Args[3])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
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
				err := db.Update(func(txn *badger.Txn) error {
					return ltrim(txn, currentDb(conn), cmd.Args[1], start, stop)
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteString("OK")
			case "linsert":
				if !checkExactArgs(conn, cmd, 5) {
					return
				}
				before := strings.ToLower(string(cmd.Args[2])) == "before"
				var result int
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					var err error
					result, err = linsert(txn, currentDb(conn), cmd.Args[1], before, cmd.Args[3], cmd.Args[4])
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				conn.WriteInt(result)
			case "lpushx":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				var size uint32
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					size, err = lpushx(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(int(size))
			case "rpushx":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				var size uint32
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					size, err = rpushx(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(int(size))
			case "hset":
				if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
					conn.WriteError("ERR wrong number of arguments for 'hset' command")
					return
				}
				var added int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					added, err = hset(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(added)
			case "hsetnx":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				var set int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					set, err = hsetnx(txn, currentDb(conn), cmd.Args[1], cmd.Args[2], cmd.Args[3])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(set)
			case "hget":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					val, err := hget(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteNull()
						return nil
					}
					conn.WriteBulk(val)
					return nil
				})
			case "hdel":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var hdelRemoved int
				var hdelErr error
				hdelErr = db.Update(func(txn *badger.Txn) error {
					var err error
					hdelRemoved, err = hdel(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if hdelErr != nil {
					conn.WriteError("ERR " + hdelErr.Error())
					return
				}
				conn.WriteInt(hdelRemoved)
			case "hexists":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					ok, err := hexists(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if ok {
						conn.WriteInt(1)
					} else {
						conn.WriteInt(0)
					}
					return nil
				})
			case "hlen":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					count, err := hlen(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
			case "hmget":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					results, err := hmget(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(len(results))
					for _, r := range results {
						if r == nil {
							conn.WriteNull()
						} else {
							conn.WriteBulk(r)
						}
					}
					return nil
				})
			case "hmset":
				if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
					conn.WriteError("ERR wrong number of arguments for 'hmset' command")
					return
				}
				var hmsetErr error
				hmsetErr = db.Update(func(txn *badger.Txn) error {
					_, err := hset(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if hmsetErr != nil {
					conn.WriteError("ERR " + hmsetErr.Error())
					return
				}
				conn.WriteString("OK")
			case "hkeys":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					keys, err := hkeys(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, keys)
					return nil
				})
			case "hvals":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					vals, err := hvals(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, vals)
					return nil
				})
			case "hgetall":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					pairs, err := hgetall(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, pairs)
					return nil
				})
			case "hincrby":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				incrAmount, incrOk := parseInt64Arg(conn, cmd.Args[3])
				if !incrOk {
					return
				}
				var newVal int64
				var incrbyErr error
				incrbyErr = db.Update(func(txn *badger.Txn) error {
					var err error
					newVal, err = hincrby(txn, currentDb(conn), cmd.Args[1], cmd.Args[2], incrAmount)
					return err
				})
				if incrbyErr != nil {
					conn.WriteError("ERR " + incrbyErr.Error())
					return
				}
				conn.WriteInt64(newVal)
			case "hincrbyfloat":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				floatAmount, floatOk := parseFloatArg(conn, cmd.Args[3])
				if !floatOk {
					return
				}
				var floatResult string
				var incrFloatErr error
				incrFloatErr = db.Update(func(txn *badger.Txn) error {
					var err error
					floatResult, err = hincrbyfloat(txn, currentDb(conn), cmd.Args[1], cmd.Args[2], floatAmount)
					return err
				})
				if incrFloatErr != nil {
					conn.WriteError("ERR " + incrFloatErr.Error())
					return
				}
				conn.WriteBulkString(floatResult)
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
				db.View(func(txn *badger.Txn) error {
					result, err := hrandfield(txn, currentDb(conn), cmd.Args[1], count, withValues)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if len(cmd.Args) < 3 || count == 1 {
						if len(result) == 0 {
							conn.WriteNull()
						} else {
							conn.WriteBulk(result[0])
						}
					} else {
						conn.WriteArray(len(result))
						for _, r := range result {
							conn.WriteBulk(r)
						}
					}
					return nil
				})
			case "hstrlen":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					length, err := hstrlen(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(length)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					pairs, err := hscan(txn, currentDb(conn), cmd.Args[1], pattern, count)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(2)
					conn.WriteBulkString("0")
					conn.WriteArray(len(pairs))
					for _, p := range pairs {
						conn.WriteBulk(p)
					}
					return nil
				})
			case "sadd":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var added int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					added, err = sadd(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(added)
			case "srem":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = srem(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
			case "scard":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					count, err := scard(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
			case "smembers":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					members, err := smembers(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, members)
					return nil
				})
			case "sismember":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					ok, err := sismember(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if ok {
						conn.WriteInt(1)
					} else {
						conn.WriteInt(0)
					}
					return nil
				})
			case "spop":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				var val []byte
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					var err error
					val, err = spop(txn, currentDb(conn), cmd.Args[1])
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				if val == nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "srandmember":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					members, err := srandmember(txn, currentDb(conn), cmd.Args[1], 1)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if len(members) == 0 {
						conn.WriteNull()
					} else {
						conn.WriteBulk(members[0])
					}
					return nil
				})
			case "smove":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				var moved bool
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					moved, err = smove(txn, currentDb(conn), cmd.Args[1], cmd.Args[2], cmd.Args[3])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				if moved {
					conn.WriteInt(1)
				} else {
					conn.WriteInt(0)
				}
			case "sdiff":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					result, err := sdiff(txn, currentDb(conn), cmd.Args[1:]...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
			case "sinter":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					result, err := sinter(txn, currentDb(conn), cmd.Args[1:]...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
			case "sunion":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					result, err := sunion(txn, currentDb(conn), cmd.Args[1:]...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
			case "sdiffstore":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var count int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					count, err = sdiffstore(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(count)
			case "sinterstore":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var count int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					count, err = sinterstore(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(count)
			case "sunionstore":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var count int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					count, err = sunionstore(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(count)
			case "zadd":
				if !checkMinArgs(conn, cmd, 4) {
					return
				}
				var added int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					added, err = zadd(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				conn.WriteInt(added)
			case "zcard":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					count, err := zcard(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
			case "zcount":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					count, err := zcount(txn, currentDb(conn), cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3]))
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
			case "zincrby":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				incr, ok := parseFloatArg(conn, cmd.Args[2])
				if !ok {
					return
				}
				var newScore float64
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					newScore, err = zincrby(txn, currentDb(conn), cmd.Args[1], incr, cmd.Args[3])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteBulkString(strconv.FormatFloat(newScore, 'f', -1, 64))
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
					db.Update(func(txn *badger.Txn) error {
						m, err := zinter(txn, currentDb(conn), aggregate, keys...)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						if len(weights) > 0 {
							for member, score := range m {
								m[member] = score * weights[0]
							}
						}
						members := zsetToSlice(m)
						count, err := storeZSetResult(txn, currentDb(conn), cmd.Args[1], members)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						conn.WriteInt(count)
						return nil
					})
				} else {
					db.View(func(txn *badger.Txn) error {
						m, err := zinter(txn, currentDb(conn), aggregate, keys...)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						if len(weights) > 0 {
							for member, score := range m {
								m[member] = score * weights[0]
							}
						}
						hasWithScores := false
						for _, arg := range cmd.Args {
							if strings.EqualFold(string(arg), "withscores") {
								hasWithScores = true
								break
							}
						}
						members := zsetToSlice(m)
						if hasWithScores {
							conn.WriteArray(len(members) * 2)
							for _, e := range members {
								conn.WriteBulk(e.member)
								conn.WriteBulkString(strconv.FormatFloat(e.score, 'f', -1, 64))
							}
						} else {
							conn.WriteArray(len(members))
							for _, e := range members {
								conn.WriteBulk(e.member)
							}
						}
						return nil
					})
				}
			case "zlexcount":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					count, err := zlexcount(txn, currentDb(conn), cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3]))
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
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
				db.Update(func(txn *badger.Txn) error {
					popped, err := zpopmax(txn, currentDb(conn), cmd.Args[1], popCount)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(len(popped) * 2)
					for _, e := range popped {
						conn.WriteBulk(e.member)
						conn.WriteBulkString(strconv.FormatFloat(e.score, 'f', -1, 64))
					}
					return nil
				})
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
				db.Update(func(txn *badger.Txn) error {
					popped, err := zpopmin(txn, currentDb(conn), cmd.Args[1], popCount)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(len(popped) * 2)
					for _, e := range popped {
						conn.WriteBulk(e.member)
						conn.WriteBulkString(strconv.FormatFloat(e.score, 'f', -1, 64))
					}
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrange(txn, currentDb(conn), cmd.Args[1], start, stop, withScores)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrangebylex(txn, currentDb(conn), cmd.Args[1], minStr, maxStr, limitOffset, limitCount, hasLimit)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrangebyscore(txn, currentDb(conn), cmd.Args[1], minStr, maxStr, withScores, limitOffset, limitCount, hasLimit)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
			case "zrank":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					rank, found, err := zrank(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if !found {
						conn.WriteNull()
					} else {
						conn.WriteInt(rank)
					}
					return nil
				})
			case "zrem":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = zrem(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
			case "zremrangebylex":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = zremrangebylex(txn, currentDb(conn), cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3]))
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
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
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = zremrangebyrank(txn, currentDb(conn), cmd.Args[1], start, stop)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
			case "zremrangebyscore":
				if !checkExactArgs(conn, cmd, 4) {
					return
				}
				var removed int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					removed, err = zremrangebyscore(txn, currentDb(conn), cmd.Args[1], string(cmd.Args[2]), string(cmd.Args[3]))
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(removed)
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrevrange(txn, currentDb(conn), cmd.Args[1], start, stop, withScores)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrevrangebylex(txn, currentDb(conn), cmd.Args[1], maxStr, minStr, limitOffset, limitCount, hasLimit)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					result, err := zrevrangebyscore(txn, currentDb(conn), cmd.Args[1], maxStr, minStr, withScores, limitOffset, limitCount, hasLimit)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					writeBulkArray(conn, result)
					return nil
				})
			case "zrevrank":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					rank, found, err := zrevrank(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if !found {
						conn.WriteNull()
					} else {
						conn.WriteInt(rank)
					}
					return nil
				})
			case "zscore":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					score, found, err := zscore(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if !found {
						conn.WriteNull()
					} else {
						conn.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
					}
					return nil
				})
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
				db.View(func(txn *badger.Txn) error {
					m, err := zdiff(txn, currentDb(conn), keys...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					members := zsetToSlice(m)
					if hasWithScores {
						conn.WriteArray(len(members) * 2)
						for _, e := range members {
							conn.WriteBulk(e.member)
							conn.WriteBulkString(strconv.FormatFloat(e.score, 'f', -1, 64))
						}
					} else {
						conn.WriteArray(len(members))
						for _, e := range members {
							conn.WriteBulk(e.member)
						}
					}
					return nil
				})
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
				db.Update(func(txn *badger.Txn) error {
					m, err := zdiff(txn, currentDb(conn), keys...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					members := zsetToSlice(m)
					count, err := storeZSetResult(txn, currentDb(conn), cmd.Args[1], members)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteInt(count)
					return nil
				})
			case "zmscore":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					scores, found, err := zmscore(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(len(scores))
					for i, s := range scores {
						if found[i] {
							conn.WriteBulkString(strconv.FormatFloat(s, 'f', -1, 64))
						} else {
							conn.WriteNull()
						}
					}
					return nil
				})
			case "zrandmember":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				count := 1
				withScores := false
				if len(cmd.Args) >= 3 {
					var ok bool
					count, ok = parseIntArg(conn, cmd.Args[2])
					if !ok {
						return
					}
				}
				if count < 0 {
					withScores = true
					count = -count
				}
				db.View(func(txn *badger.Txn) error {
					members, scores, err := zrandmember(txn, currentDb(conn), cmd.Args[1], count)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if withScores {
						conn.WriteArray(len(members) * 2)
						for i := range members {
							conn.WriteBulk(members[i])
							conn.WriteBulkString(strconv.FormatFloat(scores[i], 'f', -1, 64))
						}
					} else {
						conn.WriteArray(len(members))
						for _, m := range members {
							conn.WriteBulk(m)
						}
					}
					return nil
				})
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
					db.Update(func(txn *badger.Txn) error {
						m, err := zunion(txn, currentDb(conn), aggregate, keys...)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						if len(weights) > 0 {
							for member, score := range m {
								m[member] = score * weights[0]
							}
						}
						members := zsetToSlice(m)
						count, err := storeZSetResult(txn, currentDb(conn), cmd.Args[1], members)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						conn.WriteInt(count)
						return nil
					})
				} else {
					db.View(func(txn *badger.Txn) error {
						m, err := zunion(txn, currentDb(conn), aggregate, keys...)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return nil
						}
						if len(weights) > 0 {
							for member, score := range m {
								m[member] = score * weights[0]
							}
						}
						hasWithScores := false
						for _, arg := range cmd.Args {
							if strings.EqualFold(string(arg), "withscores") {
								hasWithScores = true
								break
							}
						}
						members := zsetToSlice(m)
						if hasWithScores {
							conn.WriteArray(len(members) * 2)
							for _, e := range members {
								conn.WriteBulk(e.member)
								conn.WriteBulkString(strconv.FormatFloat(e.score, 'f', -1, 64))
							}
						} else {
							conn.WriteArray(len(members))
							for _, e := range members {
								conn.WriteBulk(e.member)
							}
						}
						return nil
					})
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
				var storeCount int
				err := db.Update(func(txn *badger.Txn) error {
					result, err := zrange(txn, currentDb(conn), cmd.Args[2], start, stop, false)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					// Re-fetch with scores for storage
					resultWS, err := zrange(txn, currentDb(conn), cmd.Args[2], start, stop, true)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					members := make([]memberScore, len(result))
					for i := range result {
						score, _ := strconv.ParseFloat(string(resultWS[i*2+1]), 64)
						members[i] = memberScore{member: result[i], score: score}
					}
					count, err := storeZSetResult(txn, currentDb(conn), cmd.Args[1], members)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					storeCount = count
					return nil
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(storeCount)
			case "pfadd":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var added int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					added, err = pfadd(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(added)
			case "pfcount":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				var count uint64
				err := db.View(func(txn *badger.Txn) error {
					var err error
					count, err = pfcount(txn, currentDb(conn), cmd.Args[1:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt64(int64(count))
			case "pfmerge":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					return pfmerge(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:]...)
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteString("OK")
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
				err := db.Update(func(txn *badger.Txn) error {
					return bfreserve(txn, currentDb(conn), cmd.Args[1], errRate, uint64(capacity), expansion, nonScaling)
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteString("OK")
			case "bf.add":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				var added int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					added, err = bfadd(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(added)
			case "bf.exists":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				var exists bool
				err := db.View(func(txn *badger.Txn) error {
					var err error
					exists, err = bfexists(txn, currentDb(conn), cmd.Args[1], cmd.Args[2])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				if exists {
					conn.WriteInt(1)
				} else {
					conn.WriteInt(0)
				}
			case "bf.madd":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var results []int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					results, err = bfmadd(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteArray(len(results))
				for _, r := range results {
					conn.WriteInt(r)
				}
			case "bf.mexists":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				var results []int
				err := db.View(func(txn *badger.Txn) error {
					var err error
					results, err = bfmexists(txn, currentDb(conn), cmd.Args[1], cmd.Args[2:])
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteArray(len(results))
				for _, r := range results {
					conn.WriteInt(r)
				}
			case "bf.insert":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				info := &bfInsertInfo{}
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
				var results []int
				err := db.Update(func(txn *badger.Txn) error {
					var err error
					results, err = bfinsert(txn, currentDb(conn), cmd.Args[1], info)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteArray(len(results))
				for _, r := range results {
					conn.WriteInt(r)
				}
			case "bf.info":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				db.View(func(txn *badger.Txn) error {
					info, err := bfinfo(txn, currentDb(conn), cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					conn.WriteArray(len(info) * 2)
					for k, v := range info {
						conn.WriteBulkString(k)
						switch val := v.(type) {
						case int:
							conn.WriteInt64(int64(val))
						case uint64:
							conn.WriteInt64(int64(val))
						default:
							conn.WriteString(fmt.Sprintf("%v", val))
						}
					}
					return nil
				})
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
	if err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return err
		}
	}
	return nil
}
