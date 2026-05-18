package redis

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

var addr = ":6379"

// key delimeters
const internalPrefix = "-"
const prefixSeparator = ":"

// public redis types for LSM tree entries (not private/internal types)
// string, list, set, zset, hash, stream, and vectorset
type redisValueType byte

const (
	RedisString redisValueType = iota
	RedisList
	RedisSet
	RedisSortedSet
	RedisHash
	RedisStream
	RedisVectorSet
)

func currentDbPrefix(conn redcon.Conn) []byte {
	return []byte(strconv.Itoa(currentDb(conn)) + prefixSeparator)
}

// rawKeyPrefix with explicit db index (for testing)
func rawKeyPrefixWithDb(keyName []byte, dbSlot int) []byte {
	return append([]byte(strconv.Itoa(dbSlot)+prefixSeparator), keyName...)
}

// prefixer for publicly accessible keys, including the database slot
func rawKeyPrefix(keyName []byte, dbSlot int) []byte {
	return append([]byte(strconv.Itoa(dbSlot)+prefixSeparator), keyName...)
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
			var valCopy []byte
			err = item.Value(func(val []byte) error {
				valCopy = append([]byte{}, val...)
				return nil
			})
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
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
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

func incrementKey(conn redcon.Conn, db *badger.DB, key []byte, amount int64) {
	_ = db.Update(func(txn *badger.Txn) error {
		var currentValue int64 = 0
		var meta byte = byte(RedisString)
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err != badger.ErrKeyNotFound {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			currentValue = amount
			entry := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), []byte(strconv.FormatInt(currentValue, 10))).WithMeta(meta)
			err = txn.SetEntry(entry)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			conn.WriteInt64(currentValue)
			return nil
		}

		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		currentValue, err = strconv.ParseInt(string(valCopy), 10, 64)
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return err
		}
		currentValue += amount
		entry := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), []byte(strconv.FormatInt(currentValue, 10))).WithMeta(item.UserMeta())
		err = txn.SetEntry(entry)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteInt64(currentValue)
		return nil
	})
}

func Serve(db *badger.DB) {
	var ps redcon.PubSub
	go log.Info().Msgf("started redis listener at %s", addr)
	err := redcon.ListenAndServe(addr,
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
				// no-op for us
				conn.WriteString("OK")
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
			case "type":
				if !checkExactArgs(conn, cmd, 2) {
					return
				}
				typeOfKey(conn, db, cmd.Args[1])
			case "del":
				if !checkMinArgs(conn, cmd, 2) {
					return
				}
				delKeys(conn, db, cmd.Args[1:]...)
			case "lpush":
				if !checkMinArgs(conn, cmd, 3) {
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					_, err := lpush(txn, conn, cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				db.View(func(txn *badger.Txn) error {
					size, err := llen(txn, conn, cmd.Args[1])
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
					_, err := rpush(txn, conn, cmd.Args[1], cmd.Args[2:]...)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				db.View(func(txn *badger.Txn) error {
					size, err := llen(txn, conn, cmd.Args[1])
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
					val, err = lpop(txn, conn, cmd.Args[1])
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
					val, err = rpop(txn, conn, cmd.Args[1])
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
					size, err := llen(txn, conn, cmd.Args[1])
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
					items, err := lrange(txn, conn, cmd.Args[1], start, stop)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteArray(len(items))
					for _, item := range items {
						conn.WriteBulk(item)
					}
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
					val, err := lindex(txn, conn, cmd.Args[1], index)
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
					return lset(txn, conn, cmd.Args[1], index, cmd.Args[3])
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
					removed, err = lrem(txn, conn, cmd.Args[1], count, cmd.Args[3])
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
					return ltrim(txn, conn, cmd.Args[1], start, stop)
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
					result, err = linsert(txn, conn, cmd.Args[1], before, cmd.Args[3], cmd.Args[4])
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
				var size int
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					_, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err == badger.ErrKeyNotFound {
						size = 0
						return nil
					}
					if err != nil {
						return err
					}
					var newSize uint32
					newSize, err = lpush(txn, conn, cmd.Args[1], cmd.Args[2])
					size = int(newSize)
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				conn.WriteInt(size)
			case "rpushx":
				if !checkExactArgs(conn, cmd, 3) {
					return
				}
				var size int
				var dbErr error
				dbErr = db.Update(func(txn *badger.Txn) error {
					_, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err == badger.ErrKeyNotFound {
						size = 0
						return nil
					}
					if err != nil {
						return err
					}
					var newSize uint32
					newSize, err = rpush(txn, conn, cmd.Args[1], cmd.Args[2])
					size = int(newSize)
					return err
				})
				if dbErr != nil {
					conn.WriteError("ERR " + dbErr.Error())
					return
				}
				conn.WriteInt(size)
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
		log.Fatal().Err(err).Msg("")
	}
}
