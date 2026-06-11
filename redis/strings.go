package redis

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

func setKey(conn redcon.Conn, db *badger.DB, key, value []byte) {
	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func setKeyWithTTL(conn redcon.Conn, db *badger.DB, key, value []byte, ttlSec int) {
	expTime := time.Duration(ttlSec) * time.Second
	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithTTL(expTime).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func getKey(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteNull()
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteBulk(valCopy)
		return nil
	})
}

func getSet(conn redcon.Conn, db *badger.DB, key, value []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithMeta(byte(RedisString))
			err = txn.SetEntry(e)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			conn.WriteNull()
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithMeta(byte(RedisString))
		err = txn.SetEntry(e)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteBulk(valCopy)
		return nil
	})
}

func getDel(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteNull()
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		err = txn.Delete(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteNull()
			return nil
		}
		conn.WriteBulk(valCopy)
		return nil
	})
}

func strlenKey(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteInt(0)
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteInt(len(valCopy))
		return nil
	})
}

func substrKey(conn redcon.Conn, db *badger.DB, key []byte, start, end int) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteBulk([]byte{})
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return nil
		}
		if start < 0 {
			start = len(valCopy) + start
		}
		if end < 0 {
			end = len(valCopy) + end
		}
		if start < 0 {
			start = 0
		}
		if end >= len(valCopy) {
			end = len(valCopy) - 1
		}
		if start > end || start >= len(valCopy) {
			conn.WriteBulk([]byte{})
			return nil
		}
		conn.WriteBulk(valCopy[start : end+1])
		return nil
	})
}

func setNX(conn redcon.Conn, db *badger.DB, key, value []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		var set = false
		_, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithMeta(byte(RedisString))
				err = txn.SetEntry(e)
				if err != nil {
					return err
				}
				set = true
			} else {
				return err
			}
		}

		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return nil
		}
		if set {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return nil
	})
}

func appendKey(conn redcon.Conn, db *badger.DB, key, value []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithMeta(byte(RedisString))
				err = txn.SetEntry(e)
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return err
				}
				conn.WriteInt(len(value))
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return err
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var oldVal []byte
		oldVal, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		newVal := append(oldVal, value...)
		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), newVal).WithMeta(byte(RedisString))
		err = txn.SetEntry(e)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteInt(len(newVal))
		return nil
	})
}

func getEx(conn redcon.Conn, db *badger.DB, args ...[]byte) {
	key := args[0]
	var exSec, pxMs, exatSec, pxatMs *int64
	var persist bool
	for i := 1; i < len(args); i++ {
		opt := strings.ToLower(string(args[i]))
		switch opt {
		case "persist":
			persist = true
		case "ex":
			i++
			if i >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			exSec = &v
		case "px":
			i++
			if i >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			pxMs = &v
		case "exat":
			i++
			if i >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			exatSec = &v
		case "pxat":
			i++
			if i >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			pxatMs = &v
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			conn.WriteNull()
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		var entry *badger.Entry
		if persist {
			entry = badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithMeta(byte(RedisString))
		} else if exSec != nil {
			entry = badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithTTL(time.Duration(*exSec)*time.Second).WithMeta(byte(RedisString))
		} else if pxMs != nil {
			entry = badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithTTL(time.Duration(*pxMs)*time.Millisecond).WithMeta(byte(RedisString))
		} else if exatSec != nil {
			ttl := time.Until(time.Unix(*exatSec, 0))
			if ttl < 0 {
				ttl = 0
			}
			entry = badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithTTL(ttl).WithMeta(byte(RedisString))
		} else if pxatMs != nil {
			ttl := time.Until(time.UnixMilli(*pxatMs))
			if ttl < 0 {
				ttl = 0
			}
			entry = badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithTTL(ttl).WithMeta(byte(RedisString))
		}

		if entry != nil {
			err = txn.SetEntry(entry)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
		}

		conn.WriteBulk(valCopy)
		return nil
	})
}

func incrByFloat(conn redcon.Conn, db *badger.DB, key []byte, amount float64) {
	if math.IsNaN(amount) {
		conn.WriteError("ERR value is not a valid float")
		return
	}
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				result := amount
				e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), []byte(formatFloat(result))).WithMeta(byte(RedisString))
				err = txn.SetEntry(e)
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return err
				}
				conn.WriteBulkString(formatFloat(result))
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return err
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		val, err := strconv.ParseFloat(string(valCopy), 64)
		if err != nil {
			conn.WriteError("ERR value is not a float")
			return err
		}
		if math.IsInf(val, 0) {
			conn.WriteError("ERR value is not a float")
			return nil
		}
		result := val + amount
		if math.IsInf(result, -1) {
			conn.WriteBulkString("-inf")
		} else if math.IsInf(result, 1) {
			conn.WriteBulkString("inf")
		} else {
			e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), []byte(formatFloat(result))).WithMeta(byte(RedisString))
			err = txn.SetEntry(e)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			conn.WriteBulkString(formatFloat(result))
		}
		return nil
	})
}

func msetKeys(conn redcon.Conn, db *badger.DB, args ...[]byte) {
	err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			e := badger.NewEntry(rawKeyPrefix(args[i], currentDb(conn)), args[i+1]).WithMeta(byte(RedisString))
			err := txn.SetEntry(e)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func msetnxKeys(conn redcon.Conn, db *badger.DB, args ...[]byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			_, err := txn.Get(rawKeyPrefix(args[i], currentDb(conn)))
			if err == nil {
				conn.WriteInt(0)
				return nil
			}
		}
		for i := 0; i < len(args); i += 2 {
			e := badger.NewEntry(rawKeyPrefix(args[i], currentDb(conn)), args[i+1]).WithMeta(byte(RedisString))
			err := txn.SetEntry(e)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
		}
		conn.WriteInt(1)
		return nil
	})
}

func setKeyWithTTLMs(conn redcon.Conn, db *badger.DB, key, value []byte, ttlMs int) {
	expTime := time.Duration(ttlMs) * time.Millisecond
	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), value).WithTTL(expTime).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func setRangeKey(conn redcon.Conn, db *badger.DB, key []byte, offset int, value []byte) {
	if offset < 0 {
		conn.WriteError("ERR offset is out of range")
		return
	}
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				newVal := make([]byte, offset+len(value))
				copy(newVal[offset:], value)
				e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), newVal).WithMeta(byte(RedisString))
				err = txn.SetEntry(e)
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return err
				}
				conn.WriteInt(len(newVal))
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return err
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		var oldVal []byte
		oldVal, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		newLen := offset + len(value)
		if newLen < len(oldVal) {
			newLen = len(oldVal)
		}
		newVal := make([]byte, newLen)
		copy(newVal, oldVal)
		copy(newVal[offset:], value)

		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), newVal).WithMeta(byte(RedisString))
		err = txn.SetEntry(e)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteInt(len(newVal))
		return nil
	})
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
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

		valCopy, err := copyItemValue(item)
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
