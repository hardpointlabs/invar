package keys

import (
	"time"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// Exists returns the number of given keys that exist.
func Exists(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count := 0
		for _, key := range keys {
			_, err := tx.Get(session.PublicKey(key))
			if err == nil {
				count++
			} else if err != kv.ErrKeyNotFound {
				return 0, err
			}
		}
		return count, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// MGet returns the values of the given keys, or null for missing keys.
func MGet(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		values := make([]any, len(keys))
		for i, key := range keys {
			item, err := tx.Get(session.PublicKey(key))
			if err != nil {
				if err == kv.ErrKeyNotFound {
					values[i] = nil
					continue
				}
				return nil, err
			}
			val, err := item.Value()
			if err != nil {
				return nil, err
			}
			values[i] = val
		}
		return values, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		values := result.([]any)
		conn.WriteArray(len(values))
		for _, v := range values {
			if v == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(v.([]byte))
			}
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Move moves a key to another DB, returning 1 if moved, 0 if the source key
// doesn't exist.
func Move(session *common.Session, key []byte, targetDb int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return 0, nil
			}
			return 0, err
		}
		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		if err := tx.Set(session.NewEntryForDB(targetDb, key, val).Metadata(item.Metadata())); err != nil {
			return 0, err
		}
		if err := tx.Delete(session.PublicKey(key)); err != nil {
			return 0, err
		}
		return 1, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Rename renames oldKey to newKey, overwriting any existing newKey.
func Rename(session *common.Session, oldKey, newKey []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(oldKey))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(session.NewPublicEntry(newKey, val).Metadata(item.Metadata())); err != nil {
			return nil, err
		}
		if err := tx.Delete(session.PublicKey(oldKey)); err != nil {
			return nil, err
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteError("ERR no such key")
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// RenameNX renames oldKey to newKey only if newKey doesn't exist, returning
// 1 on success and 0 if newKey exists.
func RenameNX(session *common.Session, oldKey, newKey []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(newKey))
		if err == nil {
			return 0, nil
		} else if err != kv.ErrKeyNotFound {
			return 0, err
		}
		item, err := tx.Get(session.PublicKey(oldKey))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return nil, nil
			}
			return 0, err
		}
		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		if err := tx.Set(session.NewPublicEntry(newKey, val).Metadata(item.Metadata())); err != nil {
			return 0, err
		}
		if err := tx.Delete(session.PublicKey(oldKey)); err != nil {
			return 0, err
		}
		return 1, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteError("no such key")
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Expire sets a TTL in seconds on a key, returning 1 if the key exists.
func Expire(session *common.Session, key []byte, seconds int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return 0, nil
			}
			return 0, err
		}
		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		entry := session.NewPublicEntry(key, val).Metadata(item.Metadata()).TTL(time.Duration(seconds) * time.Second)
		if err := tx.Set(entry); err != nil {
			return 0, err
		}
		return 1, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// TTL returns the remaining time to live in seconds: -2 if the key is
// missing, -1 if it exists without an expiry.
func TTL(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return -2, nil
			}
			return 0, err
		}
		expiresAt := item.ExpiresAt()
		now := uint64(time.Now().Unix())
		if expiresAt == 0 || expiresAt <= now {
			return -1, nil
		}
		return int(expiresAt - now), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// PTTL returns the remaining time to live in milliseconds: -2 if the key is
// missing, -1 if it exists without an expiry.
func PTTL(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return int64(-2), nil
			}
			return int64(0), err
		}
		expiresAt := item.ExpiresAt()
		now := uint64(time.Now().Unix())
		if expiresAt == 0 || expiresAt <= now {
			return int64(-1), nil
		}
		return int64(expiresAt-now) * 1000, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt64(result.(int64))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Type returns the type of the value stored at key.
func Type(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return "none", nil
			}
			return nil, err
		}
		return typeName(common.RedisValueType(item.Metadata())), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString(result.(string))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Del removes the given keys, returning the number removed.
func Del(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count := 0
		for _, key := range keys {
			_, err := tx.Get(session.PublicKey(key))
			if err == kv.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return 0, err
			}
			if err := tx.Delete(session.PublicKey(key)); err != nil {
				return 0, err
			}
			count++
		}
		return count, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func typeName(t common.RedisValueType) string {
	switch t {
	case common.RedisString:
		return "string"
	case common.RedisList:
		return "list"
	case common.RedisSet:
		return "set"
	case common.RedisSortedSet:
		return "zset"
	case common.RedisHash:
		return "hash"
	case common.RedisStream:
		return "stream"
	case common.RedisVectorSet:
		return "vectorset"
	case common.RedisBloom:
		return "bloom"
	case common.RedisJSON:
		return "json"
	default:
		return "unknown"
	}
}
