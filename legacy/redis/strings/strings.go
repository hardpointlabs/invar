package strings

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

var errWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

func writeErr(conn redcon.Conn, err error) {
	if errors.Is(err, errWrongType) {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteError("ERR " + err.Error())
}

// Set stores a string value, overwriting any existing value at the key.
func Set(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if err := tx.Set(session.NewPublicEntry(key, value).Metadata(byte(common.RedisString))); err != nil {
			return nil, err
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// SetEx stores a string value with a TTL in seconds.
func SetEx(session *common.Session, key, value []byte, seconds int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entry := session.NewPublicEntry(key, value).Metadata(byte(common.RedisString)).TTL(time.Duration(seconds) * time.Second)
		if err := tx.Set(entry); err != nil {
			return nil, err
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// PSetEx stores a string value with a TTL in milliseconds.
func PSetEx(session *common.Session, key, value []byte, ms int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entry := session.NewPublicEntry(key, value).Metadata(byte(common.RedisString)).TTL(time.Duration(ms) * time.Millisecond)
		if err := tx.Set(entry); err != nil {
			return nil, err
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Get returns the string value stored at key, or nil if the key is missing.
func Get(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return nil, errWrongType
		}
		return item.Value()
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		if result == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// GetSet sets the value at key and returns the previous value, or nil if the
// key was missing.
func GetSet(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				if err := tx.Set(session.NewPublicEntry(key, value).Metadata(byte(common.RedisString))); err != nil {
					return nil, err
				}
				return nil, nil
			}
			return nil, err
		}
		oldVal, err := item.Value()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(session.NewPublicEntry(key, value).Metadata(byte(common.RedisString))); err != nil {
			return nil, err
		}
		return oldVal, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		if result == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// GetDel returns the string value at key and deletes it, or nil if the key is
// missing.
func GetDel(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return nil, errWrongType
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		if err := tx.Delete(session.PublicKey(key)); err != nil {
			return nil, nil
		}
		return val, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		if result == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Strlen returns the length of the string value at key, or 0 if missing.
func Strlen(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return 0, nil
			}
			return 0, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return 0, errWrongType
		}
		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		return len(val), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Substr returns a substring of the value at key in the inclusive range
// [start, end], supporting negative indices. An empty bulk is returned for a
// missing key or out-of-range slice.
func Substr(session *common.Session, key []byte, start, end int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return []byte{}, nil
			}
			return nil, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return nil, errWrongType
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		if start < 0 {
			start = len(val) + start
		}
		if end < 0 {
			end = len(val) + end
		}
		if start < 0 {
			start = 0
		}
		if end >= len(val) {
			end = len(val) - 1
		}
		if start > end || start >= len(val) {
			return []byte{}, nil
		}
		return val[start : end+1], nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// SetNX stores a string value only if the key doesn't exist, returning 1 if
// the value was set and 0 otherwise.
func SetNX(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				if err := tx.Set(session.NewPublicEntry(key, value).Metadata(byte(common.RedisString))); err != nil {
					return 0, err
				}
				return 1, nil
			}
			return 0, err
		}
		return 0, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Append appends value to the string at key, creating it if missing, and
// returns the new length.
func Append(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				if err := tx.Set(session.NewPublicEntry(key, value).Metadata(byte(common.RedisString))); err != nil {
					return 0, err
				}
				return len(value), nil
			}
			return 0, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return 0, errWrongType
		}
		oldVal, err := item.Value()
		if err != nil {
			return 0, err
		}
		newVal := append(oldVal, value...)
		if err := tx.Set(session.NewPublicEntry(key, newVal).Metadata(byte(common.RedisString))); err != nil {
			return 0, err
		}
		return len(newVal), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// GetEx returns the value at key, optionally setting a TTL (ex/px/exat/pxat)
// or clearing one (persist).
func GetEx(session *common.Session, args ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		key := args[0]
		var exSec, pxMs, exatSec, pxatMs *int64
		var persist bool
		for i := 1; i < len(args); i++ {
			opt := strings.ToLower(string(args[i]))
			switch opt {
			case "persist":
				persist = true
			case "ex", "px", "exat", "pxat":
				i++
				if i >= len(args) {
					return nil, errors.New("syntax error")
				}
				v, err := strconv.ParseInt(string(args[i]), 10, 64)
				if err != nil {
					return nil, errors.New("value is not an integer or out of range")
				}
				switch opt {
				case "ex":
					exSec = &v
				case "px":
					pxMs = &v
				case "exat":
					exatSec = &v
				case "pxat":
					pxatMs = &v
				}
			default:
				return nil, errors.New("syntax error")
			}
		}

		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return nil, errWrongType
		}
		valCopy, err := item.Value()
		if err != nil {
			return nil, err
		}

		var entry kv.Entry
		if persist {
			entry = session.NewPublicEntry(key, valCopy).Metadata(byte(common.RedisString))
		} else if exSec != nil {
			entry = session.NewPublicEntry(key, valCopy).Metadata(byte(common.RedisString)).TTL(time.Duration(*exSec) * time.Second)
		} else if pxMs != nil {
			entry = session.NewPublicEntry(key, valCopy).Metadata(byte(common.RedisString)).TTL(time.Duration(*pxMs) * time.Millisecond)
		} else if exatSec != nil {
			ttl := time.Until(time.Unix(*exatSec, 0))
			if ttl < 0 {
				ttl = 0
			}
			entry = session.NewPublicEntry(key, valCopy).Metadata(byte(common.RedisString)).TTL(ttl)
		} else if pxatMs != nil {
			ttl := time.Until(time.UnixMilli(*pxatMs))
			if ttl < 0 {
				ttl = 0
			}
			entry = session.NewPublicEntry(key, valCopy).Metadata(byte(common.RedisString)).TTL(ttl)
		}

		if entry != nil {
			if err := tx.Set(entry); err != nil {
				return nil, err
			}
		}
		return valCopy, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		if result == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// IncrByFloat increments the value at key by amount (a float), creating it if
// missing, and returns the new value.
func IncrByFloat(session *common.Session, key []byte, amount float64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if math.IsNaN(amount) {
			return nil, errors.New("value is not a valid float")
		}
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				result := amount
				if err := tx.Set(session.NewPublicEntry(key, []byte(common.FormatFloat(result))).Metadata(byte(common.RedisString))); err != nil {
					return nil, err
				}
				return common.FormatFloat(result), nil
			}
			return nil, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return nil, errWrongType
		}
		valCopy, err := item.Value()
		if err != nil {
			return nil, err
		}
		val, err := strconv.ParseFloat(string(valCopy), 64)
		if err != nil {
			return nil, errors.New("value is not a float")
		}
		if math.IsInf(val, 0) {
			return nil, errors.New("value is not a float")
		}
		result := val + amount
		if math.IsInf(result, -1) {
			return "-inf", nil
		}
		if math.IsInf(result, 1) {
			return "inf", nil
		}
		if err := tx.Set(session.NewPublicEntry(key, []byte(common.FormatFloat(result))).Metadata(byte(common.RedisString))); err != nil {
			return nil, err
		}
		return common.FormatFloat(result), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteBulkString(result.(string))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// MSet sets multiple string keys atomically, where args is an alternating
// key/value list.
func MSet(session *common.Session, args ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		for i := 0; i < len(args); i += 2 {
			if err := tx.Set(session.NewPublicEntry(args[i], args[i+1]).Metadata(byte(common.RedisString))); err != nil {
				return nil, err
			}
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// MSetNX sets multiple string keys only if none of them exist, returning 1 if
// all were set and 0 otherwise.
func MSetNX(session *common.Session, args ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		for i := 0; i < len(args); i += 2 {
			_, err := tx.Get(session.PublicKey(args[i]))
			if err == nil {
				return 0, nil
			} else if err != kv.ErrKeyNotFound {
				return 0, err
			}
		}
		for i := 0; i < len(args); i += 2 {
			if err := tx.Set(session.NewPublicEntry(args[i], args[i+1]).Metadata(byte(common.RedisString))); err != nil {
				return 0, err
			}
		}
		return 1, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// SetRange overwrites part of the string at key starting at offset, returning
// the new length.
func SetRange(session *common.Session, key []byte, offset int, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err == kv.ErrKeyNotFound {
				newVal := make([]byte, offset+len(value))
				copy(newVal[offset:], value)
				if err := tx.Set(session.NewPublicEntry(key, newVal).Metadata(byte(common.RedisString))); err != nil {
					return 0, err
				}
				return len(newVal), nil
			}
			return 0, err
		}
		if item.Metadata() != byte(common.RedisString) {
			return 0, errWrongType
		}
		oldVal, err := item.Value()
		if err != nil {
			return 0, err
		}

		newLen := offset + len(value)
		if newLen < len(oldVal) {
			newLen = len(oldVal)
		}
		newVal := make([]byte, newLen)
		copy(newVal, oldVal)
		copy(newVal[offset:], value)

		if err := tx.Set(session.NewPublicEntry(key, newVal).Metadata(byte(common.RedisString))); err != nil {
			return 0, err
		}
		return len(newVal), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Increment adds amount to the integer value at key, creating it if missing,
// and returns the new value. Supports INCR, INCRBY, DECR and DECRBY.
func Increment(session *common.Session, key []byte, amount int64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if err != kv.ErrKeyNotFound {
				return 0, err
			}
			currentValue := amount
			if err := tx.Set(session.NewPublicEntry(key, []byte(strconv.FormatInt(currentValue, 10))).Metadata(byte(common.RedisString))); err != nil {
				return 0, err
			}
			return currentValue, nil
		}

		valCopy, err := item.Value()
		if err != nil {
			return 0, err
		}
		currentValue, err := strconv.ParseInt(string(valCopy), 10, 64)
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
		currentValue += amount
		if err := tx.Set(session.NewPublicEntry(key, []byte(strconv.FormatInt(currentValue, 10))).Metadata(item.Metadata())); err != nil {
			return 0, err
		}
		return currentValue, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeErr(conn, err)
			return
		}
		conn.WriteInt64(result.(int64))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}
