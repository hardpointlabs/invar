package hash

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"strconv"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// internalKey builds the internal hash field key: session.PrivateKey(hash + \x00 + field)
func internalKey(session *common.Session, hash, field []byte) []byte {
	compound := make([]byte, 0, len(hash)+1+len(field))
	compound = append(compound, hash...)
	compound = append(compound, 0)
	compound = append(compound, field...)
	return session.PrivateKey(compound)
}

// internalKeyRaw builds the raw internal key compound (hash + \x00 + field) without session prefix.
func internalKeyRaw(hash, field []byte) []byte {
	compound := make([]byte, 0, len(hash)+1+len(field))
	compound = append(compound, hash...)
	compound = append(compound, 0)
	compound = append(compound, field...)
	return compound
}

// fieldPrefix builds the prefix for iterating all fields of a hash:
// session.PrivateKey(hash + \x00)
func fieldPrefix(session *common.Session, hash []byte) []byte {
	compound := make([]byte, len(hash)+1)
	copy(compound, hash)
	compound[len(hash)] = 0
	return session.PrivateKey(compound)
}

func HSet(session *common.Session, hash []byte, fieldValues ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, hash)
		if err != nil {
			count = 0
		}

		var added int
		for i := 0; i < len(fieldValues); i += 2 {
			field := fieldValues[i]
			value := fieldValues[i+1]
			key := internalKey(session, hash, field)
			_, getErr := tx.Get(key)
			if getErr == kv.ErrKeyNotFound {
				added++
				count++
			} else if getErr != nil {
				return added, getErr
			}
			entry := session.NewPrivateEntry(internalKeyRaw(hash, field), value).Metadata(byte(common.RedisHash))
			if err := tx.Set(entry); err != nil {
				return added, err
			}
		}

		return added, common.WriteUint32Sentinel(tx, session, hash, count, common.RedisHash)
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

func HSetNX(session *common.Session, hash, field, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		key := internalKey(session, hash, field)
		_, err := tx.Get(key)
		if err == nil {
			return 0, nil
		}
		if err != kv.ErrKeyNotFound {
			return 0, err
		}

		entry := session.NewPrivateEntry(internalKeyRaw(hash, field), value).Metadata(byte(common.RedisHash))
		if err := tx.Set(entry); err != nil {
			return 0, err
		}

		count, cerr := common.ReadUint32Sentinel(tx, session, hash)
		if cerr != nil {
			count = 0
		}
		return 1, common.WriteUint32Sentinel(tx, session, hash, count+1, common.RedisHash)
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

func HGet(session *common.Session, hash, field []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(internalKey(session, hash, field))
		if err != nil {
			return nil, err
		}
		return item.Value()
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.([]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HMGet(session *common.Session, hash []byte, fields ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		results := make([][]byte, len(fields))
		for i, field := range fields {
			item, err := tx.Get(internalKey(session, hash, field))
			if err == kv.ErrKeyNotFound {
				results[i] = nil
				continue
			}
			if err != nil {
				return nil, err
			}
			val, err := item.Value()
			if err != nil {
				return nil, err
			}
			results[i] = val
		}
		return results, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		results := result.([][]byte)
		conn.WriteArray(len(results))
		for _, r := range results {
			if r == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(r)
			}
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HDel(session *common.Session, hash []byte, fields ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, hash)
		if err != nil {
			return 0, nil
		}

		var removed int
		for _, field := range fields {
			key := internalKey(session, hash, field)
			_, gerr := tx.Get(key)
			if gerr == kv.ErrKeyNotFound {
				continue
			}
			if gerr != nil {
				return removed, gerr
			}
			if err := tx.Delete(key); err != nil {
				return removed, err
			}
			removed++
			count--
		}

		if count == 0 {
			return removed, tx.Delete(session.PublicKey(hash))
		}
		return removed, common.WriteUint32Sentinel(tx, session, hash, count, common.RedisHash)
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

func HExists(session *common.Session, hash, field []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(internalKey(session, hash, field))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
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

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HLen(session *common.Session, hash []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, hash)
		if err != nil {
			return 0, nil
		}
		return int(count), nil
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

func HKeys(session *common.Session, hash []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(hash))
		if err == kv.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := fieldPrefix(session, hash)
		kvIt := tx.NewIterator(prefix)
		it := *kvIt
		defer it.Close()

		var keys [][]byte
		for it.Next() {
			keys = append(keys, common.MemberFromInternalKey(it.Item().Key()))
		}
		return keys, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		keys := result.([][]byte)
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HVals(session *common.Session, hash []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(hash))
		if err == kv.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := fieldPrefix(session, hash)
		kvIt := tx.NewIterator(prefix)
		it := *kvIt
		defer it.Close()

		var vals [][]byte
		for it.Next() {
			val, err := it.Item().Value()
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		return vals, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		vals := result.([][]byte)
		conn.WriteArray(len(vals))
		for _, v := range vals {
			conn.WriteBulk(v)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HGetAll(session *common.Session, hash []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(hash))
		if err == kv.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := fieldPrefix(session, hash)
		kvIt := tx.NewIterator(prefix)
		it := *kvIt
		defer it.Close()

		var pairs [][]byte
		for it.Next() {
			item := it.Item()
			pairs = append(pairs, common.MemberFromInternalKey(item.Key()))
			val, err := item.Value()
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, val)
		}
		return pairs, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		pairs := result.([][]byte)
		conn.WriteArray(len(pairs))
		for _, p := range pairs {
			conn.WriteBulk(p)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HMSet(session *common.Session, hash []byte, fieldValues ...[]byte) common.QueuedOp {
	hsetOp := HSet(session, hash, fieldValues...)
	return common.QueuedOp{
		DbOp: hsetOp.DbOp,
		WireOp: func(conn redcon.Conn, result any, err error) {
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return
			}
			conn.WriteString("OK")
		},
		IsMutating: true,
	}
}

func HIncrBy(session *common.Session, hash, field []byte, amount int64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		key := internalKey(session, hash, field)
		item, err := tx.Get(key)
		var current int64
		if err == kv.ErrKeyNotFound {
			current = 0
		} else if err != nil {
			return 0, err
		} else {
			val, vErr := item.Value()
			if vErr != nil {
				return 0, vErr
			}
			current, err = strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("hash value is not an integer")
			}
		}

		newVal := current + amount
		entry := session.NewPrivateEntry(internalKeyRaw(hash, field), []byte(strconv.FormatInt(newVal, 10))).Metadata(byte(common.RedisHash))
		if err := tx.Set(entry); err != nil {
			return 0, err
		}

		if err == kv.ErrKeyNotFound {
			count, cerr := common.ReadUint32Sentinel(tx, session, hash)
			if cerr != nil {
				count = 0
			}
			if wErr := common.WriteUint32Sentinel(tx, session, hash, count+1, common.RedisHash); wErr != nil {
				return 0, wErr
			}
		}
		return newVal, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt64(result.(int64))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func HIncrByFloat(session *common.Session, hash, field []byte, amount float64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if math.IsNaN(amount) {
			return "", fmt.Errorf("value is not a valid float")
		}

		key := internalKey(session, hash, field)
		item, err := tx.Get(key)
		var current float64
		var fieldExisted bool
		if err == kv.ErrKeyNotFound {
			current = 0
			fieldExisted = false
		} else if err != nil {
			return "", err
		} else {
			fieldExisted = true
			val, vErr := item.Value()
			if vErr != nil {
				return "", vErr
			}
			current, err = strconv.ParseFloat(string(val), 64)
			if err != nil {
				return "", fmt.Errorf("hash value is not a float")
			}
		}

		result := current + amount
		str := common.FormatFloat(result)
		entry := session.NewPrivateEntry(internalKeyRaw(hash, field), []byte(str)).Metadata(byte(common.RedisHash))
		if err := tx.Set(entry); err != nil {
			return "", err
		}

		if !fieldExisted {
			count, cerr := common.ReadUint32Sentinel(tx, session, hash)
			if cerr != nil {
				count = 0
			}
			if wErr := common.WriteUint32Sentinel(tx, session, hash, count+1, common.RedisHash); wErr != nil {
				return "", wErr
			}
		}
		return str, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteBulkString(result.(string))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func HRandField(session *common.Session, hash []byte, count int, withValues bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(hash))
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := fieldPrefix(session, hash)
		kvIt := tx.NewIterator(prefix)
		it := *kvIt
		defer it.Close()

		fieldList := make([]string, 0)
		valList := make([][]byte, 0)
		for it.Next() {
			item := it.Item()
			field := string(common.MemberFromInternalKey(item.Key()))
			val, err := item.Value()
			if err != nil {
				return nil, err
			}
			fieldList = append(fieldList, field)
			valList = append(valList, val)
		}

		if count == 0 || len(fieldList) == 0 {
			return [][]byte{}, nil
		}

		if count > 0 && count >= len(fieldList) {
			if withValues {
				result := make([][]byte, 0, len(fieldList)*2)
				for i := 0; i < len(fieldList); i++ {
					result = append(result, []byte(fieldList[i]), valList[i])
				}
				return result, nil
			}
			result := make([][]byte, len(fieldList))
			for i := 0; i < len(fieldList); i++ {
				result[i] = []byte(fieldList[i])
			}
			return result, nil
		}

		if count > 0 {
			perm := rand.Perm(len(fieldList))
			if withValues {
				result := make([][]byte, 0, count*2)
				for i := 0; i < count; i++ {
					idx := perm[i]
					result = append(result, []byte(fieldList[idx]), valList[idx])
				}
				return result, nil
			}
			result := make([][]byte, count)
			for i := 0; i < count; i++ {
				result[i] = []byte(fieldList[perm[i]])
			}
			return result, nil
		}

		count = -count
		if withValues {
			result := make([][]byte, 0, count*2)
			for i := 0; i < count; i++ {
				idx := rand.IntN(len(fieldList))
				result = append(result, []byte(fieldList[idx]), valList[idx])
			}
			return result, nil
		}
		result := make([][]byte, count)
		for i := 0; i < count; i++ {
			result[i] = []byte(fieldList[rand.IntN(len(fieldList))])
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
			return
		}
		list := result.([][]byte)
		conn.WriteArray(len(list))
		for _, item := range list {
			conn.WriteBulk(item)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func HStrLen(session *common.Session, hash, field []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(internalKey(session, hash, field))
		if err != nil {
			return 0, nil
		}
		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		return len(val), nil
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

func HScan(session *common.Session, hash []byte, pattern string, count int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(hash))
		if err == kv.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := fieldPrefix(session, hash)
		kvIt := tx.NewIterator(prefix)
		it := *kvIt
		defer it.Close()

		matchPattern := len(pattern) > 0
		var pairs [][]byte
		for it.Next() {
			item := it.Item()
			field := string(common.MemberFromInternalKey(item.Key()))

			if matchPattern {
				matched, _ := path.Match(pattern, field)
				if !matched {
					continue
				}
			}

			val, vErr := item.Value()
			if vErr != nil {
				return nil, vErr
			}
			pairs = append(pairs, []byte(field), val)

			if count > 0 && len(pairs)/2 >= count {
				break
			}
		}

		return pairs, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		pairs := result.([][]byte)
		conn.WriteArray(2)
		conn.WriteBulkString("0")
		conn.WriteArray(len(pairs))
		for _, p := range pairs {
			conn.WriteBulk(p)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}
