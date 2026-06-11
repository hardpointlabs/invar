package redis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

func internalHashKey(hash, field []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, hash...)
	prefix = append(prefix, 0)
	return append(prefix, field...)
}

func hashFieldsPrefix(hash []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, hash...)
	return append(prefix, 0)
}

func fieldFromInternalKey(key []byte) []byte {
	idx := bytes.LastIndexByte(key, 0)
	if idx < 0 {
		return nil
	}
	return key[idx+1:]
}

func writeHashCount(txn *badger.Txn, hash []byte, count uint32, dbSlot int) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	return txn.SetEntry(badger.NewEntry(rawKeyPrefixWithDb(hash, dbSlot), buf).WithMeta(byte(RedisHash)))
}

func readHashCount(txn *badger.Txn, hash []byte, dbSlot int) (uint32, error) {
	item, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
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

func clearHash(txn *badger.Txn, hash []byte, dbSlot int) error {
	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return txn.Delete(rawKeyPrefixWithDb(hash, dbSlot))
}

func loadAllFields(txn *badger.Txn, hash []byte, dbSlot int) (map[string][]byte, error) {
	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	fields := make(map[string][]byte)
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		field := string(fieldFromInternalKey(item.KeyCopy(nil)))
		_ = item.Value(func(val []byte) error {
			fields[field] = append([]byte{}, val...)
			return nil
		})
	}
	return fields, nil
}

func hset(txn *badger.Txn, conn redcon.Conn, hash []byte, args ...[]byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	var added int
	var count uint32

	item, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
	if err == badger.ErrKeyNotFound {
		count = 0
	} else if err != nil {
		return 0, err
	} else {
		if err := item.Value(func(val []byte) error {
			count = binary.BigEndian.Uint32(val)
			return nil
		}); err != nil {
			return 0, err
		}
	}

	for i := 0; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		internalKey := internalHashKey(hash, field, dbSlot)
		_, err := txn.Get(internalKey)
		if err == badger.ErrKeyNotFound {
			added++
			count++
		} else if err != nil {
			return added, err
		}
		if err := txn.SetEntry(badger.NewEntry(internalKey, value).WithMeta(byte(RedisHash))); err != nil {
			return added, err
		}
	}

	return added, writeHashCount(txn, hash, count, dbSlot)
}

func hget(txn *badger.Txn, conn redcon.Conn, hash, field []byte) ([]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	item, err := txn.Get(internalHashKey(hash, field, dbSlot))
	if err != nil {
		return nil, err
	}
	var valCopy []byte
	if err := item.Value(func(val []byte) error {
		valCopy = append([]byte{}, val...)
		return nil
	}); err != nil {
		return nil, err
	}
	return valCopy, nil
}

func hdel(txn *badger.Txn, conn redcon.Conn, hash []byte, fields ...[]byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	count, err := readHashCount(txn, hash, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var removed int
	for _, field := range fields {
		internalKey := internalHashKey(hash, field, dbSlot)
		_, err := txn.Get(internalKey)
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return removed, err
		}
		if err := txn.Delete(internalKey); err != nil {
			return removed, err
		}
		removed++
		count--
	}

	if count == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(hash, dbSlot))
	}
	return removed, writeHashCount(txn, hash, count, dbSlot)
}

func hexists(txn *badger.Txn, conn redcon.Conn, hash, field []byte) (bool, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_, err := txn.Get(internalHashKey(hash, field, dbSlot))
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func hlen(txn *badger.Txn, conn redcon.Conn, hash []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	count, err := readHashCount(txn, hash, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func hmget(txn *badger.Txn, conn redcon.Conn, hash []byte, fields ...[]byte) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	results := make([][]byte, len(fields))
	for i, field := range fields {
		item, err := txn.Get(internalHashKey(hash, field, dbSlot))
		if err == badger.ErrKeyNotFound {
			results[i] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		_ = item.Value(func(val []byte) error {
			results[i] = append([]byte{}, val...)
			return nil
		})
	}
	return results, nil
}

func hkeys(txn *badger.Txn, conn redcon.Conn, hash []byte) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Item().KeyCopy(nil)
		keys = append(keys, fieldFromInternalKey(k))
	}
	return keys, nil
}

func hvals(txn *badger.Txn, conn redcon.Conn, hash []byte) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	var vals [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		_ = it.Item().Value(func(val []byte) error {
			vals = append(vals, append([]byte{}, val...))
			return nil
		})
	}
	return vals, nil
}

func hgetall(txn *badger.Txn, conn redcon.Conn, hash []byte) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	var pairs [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		pairs = append(pairs, fieldFromInternalKey(k))
		_ = item.Value(func(val []byte) error {
			pairs = append(pairs, append([]byte{}, val...))
			return nil
		})
	}
	return pairs, nil
}

func hincrby(txn *badger.Txn, conn redcon.Conn, hash, field []byte, amount int64) (int64, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	internalKey := internalHashKey(hash, field, dbSlot)
	item, err := txn.Get(internalKey)
	if err == badger.ErrKeyNotFound {
		newVal := amount
		if err := txn.SetEntry(badger.NewEntry(internalKey, []byte(strconv.FormatInt(newVal, 10))).WithMeta(byte(RedisHash))); err != nil {
			return 0, err
		}
		if err := hashFieldAdded(txn, hash, dbSlot); err != nil {
			return 0, err
		}
		return newVal, nil
	}
	if err != nil {
		return 0, err
	}

	var valCopy []byte
	if err := item.Value(func(val []byte) error {
		valCopy = append([]byte{}, val...)
		return nil
	}); err != nil {
		return 0, err
	}

	current, err := strconv.ParseInt(string(valCopy), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("hash value is not an integer")
	}

	newVal := current + amount
	if err := txn.SetEntry(badger.NewEntry(internalKey, []byte(strconv.FormatInt(newVal, 10))).WithMeta(byte(RedisHash))); err != nil {
		return 0, err
	}
	return newVal, nil
}

func hashFieldAdded(txn *badger.Txn, hash []byte, dbSlot int) error {
	count, err := readHashCount(txn, hash, dbSlot)
	if err == badger.ErrKeyNotFound {
		return writeHashCount(txn, hash, 1, dbSlot)
	}
	if err != nil {
		return err
	}
	return writeHashCount(txn, hash, count+1, dbSlot)
}

func hincrbyfloat(txn *badger.Txn, conn redcon.Conn, hash, field []byte, amount float64) (string, error) {
	if math.IsNaN(amount) {
		return "", fmt.Errorf("value is not a valid float")
	}
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	internalKey := internalHashKey(hash, field, dbSlot)
	item, err := txn.Get(internalKey)
	if err == badger.ErrKeyNotFound {
		result := amount
		str := formatFloat(result)
		if err := txn.SetEntry(badger.NewEntry(internalKey, []byte(str)).WithMeta(byte(RedisHash))); err != nil {
			return "", err
		}
		if err := hashFieldAdded(txn, hash, dbSlot); err != nil {
			return "", err
		}
		return str, nil
	}
	if err != nil {
		return "", err
	}

	var valCopy []byte
	if err := item.Value(func(val []byte) error {
		valCopy = append([]byte{}, val...)
		return nil
	}); err != nil {
		return "", err
	}

	val, err := strconv.ParseFloat(string(valCopy), 64)
	if err != nil {
		return "", fmt.Errorf("hash value is not a float")
	}

	result := val + amount
	str := formatFloat(result)
	if err := txn.SetEntry(badger.NewEntry(internalKey, []byte(str)).WithMeta(byte(RedisHash))); err != nil {
		return "", err
	}
	return str, nil
}

func hrandfield(txn *badger.Txn, conn redcon.Conn, hash []byte, count int, withValues bool) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	fields, err := loadAllFields(txn, hash, dbSlot)
	if err == badger.ErrKeyNotFound || len(fields) == 0 {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	fieldList := make([]string, 0, len(fields))
	valList := make([][]byte, 0, len(fields))
	for f, v := range fields {
		fieldList = append(fieldList, f)
		valList = append(valList, v)
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

func hstrlen(txn *badger.Txn, conn redcon.Conn, hash, field []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	item, err := txn.Get(internalHashKey(hash, field, dbSlot))
	if err != nil {
		return 0, nil
	}
	var length int
	if err := item.Value(func(val []byte) error {
		length = len(val)
		return nil
	}); err != nil {
		return 0, err
	}
	return length, nil
}

func hscan(txn *badger.Txn, conn redcon.Conn, hash []byte, pattern string, count int) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_, err := txn.Get(rawKeyPrefixWithDb(hash, dbSlot))
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := hashFieldsPrefix(hash, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	matchPattern := len(pattern) > 0
	var pairs [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		field := string(fieldFromInternalKey(k))

		if matchPattern {
			matched, _ := path.Match(pattern, field)
			if !matched {
				continue
			}
		}

		pairs = append(pairs, []byte(field))
		_ = item.Value(func(val []byte) error {
			pairs = append(pairs, append([]byte{}, val...))
			return nil
		})

		if count > 0 && len(pairs)/2 >= count {
			break
		}
	}

	return pairs, nil
}
