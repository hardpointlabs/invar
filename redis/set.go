package redis

import (
	"bytes"
	"math/rand/v2"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

// Internal key format:  -{db}:{setname}\x00{member}
// Sentinel key format:  {db}:{setname}           (value = 4-byte uint32 count)

func internalSetKey(setName, member []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	prefix = append(prefix, 0)
	return append(prefix, member...)
}

func membersPrefix(setName []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	return append(prefix, 0)
}

func memberFromInternalKey(key []byte) []byte {
	idx := bytes.LastIndexByte(key, 0)
	if idx < 0 {
		return nil
	}
	return key[idx+1:]
}

func writeSetCount(txn *badger.Txn, setName []byte, count uint32, dbSlot int) error {
	return writeUint32Sentinel(txn, setName, count, RedisSet, dbSlot)
}

func readSetCount(txn *badger.Txn, setName []byte, dbSlot int) (uint32, error) {
	return readUint32Sentinel(txn, setName, dbSlot)
}

func loadSetMembers(txn *badger.Txn, setName []byte, dbSlot int) (map[string]struct{}, error) {
	prefix := membersPrefix(setName, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	members := make(map[string]struct{})
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		_ = item.Value(func(val []byte) error {
			members[string(val)] = struct{}{}
			return nil
		})
	}
	return members, nil
}

func clearSet(txn *badger.Txn, setName []byte, dbSlot int) error {
	return clearPrefixedKeys(txn, membersPrefix(setName, dbSlot), rawKeyPrefix(setName, dbSlot))
}

func sadd(txn *badger.Txn, dbSlot int, key []byte, members ...[]byte) (int, error) {

	var added int
	count, err := readSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		count = 0
	} else if err != nil {
		return 0, err
	}

	for _, member := range members {
		_, err := txn.Get(internalSetKey(key, member, dbSlot))
		if err == badger.ErrKeyNotFound {
			if err := txn.SetEntry(badger.NewEntry(internalSetKey(key, member, dbSlot), member).WithMeta(byte(RedisSet))); err != nil {
				return added, err
			}
			added++
			count++
		} else if err != nil {
			return added, err
		}
	}

	return added, writeSetCount(txn, key, count, dbSlot)
}

func srem(txn *badger.Txn, dbSlot int, key []byte, members ...[]byte) (int, error) {

	count, err := readSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var removed int
	for _, member := range members {
		internalKey := internalSetKey(key, member, dbSlot)
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
		return removed, txn.Delete(rawKeyPrefix(key, dbSlot))
	}
	return removed, writeSetCount(txn, key, count, dbSlot)
}

func scard(txn *badger.Txn, dbSlot int, key []byte) (int, error) {

	count, err := readSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func smembers(txn *badger.Txn, dbSlot int, key []byte) ([][]byte, error) {

	_, err := txn.Get(rawKeyPrefix(key, dbSlot))
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := membersPrefix(key, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	var members [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		_ = item.Value(func(val []byte) error {
			members = append(members, append([]byte{}, val...))
			return nil
		})
	}
	return members, nil
}

func sismember(txn *badger.Txn, dbSlot int, key, member []byte) (bool, error) {

	_, err := txn.Get(internalSetKey(key, member, dbSlot))
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func spop(txn *badger.Txn, dbSlot int, key []byte) ([]byte, error) {

	count, err := readSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := membersPrefix(key, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	idx := rand.IntN(int(count))
	var member []byte
	var found int
	for it.Rewind(); it.Valid() && found <= idx; it.Next() {
		if found == idx {
			k := it.Item().KeyCopy(nil)
			member = memberFromInternalKey(k)
			if err := txn.Delete(k); err != nil {
				return nil, err
			}
		}
		found++
	}

	count--
	if count == 0 {
		return member, txn.Delete(rawKeyPrefix(key, dbSlot))
	}
	return member, writeSetCount(txn, key, count, dbSlot)
}

func srandmember(txn *badger.Txn, dbSlot int, key []byte, count int) ([][]byte, error) {

	all, err := loadSetMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	members := make([][]byte, 0, len(all))
	for m := range all {
		members = append(members, []byte(m))
	}

	if count == 0 || len(members) == 0 {
		return [][]byte{}, nil
	}

	if count > 0 && count >= len(members) {
		return members, nil
	}

	if count > 0 {
		perm := rand.Perm(len(members))
		result := make([][]byte, count)
		for i := 0; i < count; i++ {
			result[i] = members[perm[i]]
		}
		return result, nil
	}

	count = -count
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		result[i] = members[rand.IntN(len(members))]
	}
	return result, nil
}

func smove(txn *badger.Txn, dbSlot int, src, dst, member []byte) (bool, error) {
	if bytes.Equal(src, dst) {
		_, err := txn.Get(internalSetKey(src, member, dbSlot))
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return err == nil, err
	}


	srcKey := internalSetKey(src, member, dbSlot)
	_, err := txn.Get(srcKey)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := txn.Delete(srcKey); err != nil {
		return false, err
	}

	srcCount, err := readSetCount(txn, src, dbSlot)
	if err != nil {
		return false, err
	}
	srcCount--
	if srcCount == 0 {
		if err := txn.Delete(rawKeyPrefix(src, dbSlot)); err != nil {
			return false, err
		}
	} else {
		if err := writeSetCount(txn, src, srcCount, dbSlot); err != nil {
			return false, err
		}
	}

	dstKey := internalSetKey(dst, member, dbSlot)
	_, err = txn.Get(dstKey)
	if err == badger.ErrKeyNotFound {
		if err := txn.SetEntry(badger.NewEntry(dstKey, member).WithMeta(byte(RedisSet))); err != nil {
			return false, err
		}
		dstCount, err := readSetCount(txn, dst, dbSlot)
		if err == badger.ErrKeyNotFound {
			return true, writeSetCount(txn, dst, 1, dbSlot)
		}
		if err != nil {
			return false, err
		}
		dstCount++
		return true, writeSetCount(txn, dst, dstCount, dbSlot)
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func sdiff(txn *badger.Txn, dbSlot int, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	result, err := loadSetMembers(txn, keys[0], dbSlot)
	if err != nil {
		return nil, err
	}

	for _, key := range keys[1:] {
		other, err := loadSetMembers(txn, key, dbSlot)
		if err != nil {
			return nil, err
		}
		for m := range other {
			delete(result, m)
		}
	}

	var members [][]byte
	for m := range result {
		members = append(members, []byte(m))
	}
	return members, nil
}

func sinter(txn *badger.Txn, dbSlot int, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	result, err := loadSetMembers(txn, keys[0], dbSlot)
	if err != nil {
		return nil, err
	}

	for _, key := range keys[1:] {
		other, err := loadSetMembers(txn, key, dbSlot)
		if err != nil {
			return nil, err
		}
		for m := range result {
			if _, ok := other[m]; !ok {
				delete(result, m)
			}
		}
	}

	var members [][]byte
	for m := range result {
		members = append(members, []byte(m))
	}
	return members, nil
}

func sunion(txn *badger.Txn, dbSlot int, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	result := make(map[string]struct{})
	for _, key := range keys {
		other, err := loadSetMembers(txn, key, dbSlot)
		if err != nil {
			return nil, err
		}
		for m := range other {
			result[m] = struct{}{}
		}
	}

	var members [][]byte
	for m := range result {
		members = append(members, []byte(m))
	}
	return members, nil
}

func storeSetResult(txn *badger.Txn, dbSlot int, dest []byte, members [][]byte) (int, error) {

	_ = clearSet(txn, dest, dbSlot)

	for _, m := range members {
		if err := txn.SetEntry(badger.NewEntry(internalSetKey(dest, m, dbSlot), m).WithMeta(byte(RedisSet))); err != nil {
			return 0, err
		}
	}

	return len(members), writeSetCount(txn, dest, uint32(len(members)), dbSlot)
}

func sdiffstore(txn *badger.Txn, dbSlot int, dest []byte, keys ...[]byte) (int, error) {
	result, err := sdiff(txn, dbSlot, keys...)
	if err != nil {
		return 0, err
	}
	return storeSetResult(txn, dbSlot, dest, result)
}

func sinterstore(txn *badger.Txn, dbSlot int, dest []byte, keys ...[]byte) (int, error) {
	result, err := sinter(txn, dbSlot, keys...)
	if err != nil {
		return 0, err
	}
	return storeSetResult(txn, dbSlot, dest, result)
}

func sunionstore(txn *badger.Txn, dbSlot int, dest []byte, keys ...[]byte) (int, error) {
	result, err := sunion(txn, dbSlot, keys...)
	if err != nil {
		return 0, err
	}
	return storeSetResult(txn, dbSlot, dest, result)
}
