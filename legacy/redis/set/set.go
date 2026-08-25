package set

import (
	"bytes"
	"math/rand/v2"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// Internal key format:  -{db}:{setname}\x00{member}
// Sentinel key format:  {db}:{setname}           (value = 4-byte uint32 count)

// setMemberCompound builds the raw internal compound "{setname}\x00{member}" without session prefix.
func setMemberCompound(setName, member []byte) []byte {
	compound := make([]byte, 0, len(setName)+1+len(member))
	compound = append(compound, setName...)
	compound = append(compound, 0)
	compound = append(compound, member...)
	return compound
}

// internalSetKey builds the internal member key: session.PrivateKey(setName + \x00 + member)
func internalSetKey(session *common.Session, setName, member []byte) []byte {
	return session.PrivateKey(setMemberCompound(setName, member))
}

// membersPrefix builds the prefix for iterating all members of a set:
// session.PrivateKey(setName + \x00)
func membersPrefix(session *common.Session, setName []byte) []byte {
	compound := make([]byte, len(setName)+1)
	copy(compound, setName)
	compound[len(setName)] = 0
	return session.PrivateKey(compound)
}

func loadSetMembers(tx kv.Tx, session *common.Session, setName []byte) (map[string]struct{}, error) {
	prefix := membersPrefix(session, setName)
	it := tx.NewIterator(prefix)
	defer it.Close()

	members := make(map[string]struct{})
	for it.Next() {
		val, err := it.Item().Value()
		if err != nil {
			return nil, err
		}
		members[string(val)] = struct{}{}
	}
	return members, nil
}

func SAdd(session *common.Session, key []byte, members ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		var added int
		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err != nil {
			count = 0
		}

		for _, member := range members {
			_, getErr := tx.Get(internalSetKey(session, key, member))
			if getErr == kv.ErrKeyNotFound {
				entry := session.NewPrivateEntry(setMemberCompound(key, member), member).Metadata(byte(common.RedisSet))
				if err := tx.Set(entry); err != nil {
					return added, err
				}
				added++
				count++
			} else if getErr != nil {
				return added, getErr
			}
		}

		return added, common.WriteUint32Sentinel(tx, session, key, count, common.RedisSet)
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

func SRem(session *common.Session, key []byte, members ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}

		var removed int
		for _, member := range members {
			internalKey := internalSetKey(session, key, member)
			_, gerr := tx.Get(internalKey)
			if gerr == kv.ErrKeyNotFound {
				continue
			}
			if gerr != nil {
				return removed, gerr
			}
			if err := tx.Delete(internalKey); err != nil {
				return removed, err
			}
			removed++
			count--
		}

		if count == 0 {
			return removed, tx.Delete(session.PublicKey(key))
		}
		return removed, common.WriteUint32Sentinel(tx, session, key, count, common.RedisSet)
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

func SCard(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
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

func SMembers(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(key))
		if err == kv.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := membersPrefix(session, key)
		it := tx.NewIterator(prefix)
		defer it.Close()

		var members [][]byte
		for it.Next() {
			val, err := it.Item().Value()
			if err != nil {
				return nil, err
			}
			members = append(members, val)
		}
		return members, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([][]byte)
		conn.WriteArray(len(members))
		for _, m := range members {
			conn.WriteBulk(m)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func SIsMember(session *common.Session, key, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(internalSetKey(session, key, member))
		if err == kv.ErrKeyNotFound {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result.(bool) {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func SPop(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		prefix := membersPrefix(session, key)
		it := tx.NewIterator(prefix)
		defer it.Close()

		idx := rand.IntN(int(count))
		var member []byte
		var found int
		for it.Next() {
			if found == idx {
				k := append([]byte{}, it.Item().Key()...)
				member = common.MemberFromInternalKey(k)
				if err := tx.Delete(k); err != nil {
					return nil, err
				}
			}
			found++
		}

		count--
		if count == 0 {
			return member, tx.Delete(session.PublicKey(key))
		}
		return member, common.WriteUint32Sentinel(tx, session, key, count, common.RedisSet)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(result.([]byte))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func SRandMember(session *common.Session, key []byte, count int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		all, err := loadSetMembers(tx, session, key)
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([][]byte)
		if count == 1 {
			if len(members) == 0 {
				conn.WriteNull()
			} else {
				conn.WriteBulk(members[0])
			}
			return
		}
		conn.WriteArray(len(members))
		for _, m := range members {
			conn.WriteBulk(m)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func SMove(session *common.Session, src, dst, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if bytes.Equal(src, dst) {
			_, err := tx.Get(internalSetKey(session, src, member))
			if err == kv.ErrKeyNotFound {
				return false, nil
			}
			return err == nil, err
		}

		srcKey := internalSetKey(session, src, member)
		_, err := tx.Get(srcKey)
		if err == kv.ErrKeyNotFound {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		if err := tx.Delete(srcKey); err != nil {
			return false, err
		}

		srcCount, err := common.ReadUint32Sentinel(tx, session, src)
		if err != nil {
			return false, err
		}
		srcCount--
		if srcCount == 0 {
			if err := tx.Delete(session.PublicKey(src)); err != nil {
				return false, err
			}
		} else if err := common.WriteUint32Sentinel(tx, session, src, srcCount, common.RedisSet); err != nil {
			return false, err
		}

		dstKey := internalSetKey(session, dst, member)
		_, err = tx.Get(dstKey)
		if err == kv.ErrKeyNotFound {
			if err := tx.Set(session.NewPrivateEntry(setMemberCompound(dst, member), member).Metadata(byte(common.RedisSet))); err != nil {
				return false, err
			}
			dstCount, err := common.ReadUint32Sentinel(tx, session, dst)
			if err == kv.ErrKeyNotFound {
				return true, common.WriteUint32Sentinel(tx, session, dst, 1, common.RedisSet)
			}
			if err != nil {
				return false, err
			}
			dstCount++
			return true, common.WriteUint32Sentinel(tx, session, dst, dstCount, common.RedisSet)
		}
		if err != nil {
			return false, err
		}

		return true, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result.(bool) {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func SDiff(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if len(keys) == 0 {
			return [][]byte{}, nil
		}

		result, err := loadSetMembers(tx, session, keys[0])
		if err != nil {
			return nil, err
		}

		for _, key := range keys[1:] {
			other, err := loadSetMembers(tx, session, key)
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([][]byte)
		conn.WriteArray(len(members))
		for _, m := range members {
			conn.WriteBulk(m)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func SInter(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if len(keys) == 0 {
			return [][]byte{}, nil
		}

		result, err := loadSetMembers(tx, session, keys[0])
		if err != nil {
			return nil, err
		}

		for _, key := range keys[1:] {
			other, err := loadSetMembers(tx, session, key)
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([][]byte)
		conn.WriteArray(len(members))
		for _, m := range members {
			conn.WriteBulk(m)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func SUnion(session *common.Session, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if len(keys) == 0 {
			return [][]byte{}, nil
		}

		result := make(map[string]struct{})
		for _, key := range keys {
			other, err := loadSetMembers(tx, session, key)
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([][]byte)
		conn.WriteArray(len(members))
		for _, m := range members {
			conn.WriteBulk(m)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func storeSetResult(tx kv.Tx, session *common.Session, dest []byte, members [][]byte) (int, error) {
	if err := clearSet(tx, session, dest); err != nil {
		return 0, err
	}

	for _, m := range members {
		if err := tx.Set(session.NewPrivateEntry(setMemberCompound(dest, m), m).Metadata(byte(common.RedisSet))); err != nil {
			return 0, err
		}
	}

	return len(members), common.WriteUint32Sentinel(tx, session, dest, uint32(len(members)), common.RedisSet)
}

func clearSet(tx kv.Tx, session *common.Session, setName []byte) error {
	return common.ClearPrefixedKeys(tx, membersPrefix(session, setName), session.PublicKey(setName))
}

func SDiffStore(session *common.Session, dest []byte, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		result, err := SDiff(session, keys...).DbOp(tx)
		if err != nil {
			return 0, err
		}
		return storeSetResult(tx, session, dest, result.([][]byte))
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

func SInterStore(session *common.Session, dest []byte, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		result, err := SInter(session, keys...).DbOp(tx)
		if err != nil {
			return 0, err
		}
		return storeSetResult(tx, session, dest, result.([][]byte))
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

func SUnionStore(session *common.Session, dest []byte, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		result, err := SUnion(session, keys...).DbOp(tx)
		if err != nil {
			return 0, err
		}
		return storeSetResult(tx, session, dest, result.([][]byte))
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
