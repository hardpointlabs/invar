package zset

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// Sorted set key structure:
//   Sentinel:   {db}:{keyname}                      → 4-byte uint32 count, meta=RedisSortedSet
//   Score idx:  -{db}:{keyname}:score:{8B encScore}:{member}  → empty value
//   Member idx: -{db}:{keyname}:member:{member}               → 8-byte big-endian score

// scoreCompound builds "-{db}:{setname}:score:{8B encScore}:{member}" without the session prefix.
func scoreCompound(setName []byte, score float64, member []byte) []byte {
	b := make([]byte, 0, len(setName)+9+8+1+len(member))
	b = append(b, setName...)
	b = append(b, ":score:"...)
	b = append(b, encodeScore(score)...)
	b = append(b, ':')
	return append(b, member...)
}

// memberCompound builds "-{db}:{setname}:member:{member}" without the session prefix.
func memberCompound(setName []byte, member []byte) []byte {
	b := make([]byte, 0, len(setName)+8+len(member))
	b = append(b, setName...)
	b = append(b, ":member:"...)
	return append(b, member...)
}

func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	if bits>>63 == 1 {
		bits ^= 0xFFFFFFFFFFFFFFFF
	} else {
		bits ^= 0x8000000000000000
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits>>63 == 1 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	return math.Float64frombits(bits)
}

func scorePrefix(session *common.Session, setName []byte) []byte {
	b := make([]byte, 0, len(setName)+7)
	b = append(b, setName...)
	b = append(b, ":score:"...)
	return session.PrivateKey(b)
}

func memberPrefix(session *common.Session, setName []byte) []byte {
	b := make([]byte, 0, len(setName)+8)
	b = append(b, setName...)
	b = append(b, ":member:"...)
	return session.PrivateKey(b)
}

// MemberScore holds a member and its score, used for sorted iteration and pop results.
type MemberScore struct {
	Member []byte
	Score  float64
}

// loadAllMembers returns all members in score order (ascending, with lex tie-break).
func loadAllMembers(tx kv.Tx, session *common.Session, setName []byte) ([]MemberScore, error) {
	prefix := scorePrefix(session, setName)
	it := tx.NewIterator(prefix)
	defer it.Close()

	var result []MemberScore
	for it.Next() {
		k := append([]byte{}, it.Item().Key()...)
		encScore := k[len(prefix) : len(prefix)+8]
		member := k[len(prefix)+8+1:] // skip : after encoded score
		result = append(result, MemberScore{
			Member: member,
			Score:  decodeScore(encScore),
		})
	}
	return result, nil
}

// clearInternalKeys deletes all score and member index keys plus the sentinel.
func clearInternalKeys(tx kv.Tx, session *common.Session, setName []byte) error {
	scorePfx := scorePrefix(session, setName)
	it := tx.NewIterator(scorePfx)
	for it.Next() {
		if err := tx.Delete(append([]byte{}, it.Item().Key()...)); err != nil {
			it.Close()
			return err
		}
	}
	it.Close()

	memberPfx := memberPrefix(session, setName)
	it2 := tx.NewIterator(memberPfx)
	for it2.Next() {
		if err := tx.Delete(append([]byte{}, it2.Item().Key()...)); err != nil {
			it2.Close()
			return err
		}
	}
	it2.Close()

	return tx.Delete(session.PublicKey(setName))
}

// --- Command implementations ---

func ZAdd(session *common.Session, key []byte, args ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if len(args) == 0 {
			return zaddResult{}, fmt.Errorf("ERR wrong number of arguments for 'zadd' command")
		}

		// Parse options
		var nx bool
		var xx bool
		var ch bool
		var gt bool
		var lt bool

		idx := 0
		for idx < len(args) {
			arg := strings.ToLower(string(args[idx]))
			if arg == "nx" {
				nx = true
				idx++
			} else if arg == "xx" {
				xx = true
				idx++
			} else if arg == "ch" {
				ch = true
				idx++
			} else if arg == "gt" {
				gt = true
				idx++
			} else if arg == "lt" {
				lt = true
				idx++
			} else {
				break
			}
		}

		if nx && xx {
			return zaddResult{}, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
		}
		if nx && (gt || lt) {
			return zaddResult{}, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
		}
		if xx && (gt || lt) {
			return zaddResult{}, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
		}
		if gt && lt {
			return zaddResult{}, fmt.Errorf("ERR GT and LT options are not compatible")
		}

		remaining := args[idx:]
		if len(remaining) == 0 || len(remaining)%2 != 0 {
			return zaddResult{}, fmt.Errorf("ERR wrong number of arguments for 'zadd' command")
		}

		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			if xx {
				return zaddResult{}, nil
			}
			count = 0
		} else if err != nil {
			return zaddResult{}, err
		}

		changed := 0
		added := 0

		for i := 0; i < len(remaining); i += 2 {
			member := remaining[i+1]
			scoreStr := string(remaining[i])
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil || math.IsNaN(score) {
				return zaddResult{}, fmt.Errorf("ERR value is not a valid float")
			}

			memberKey := session.PrivateKey(memberCompound(key, member))
			existingItem, existingErr := tx.Get(memberKey)

			if existingErr == kv.ErrKeyNotFound {
				if nx || xx {
					if nx {
						// Add new
						if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, score, member), nil)); err != nil {
							return zaddResult{count: changed + added}, err
						}
						if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(score))); err != nil {
							return zaddResult{count: changed + added}, err
						}
						count++
						added++
					}
					// xx would skip non-existing
				} else {
					if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, score, member), nil)); err != nil {
						return zaddResult{count: changed + added}, err
					}
					if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(score))); err != nil {
						return zaddResult{count: changed + added}, err
					}
					count++
					added++
				}
			} else if existingErr != nil {
				return zaddResult{}, existingErr
			} else {
				// Member exists
				if nx {
					continue
				}

				val, err := existingItem.Value()
				if err != nil {
					return zaddResult{}, err
				}
				oldScore := math.Float64frombits(binary.BigEndian.Uint64(val))

				// GT: update only if new score > old score
				if gt && !(score > oldScore) {
					continue
				}
				// LT: update only if new score < old score
				if lt && !(score < oldScore) {
					continue
				}

				if oldScore != score {
					// Remove old score entry
					if err := tx.Delete(session.PrivateKey(scoreCompound(key, oldScore, member))); err != nil {
						return zaddResult{count: changed + added}, err
					}
					// Add new score entry
					if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, score, member), nil)); err != nil {
						return zaddResult{count: changed + added}, err
					}
					// Update member entry
					if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(score))); err != nil {
						return zaddResult{count: changed + added}, err
					}
					changed++
				}
			}
		}

		if added > 0 || changed > 0 {
			if err := common.WriteUint32Sentinel(tx, session, key, count, common.RedisSortedSet); err != nil {
				return zaddResult{count: changed + added}, err
			}
		}

		// After persisting the write, check if any waiter is blocked on this key.
		// If so, claim the front waiter and pop an element on its behalf — atomically
		// within this same transaction.  The claim is returned so WireOp can wake the
		// waiter after Commit() succeeds.
		var claim *common.Claim
		if added > 0 {
			publicKey := string(session.PublicKey(key))
			claim = common.GlobalWatchRegistry.TryClaim(publicKey)
			if claim != nil {
				var popped *MemberScore
				var popErr error
				if claim.WantMin {
					popped, popErr = popOneMin(tx, session, key)
				} else {
					popped, popErr = popOneMax(tx, session, key)
				}
				if popErr != nil {
					// Pop failed; release the claim so the waiter stays in queue.
					common.GlobalWatchRegistry.ReleaseFront(claim)
					claim = nil
					return zaddResult{count: changed + added}, popErr
				}
				if popped != nil {
					claim.SetResult(common.PopResult{
						Key:    string(key),
						Member: popped.Member,
						Score:  popped.Score,
					})
				}
			}
		}

		n := changed + added
		if ch {
			return zaddResult{count: n, claim: claim}, nil
		}
		return zaddResult{count: added, claim: claim}, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		r := result.(zaddResult)
		conn.WriteInt(r.count)
		// Wake any claimed waiter — this runs only after Commit() succeeded.
		if r.claim != nil {
			r.claim.Wake()
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// zaddResult carries the integer count returned to the client plus any waiter
// claim made inside the DbOp.  It implements common.Claimer so DispatchPendingOps
// can release the claim on transaction failure.
type zaddResult struct {
	count int
	claim *common.Claim
}

// Claims implements common.Claimer.
func (r zaddResult) Claims() []*common.Claim {
	if r.claim == nil {
		return nil
	}
	return []*common.Claim{r.claim}
}

func scoreBytes(score float64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(score))
	return buf
}

func ZCard(session *common.Session, key []byte) common.QueuedOp {
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

func ZScore(session *common.Session, key, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PrivateKey(memberCompound(key, member)))
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(val))
		return score, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(strconv.FormatFloat(result.(float64), 'f', -1, 64))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZRem(session *common.Session, key []byte, members ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		count, err := common.ReadUint32Sentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}

		removed := 0
		for _, member := range members {
			memberKey := session.PrivateKey(memberCompound(key, member))
			item, err := tx.Get(memberKey)
			if err == kv.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return removed, err
			}

			val, err := item.Value()
			if err != nil {
				return removed, err
			}
			score := math.Float64frombits(binary.BigEndian.Uint64(val))

			if err := tx.Delete(memberKey); err != nil {
				return removed, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, score, member))); err != nil {
				return removed, err
			}
			count--
			removed++
		}

		if count == 0 {
			return removed, tx.Delete(session.PublicKey(key))
		}
		return removed, common.WriteUint32Sentinel(tx, session, key, count, common.RedisSortedSet)
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

// rangeIndexes normalizes start/stop (supporting negative indices) to [lo, hi].
func rangeIndexes(n, start, stop int) (int, int, bool) {
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return 0, 0, false
	}
	return start, stop, true
}

func ZRange(session *common.Session, key []byte, start, stop int, withScores bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		n := len(entries)
		lo, hi, ok := rangeIndexes(n, start, stop)
		if !ok {
			return [][]byte{}, nil
		}

		size := hi - lo + 1
		if withScores {
			result := make([][]byte, 0, size*2)
			for i := lo; i <= hi; i++ {
				result = append(result, entries[i].Member)
				result = append(result, []byte(strconv.FormatFloat(entries[i].Score, 'f', -1, 64)))
			}
			return result, nil
		}

		result := make([][]byte, size)
		for i := lo; i <= hi; i++ {
			result[i-lo] = entries[i].Member
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func writeBulkArray(conn redcon.Conn, items [][]byte) {
	conn.WriteArray(len(items))
	for _, item := range items {
		conn.WriteBulk(item)
	}
}

func ZRevRange(session *common.Session, key []byte, start, stop int, withScores bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		n := len(entries)
		lo, hi, ok := rangeIndexes(n, start, stop)
		if !ok {
			return [][]byte{}, nil
		}

		// Reverse: iterate from (n-1-start) down to (n-1-stop) in the entries
		rHi := n - 1 - lo
		rLo := n - 1 - hi
		if rLo < 0 {
			rLo = 0
		}

		size := rHi - rLo + 1
		if withScores {
			result := make([][]byte, 0, size*2)
			for i := rHi; i >= rLo; i-- {
				result = append(result, entries[i].Member)
				result = append(result, []byte(strconv.FormatFloat(entries[i].Score, 'f', -1, 64)))
			}
			return result, nil
		}

		result := make([][]byte, size)
		idx := 0
		for i := rHi; i >= rLo; i-- {
			result[idx] = entries[i].Member
			idx++
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZRank(session *common.Session, key, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		for i, e := range entries {
			if bytes.Equal(e.Member, member) {
				return i, nil
			}
		}
		return nil, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteInt(result.(int))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZRevRank(session *common.Session, key, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		n := len(entries)
		for i, e := range entries {
			if bytes.Equal(e.Member, member) {
				return n - 1 - i, nil
			}
		}
		return nil, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteInt(result.(int))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZCount(session *common.Session, key []byte, minStr, maxStr string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return 0, err
		}

		minVal, minExcl, err := parseFloatBound(minStr)
		if err != nil {
			return 0, err
		}
		maxVal, maxExcl, err := parseFloatBound(maxStr)
		if err != nil {
			return 0, err
		}

		count := 0
		for _, e := range entries {
			if minExcl && e.Score <= minVal {
				continue
			}
			if !minExcl && e.Score < minVal {
				continue
			}
			if maxExcl && e.Score >= maxVal {
				continue
			}
			if !maxExcl && e.Score > maxVal {
				continue
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

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZIncrBy(session *common.Session, key []byte, increment float64, member []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PrivateKey(memberCompound(key, member)))
		if err == kv.ErrKeyNotFound {
			// Add new member with increment as score
			if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, increment, member), nil)); err != nil {
				return 0, err
			}
			if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(increment))); err != nil {
				return 0, err
			}
			// Update count
			count, err := common.ReadUint32Sentinel(tx, session, key)
			if err == kv.ErrKeyNotFound {
				count = 1
			} else if err != nil {
				return 0, err
			} else {
				count++
			}
			if err := common.WriteUint32Sentinel(tx, session, key, count, common.RedisSortedSet); err != nil {
				return 0, err
			}
			return increment, nil
		}
		if err != nil {
			return 0, err
		}

		val, err := item.Value()
		if err != nil {
			return 0, err
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(val))

		newScore := score + increment
		if math.IsNaN(newScore) {
			return 0, fmt.Errorf("ERR resulting score is not a valid float")
		}

		// Remove old score entry
		if err := tx.Delete(session.PrivateKey(scoreCompound(key, score, member))); err != nil {
			return 0, err
		}
		// Add new score entry
		if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, newScore, member), nil)); err != nil {
			return 0, err
		}
		// Update member entry
		if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(newScore))); err != nil {
			return 0, err
		}

		return newScore, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteBulkString(strconv.FormatFloat(result.(float64), 'f', -1, 64))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func ZRangeByScore(session *common.Session, key []byte, minStr, maxStr string, withScores bool, limitOffset, limitCount int, hasLimit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		minVal, minExcl, err := parseFloatBound(minStr)
		if err != nil {
			return nil, err
		}
		maxVal, maxExcl, err := parseFloatBound(maxStr)
		if err != nil {
			return nil, err
		}

		var filtered []MemberScore
		for _, e := range entries {
			if minExcl && e.Score <= minVal {
				continue
			}
			if !minExcl && e.Score < minVal {
				continue
			}
			if maxExcl && e.Score >= maxVal {
				continue
			}
			if !maxExcl && e.Score > maxVal {
				continue
			}
			filtered = append(filtered, e)
		}

		if hasLimit {
			if limitOffset < 0 {
				limitOffset = 0
			}
			if limitOffset >= len(filtered) {
				return [][]byte{}, nil
			}
			if limitCount < 0 {
				limitCount = len(filtered) - limitOffset
			}
			filtered = filtered[limitOffset:]
			if limitCount < len(filtered) {
				filtered = filtered[:limitCount]
			}
		}

		if withScores {
			result := make([][]byte, 0, len(filtered)*2)
			for _, e := range filtered {
				result = append(result, e.Member)
				result = append(result, []byte(strconv.FormatFloat(e.Score, 'f', -1, 64)))
			}
			return result, nil
		}

		result := make([][]byte, len(filtered))
		for i, e := range filtered {
			result[i] = e.Member
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZRevRangeByScore(session *common.Session, key []byte, maxStr, minStr string, withScores bool, limitOffset, limitCount int, hasLimit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		// Note: ZREVRANGEBYSCORE takes max first, then min
		result, err := ZRangeByScore(session, key, minStr, maxStr, withScores, limitOffset, limitCount, hasLimit).DbOp(tx)
		if err != nil {
			return nil, err
		}
		flat := result.([][]byte)

		// Reverse the result
		if withScores {
			for i, j := 0, len(flat)-2; i < j; i, j = i+2, j-2 {
				flat[i], flat[j] = flat[j], flat[i]
				flat[i+1], flat[j+1] = flat[j+1], flat[i+1]
			}
		} else {
			for i, j := 0, len(flat)-1; i < j; i, j = i+1, j-1 {
				flat[i], flat[j] = flat[j], flat[i]
			}
		}
		return flat, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// --- Lexicographic commands ---

// filterLexRange filters members by the lexicographic range [min, max) or (min, max)
// min/max use Redis lex notation: [foo (bar + -
func filterLexRange(members [][]byte, minStr, maxStr string) ([][]byte, error) {
	minVal, minExcl, err := parseLexBound(minStr)
	if err != nil {
		return nil, err
	}
	maxVal, maxExcl, err := parseLexBound(maxStr)
	if err != nil {
		return nil, err
	}

	var result [][]byte
	for _, m := range members {
		if minVal != nil {
			cmp := bytes.Compare(m, minVal)
			if minExcl && cmp <= 0 {
				continue
			}
			if !minExcl && cmp < 0 {
				continue
			}
		}
		if maxVal != nil {
			cmp := bytes.Compare(m, maxVal)
			if maxExcl && cmp >= 0 {
				continue
			}
			if !maxExcl && cmp > 0 {
				continue
			}
		}
		result = append(result, m)
	}
	return result, nil
}

func ZRangeByLex(session *common.Session, key []byte, minStr, maxStr string, limitOffset, limitCount int, hasLimit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		members := make([][]byte, len(entries))
		for i, e := range entries {
			members[i] = e.Member
		}

		result, err := filterLexRange(members, minStr, maxStr)
		if err != nil {
			return nil, err
		}

		if hasLimit {
			if limitOffset < 0 {
				limitOffset = 0
			}
			if limitOffset >= len(result) {
				return [][]byte{}, nil
			}
			if limitCount < 0 {
				limitCount = len(result) - limitOffset
			}
			result = result[limitOffset:]
			if limitCount < len(result) {
				result = result[:limitCount]
			}
		}

		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZRevRangeByLex(session *common.Session, key []byte, maxStr, minStr string, limitOffset, limitCount int, hasLimit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		result, err := ZRangeByLex(session, key, minStr, maxStr, limitOffset, limitCount, hasLimit).DbOp(tx)
		if err != nil {
			return nil, err
		}
		flat := result.([][]byte)

		for i, j := 0, len(flat)-1; i < j; i, j = i+1, j-1 {
			flat[i], flat[j] = flat[j], flat[i]
		}
		return flat, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZLexCount(session *common.Session, key []byte, minStr, maxStr string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return 0, err
		}

		members := make([][]byte, len(entries))
		for i, e := range entries {
			members[i] = e.Member
		}

		result, err := filterLexRange(members, minStr, maxStr)
		if err != nil {
			return 0, err
		}
		return len(result), nil
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

func ZRemRangeByRank(session *common.Session, key []byte, start, stop int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return 0, err
		}

		n := len(entries)
		lo, hi, ok := rangeIndexes(n, start, stop)
		if !ok {
			return 0, nil
		}

		removed := 0
		for i := lo; i <= hi; i++ {
			e := entries[i]
			if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
				return removed, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
				return removed, err
			}
			removed++
		}

		newCount := n - removed
		if newCount == 0 {
			return removed, tx.Delete(session.PublicKey(key))
		}
		return removed, common.WriteUint32Sentinel(tx, session, key, uint32(newCount), common.RedisSortedSet)
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

func ZRemRangeByScore(session *common.Session, key []byte, minStr, maxStr string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return 0, err
		}

		minVal, minExcl, err := parseFloatBound(minStr)
		if err != nil {
			return 0, err
		}
		maxVal, maxExcl, err := parseFloatBound(maxStr)
		if err != nil {
			return 0, err
		}

		removed := 0
		for _, e := range entries {
			inRange := true
			if minExcl && e.Score <= minVal {
				inRange = false
			}
			if !minExcl && e.Score < minVal {
				inRange = false
			}
			if maxExcl && e.Score >= maxVal {
				inRange = false
			}
			if !maxExcl && e.Score > maxVal {
				inRange = false
			}
			if !inRange {
				continue
			}

			if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
				return removed, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
				return removed, err
			}
			removed++
		}

		if removed == 0 {
			return 0, nil
		}

		newCount, err := common.ReadUint32Sentinel(tx, session, key)
		if err != nil {
			return removed, err
		}
		newCount -= uint32(removed)
		if newCount == 0 {
			return removed, tx.Delete(session.PublicKey(key))
		}
		return removed, common.WriteUint32Sentinel(tx, session, key, newCount, common.RedisSortedSet)
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

func ZRemRangeByLex(session *common.Session, key []byte, minStr, maxStr string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return 0, err
		}

		members := make([][]byte, len(entries))
		for i, e := range entries {
			members[i] = e.Member
		}

		toRemove, err := filterLexRange(members, minStr, maxStr)
		if err != nil {
			return 0, err
		}

		removed := 0
		removeMap := make(map[string]bool)
		for _, m := range toRemove {
			removeMap[string(m)] = true
		}

		// Use the entries to build the remove list with scores
		for _, e := range entries {
			if !removeMap[string(e.Member)] {
				continue
			}
			member := e.Member
			if err := tx.Delete(session.PrivateKey(memberCompound(key, member))); err != nil {
				return removed, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, member))); err != nil {
				return removed, err
			}
			removed++
		}

		if removed == 0 {
			return 0, nil
		}

		newCount, err := common.ReadUint32Sentinel(tx, session, key)
		if err != nil {
			return removed, err
		}
		newCount -= uint32(removed)
		if newCount == 0 {
			return removed, tx.Delete(session.PublicKey(key))
		}
		return removed, common.WriteUint32Sentinel(tx, session, key, newCount, common.RedisSortedSet)
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

// --- Pop commands ---

// popOneMin removes and returns the single element with the lowest score, or nil if empty.
// It is the shared primitive used by ZPopMin, BZPOPMIN claim-side, and ZAdd wakeup.
func popOneMin(tx kv.Tx, session *common.Session, key []byte) (*MemberScore, error) {
	entries, err := loadAllMembers(tx, session, key)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	e := entries[0]
	if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
		return &e, err
	}
	if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
		return &e, err
	}
	newCount := uint32(len(entries) - 1)
	if newCount == 0 {
		return &e, tx.Delete(session.PublicKey(key))
	}
	return &e, common.WriteUint32Sentinel(tx, session, key, newCount, common.RedisSortedSet)
}

// popOneMax removes and returns the single element with the highest score, or nil if empty.
func popOneMax(tx kv.Tx, session *common.Session, key []byte) (*MemberScore, error) {
	entries, err := loadAllMembers(tx, session, key)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	e := entries[len(entries)-1]
	if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
		return &e, err
	}
	if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
		return &e, err
	}
	newCount := uint32(len(entries) - 1)
	if newCount == 0 {
		return &e, tx.Delete(session.PublicKey(key))
	}
	return &e, common.WriteUint32Sentinel(tx, session, key, newCount, common.RedisSortedSet)
}

func ZPopMin(session *common.Session, key []byte, count int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		if count > len(entries) {
			count = len(entries)
		}

		popped := entries[:count]
		for _, e := range popped {
			if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
				return popped, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
				return popped, err
			}
		}

		newCount := len(entries) - count
		if newCount == 0 {
			return popped, tx.Delete(session.PublicKey(key))
		}
		return popped, common.WriteUint32Sentinel(tx, session, key, uint32(newCount), common.RedisSortedSet)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeMemberScoreArray(conn, result.([]MemberScore))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func ZPopMax(session *common.Session, key []byte, count int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		if count > len(entries) {
			count = len(entries)
		}

		popped := entries[len(entries)-count:]
		for _, e := range popped {
			if err := tx.Delete(session.PrivateKey(memberCompound(key, e.Member))); err != nil {
				return popped, err
			}
			if err := tx.Delete(session.PrivateKey(scoreCompound(key, e.Score, e.Member))); err != nil {
				return popped, err
			}
		}

		newCount := len(entries) - count
		if newCount == 0 {
			return popped, tx.Delete(session.PublicKey(key))
		}
		return popped, common.WriteUint32Sentinel(tx, session, key, uint32(newCount), common.RedisSortedSet)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeMemberScoreArray(conn, result.([]MemberScore))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func writeMemberScoreArray(conn redcon.Conn, members []MemberScore) {
	conn.WriteArray(len(members) * 2)
	for _, e := range members {
		conn.WriteBulk(e.Member)
		conn.WriteBulkString(strconv.FormatFloat(e.Score, 'f', -1, 64))
	}
}

// --- Multi-score ---

func ZMScore(session *common.Session, key []byte, members ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		scores := make([]float64, len(members))
		found := make([]bool, len(members))

		for i, member := range members {
			item, err := tx.Get(session.PrivateKey(memberCompound(key, member)))
			if err == kv.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			val, err := item.Value()
			if err != nil {
				return nil, err
			}
			scores[i] = math.Float64frombits(binary.BigEndian.Uint64(val))
			found[i] = true
		}

		return zmscoreResult{scores: scores, found: found}, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		res := result.(zmscoreResult)
		conn.WriteArray(len(res.scores))
		for i, s := range res.scores {
			if res.found[i] {
				conn.WriteBulkString(strconv.FormatFloat(s, 'f', -1, 64))
			} else {
				conn.WriteNull()
			}
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// zmscoreResult carries parallel score/found slices between the DbOp and WireOp.
type zmscoreResult struct {
	scores []float64
	found  []bool
}

// --- Random member ---

func ZRandMember(session *common.Session, key []byte, count int) common.QueuedOp {
	withScores := count < 0

	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, key)
		if err != nil {
			return nil, err
		}

		n := len(entries)
		if count == 0 || n == 0 {
			return []MemberScore{}, nil
		}

		if count < 0 {
			count = -count
		}

		if count >= n {
			count = n
		}

		// Pick count random distinct entries
		perm := rand.Perm(n)
		result := make([]MemberScore, count)
		for i := 0; i < count; i++ {
			result[i] = entries[perm[i]]
		}

		if !withScores {
			for i := range result {
				result[i].Score = 0
			}
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		members := result.([]MemberScore)
		if withScores {
			writeMemberScoreArray(conn, members)
		} else {
			conn.WriteArray(len(members))
			for _, e := range members {
				conn.WriteBulk(e.Member)
			}
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// --- Set operations ---

func loadZSetMap(tx kv.Tx, session *common.Session, setName []byte) (map[string]float64, error) {
	entries, err := loadAllMembers(tx, session, setName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]float64, len(entries))
	for _, e := range entries {
		result[string(e.Member)] = e.Score
	}
	return result, nil
}

func zsetToSlice(m map[string]float64) []MemberScore {
	result := make([]MemberScore, 0, len(m))
	for member, score := range m {
		result = append(result, MemberScore{Member: []byte(member), Score: score})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score < result[j].Score
		}
		return string(result[i].Member) < string(result[j].Member)
	})
	return result
}

func storeZSetResult(tx kv.Tx, session *common.Session, dest []byte, members []MemberScore) (int, error) {
	if err := clearInternalKeys(tx, session, dest); err != nil {
		return 0, err
	}

	for _, e := range members {
		if err := tx.Set(session.NewPrivateEntry(scoreCompound(dest, e.Score, e.Member), nil)); err != nil {
			return 0, err
		}
		if err := tx.Set(session.NewPrivateEntry(memberCompound(dest, e.Member), scoreBytes(e.Score))); err != nil {
			return 0, err
		}
	}

	return len(members), common.WriteUint32Sentinel(tx, session, dest, uint32(len(members)), common.RedisSortedSet)
}

// applyWeights multiplies every member's score by the first weight, if any were supplied.
func applyWeights(m map[string]float64, weights []float64) {
	if len(weights) > 0 {
		for member, score := range m {
			m[member] = score * weights[0]
		}
	}
}

func mergeScores(maps ...map[string]float64) map[string]float64 {
	result := make(map[string]float64)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func intersectScores(aggregate string, maps ...map[string]float64) map[string]float64 {
	if len(maps) == 0 {
		return nil
	}

	result := make(map[string]float64)
	for member := range maps[0] {
		present := true
		var scores []float64
		for _, m := range maps {
			if s, ok := m[member]; ok {
				scores = append(scores, s)
			} else {
				present = false
				break
			}
		}
		if !present {
			continue
		}

		score := scores[0]
		switch strings.ToUpper(aggregate) {
		case "SUM":
			score = 0
			for _, s := range scores {
				score += s
			}
		case "MIN":
			score = scores[0]
			for _, s := range scores[1:] {
				if s < score {
					score = s
				}
			}
		case "MAX":
			score = scores[0]
			for _, s := range scores[1:] {
				if s > score {
					score = s
				}
			}
		}
		result[member] = score
	}
	return result
}

func zdiff(tx kv.Tx, session *common.Session, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}

	first, err := loadZSetMap(tx, session, keys[0])
	if err != nil {
		return nil, err
	}

	for _, key := range keys[1:] {
		other, err := loadZSetMap(tx, session, key)
		if err != nil {
			return nil, err
		}
		for m := range other {
			delete(first, m)
		}
	}
	return first, nil
}

func zinter(tx kv.Tx, session *common.Session, aggregate string, weights []float64, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}

	maps := make([]map[string]float64, len(keys))
	for i, key := range keys {
		m, err := loadZSetMap(tx, session, key)
		if err != nil {
			return nil, err
		}
		maps[i] = m
	}

	result := intersectScores(aggregate, maps...)
	applyWeights(result, weights)
	return result, nil
}

func zunion(tx kv.Tx, session *common.Session, aggregate string, weights []float64, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}

	maps := make([]map[string]float64, len(keys))
	for i, key := range keys {
		m, err := loadZSetMap(tx, session, key)
		if err != nil {
			return nil, err
		}
		maps[i] = m
	}

	union := mergeScores(maps...)

	// Apply aggregate to union: all scores get summed/minned/maxed
	// For union, members present in multiple sources need aggregation
	// First, track member->sources mapping
	memberSources := make(map[string][]float64)
	for _, m := range maps {
		for member, score := range m {
			memberSources[member] = append(memberSources[member], score)
		}
	}

	for member, scores := range memberSources {
		if len(scores) == 1 {
			union[member] = scores[0]
		} else {
			switch strings.ToUpper(aggregate) {
			case "MIN":
				min := scores[0]
				for _, s := range scores[1:] {
					if s < min {
						min = s
					}
				}
				union[member] = min
			case "MAX":
				max := scores[0]
				for _, s := range scores[1:] {
					if s > max {
						max = s
					}
				}
				union[member] = max
			default: // SUM
				sum := 0.0
				for _, s := range scores {
					sum += s
				}
				union[member] = sum
			}
		}
	}

	applyWeights(union, weights)
	return union, nil
}

func flattenMembers(members []MemberScore, withScores bool) [][]byte {
	if withScores {
		result := make([][]byte, 0, len(members)*2)
		for _, e := range members {
			result = append(result, e.Member)
			result = append(result, []byte(strconv.FormatFloat(e.Score, 'f', -1, 64)))
		}
		return result
	}
	result := make([][]byte, len(members))
	for i, e := range members {
		result[i] = e.Member
	}
	return result
}

func ZDiff(session *common.Session, withScores bool, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zdiff(tx, session, keys...)
		if err != nil {
			return nil, err
		}
		return flattenMembers(zsetToSlice(m), withScores), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZDiffStore(session *common.Session, dest []byte, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zdiff(tx, session, keys...)
		if err != nil {
			return 0, err
		}
		return storeZSetResult(tx, session, dest, zsetToSlice(m))
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

func ZInter(session *common.Session, aggregate string, weights []float64, withScores bool, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zinter(tx, session, aggregate, weights, keys...)
		if err != nil {
			return nil, err
		}
		return flattenMembers(zsetToSlice(m), withScores), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZInterStore(session *common.Session, dest []byte, aggregate string, weights []float64, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zinter(tx, session, aggregate, weights, keys...)
		if err != nil {
			return 0, err
		}
		return storeZSetResult(tx, session, dest, zsetToSlice(m))
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

func ZUnion(session *common.Session, aggregate string, weights []float64, withScores bool, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zunion(tx, session, aggregate, weights, keys...)
		if err != nil {
			return nil, err
		}
		return flattenMembers(zsetToSlice(m), withScores), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		writeBulkArray(conn, result.([][]byte))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func ZUnionStore(session *common.Session, dest []byte, aggregate string, weights []float64, keys ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		m, err := zunion(tx, session, aggregate, weights, keys...)
		if err != nil {
			return 0, err
		}
		return storeZSetResult(tx, session, dest, zsetToSlice(m))
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

func ZRangeStore(session *common.Session, dest, src []byte, start, stop int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		entries, err := loadAllMembers(tx, session, src)
		if err != nil {
			return 0, err
		}

		lo, hi, ok := rangeIndexes(len(entries), start, stop)
		if !ok {
			_, err := storeZSetResult(tx, session, dest, nil)
			return 0, err
		}

		return storeZSetResult(tx, session, dest, entries[lo:hi+1])
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

// --- Blocking pop commands ---

// bzpopResult is the value returned when a blocking pop succeeds (either immediately
// or after being woken by a writer).
type bzpopResult struct {
	key    string
	member []byte
	score  float64
}

// bzpop is the shared implementation for BZPOPMIN and BZPOPMAX.
//
// Step 1: attempt an immediate non-blocking pop across keys (first non-empty key wins).
// Step 2: if all keys are empty and session.ShouldBlock() is true, register a waiter
// and block.  If ShouldBlock() is false (MULTI/EXEC or Lua script context), reply with
// a null array immediately instead — the same reply a real timeout would produce.
//
// wantMin=true for BZPOPMIN, false for BZPOPMAX.
// timeout==0 means block indefinitely.
func bzpop(session *common.Session, conn redcon.Conn, keys [][]byte, timeout float64, wantMin bool) {
	kvs := session.KVS()

	// Step 1: try an immediate pop across all keys in order.
	var immediate *bzpopResult
	err := kvs.Update(func(tx kv.Tx) error {
		for _, k := range keys {
			var popped *MemberScore
			var popErr error
			if wantMin {
				popped, popErr = popOneMin(tx, session, k)
			} else {
				popped, popErr = popOneMax(tx, session, k)
			}
			if popErr != nil {
				return popErr
			}
			if popped != nil {
				immediate = &bzpopResult{
					key:    string(k),
					member: popped.Member,
					score:  popped.Score,
				}
				return nil
			}
		}
		return nil
	})

	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	if immediate != nil {
		writeBZPopReply(conn, immediate.key, immediate.member, immediate.score)
		return
	}

	// All listed keys were empty.  If we are inside a MULTI/EXEC block or a Lua
	// script, blocking is forbidden — reply with null immediately.
	if !session.ShouldBlock() {
		conn.WriteNull()
		return
	}

	// Step 2: register a waiter and block.
	keyStrs := make([]string, len(keys))
	for i, k := range keys {
		keyStrs[i] = string(session.PublicKey(k))
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		d := time.Duration(float64(time.Second) * timeout)
		ctx, cancel = context.WithTimeout(context.Background(), d)
	}
	defer cancel()

	res, ok := common.GlobalWatchRegistry.Block(ctx, keyStrs, wantMin)
	if !ok {
		// Timed out with no writer — return null array.
		conn.WriteNull()
		return
	}

	writeBZPopReply(conn, res.Key, res.Member, res.Score)
}

func writeBZPopReply(conn redcon.Conn, key string, member []byte, score float64) {
	conn.WriteArray(3)
	conn.WriteBulkString(key)
	conn.WriteBulk(member)
	conn.WriteBulkString(common.FormatFloat(score))
}

// BZPopMin implements BZPOPMIN. It writes directly to conn and does NOT enqueue a
// QueuedOp — callers must NOT call session.DispatchPendingOps after this.
func BZPopMin(session *common.Session, conn redcon.Conn, keys [][]byte, timeout float64) {
	bzpop(session, conn, keys, timeout, true)
}

// BZPopMax implements BZPOPMAX. It writes directly to conn and does NOT enqueue a
// QueuedOp — callers must NOT call session.DispatchPendingOps after this.
func BZPopMax(session *common.Session, conn redcon.Conn, keys [][]byte, timeout float64) {
	bzpop(session, conn, keys, timeout, false)
}

// --- Parsing helpers ---

func parseFloatBound(s string) (val float64, exclusive bool, err error) {
	if s == "+inf" || s == "inf" {
		return math.Inf(1), false, nil
	}
	if s == "-inf" {
		return math.Inf(-1), false, nil
	}

	if strings.HasPrefix(s, "(") {
		val, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return 0, false, fmt.Errorf("ERR min or max value is not a float")
		}
		return val, true, nil
	}

	val, err = strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, fmt.Errorf("ERR min or max value is not a float")
	}
	return val, false, nil
}

func parseLexBound(s string) (val []byte, exclusive bool, err error) {
	if s == "+" {
		return nil, false, nil
	}
	if s == "-" {
		return nil, false, nil
	}

	if strings.HasPrefix(s, "(") {
		return []byte(s[1:]), true, nil
	}
	if strings.HasPrefix(s, "[") {
		return []byte(s[1:]), false, nil
	}

	return nil, false, fmt.Errorf("ERR min or max value is not a string")
}
