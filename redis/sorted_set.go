package redis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

// Sorted set key structure:
//   Sentinel:   {db}:{keyname}                      → 4-byte uint32 count, meta=RedisSortedSet
//   Score idx:  -{db}:{keyname}:score:{8B encScore}:{member}  → empty value
//   Member idx: -{db}:{keyname}:member:{member}               → 8-byte big-endian score

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

func internalScoreKey(setName []byte, score float64, member []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	prefix = append(prefix, ":score:"...)
	prefix = append(prefix, encodeScore(score)...)
	prefix = append(prefix, ':')
	return append(prefix, member...)
}

func internalMemberKey(setName []byte, member []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	return append(prefix, ":member:"...)
}

func internalMemberFullKey(setName []byte, member []byte, dbSlot int) []byte {
	prefix := internalMemberKey(setName, member, dbSlot)
	return append(prefix, member...)
}

func scorePrefixBytes(setName []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	return append(prefix, ":score:"...)
}

func memberPrefixBytes(setName []byte, dbSlot int) []byte {
	prefix := append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...)
	prefix = append(prefix, setName...)
	return append(prefix, ":member:"...)
}

func makeScoreEntry(setName []byte, score float64, member []byte, dbSlot int) *badger.Entry {
	return badger.NewEntry(internalScoreKey(setName, score, member, dbSlot), nil)
}

func makeMemberEntry(setName []byte, score float64, member []byte, dbSlot int) *badger.Entry {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(score))
	return badger.NewEntry(internalMemberFullKey(setName, member, dbSlot), buf)
}

func readZSetCount(txn *badger.Txn, setName []byte, dbSlot int) (uint32, error) {
	item, err := txn.Get(rawKeyPrefixWithDb(setName, dbSlot))
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

func writeZSetCount(txn *badger.Txn, setName []byte, count uint32, dbSlot int) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	return txn.SetEntry(badger.NewEntry(rawKeyPrefixWithDb(setName, dbSlot), buf).WithMeta(byte(RedisSortedSet)))
}

// memberScore holds a member and its score, used for sorted iteration
type memberScore struct {
	member []byte
	score  float64
}

// loadAllSortedMembers returns all members in score order (ascending, with lex tie-break).
func loadAllSortedMembers(txn *badger.Txn, setName []byte, dbSlot int) ([]memberScore, error) {
	prefix := scorePrefixBytes(setName, dbSlot)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	var result []memberScore
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Item().KeyCopy(nil)
		encScore := k[len(prefix) : len(prefix)+8]
		member := k[len(prefix)+8+1:] // skip : after encoded score
		result = append(result, memberScore{
			member: member,
			score:  decodeScore(encScore),
		})
	}
	return result, nil
}

// loadAllMembersOnly returns all member byte slices without scores.
func deleteAllInternalKeys(txn *badger.Txn, setName []byte, dbSlot int) error {
	scorePrefix := scorePrefixBytes(setName, dbSlot)
	scoreOpts := badger.DefaultIteratorOptions
	scoreOpts.Prefix = scorePrefix
	it := txn.NewIterator(scoreOpts)
	for it.Rewind(); it.Valid(); it.Next() {
		if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
			it.Close()
			return err
		}
	}
	it.Close()

	memberPrefix := memberPrefixBytes(setName, dbSlot)
	memberOpts := badger.DefaultIteratorOptions
	memberOpts.Prefix = memberPrefix
	it2 := txn.NewIterator(memberOpts)
	for it2.Rewind(); it2.Valid(); it2.Next() {
		if err := txn.Delete(it2.Item().KeyCopy(nil)); err != nil {
			it2.Close()
			return err
		}
	}
	it2.Close()

	return txn.Delete(rawKeyPrefixWithDb(setName, dbSlot))
}

// --- Command implementations ---

func zadd(txn *badger.Txn, conn redcon.Conn, key []byte, args ...[]byte) (int, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'zadd' command")
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
		return 0, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
	}
	if nx && (gt || lt) {
		return 0, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
	}
	if xx && (gt || lt) {
		return 0, fmt.Errorf("ERR XX and NX, XX and GT/LT, NX and GT/LT options are not compatible")
	}
	if gt && lt {
		return 0, fmt.Errorf("ERR GT and LT options are not compatible")
	}

	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'zadd' command")
	}

	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	var count uint32
	item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
	if err == badger.ErrKeyNotFound {
		if xx {
			return 0, nil
		}
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

	changed := 0
	added := 0

	for i := 0; i < len(remaining); i += 2 {
		member := remaining[i+1]
		scoreStr := string(remaining[i])
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil || math.IsNaN(score) {
			return 0, fmt.Errorf("ERR value is not a valid float")
		}

		memberKey := internalMemberFullKey(key, member, dbSlot)
		existingItem, existingErr := txn.Get(memberKey)

		if existingErr == badger.ErrKeyNotFound {
			if nx || xx {
				if nx {
					// Add new
					if err := txn.SetEntry(makeScoreEntry(key, score, member, dbSlot)); err != nil {
						return changed + added, err
					}
					if err := txn.SetEntry(makeMemberEntry(key, score, member, dbSlot)); err != nil {
						return changed + added, err
					}
					count++
					added++
				}
				// xx would skip non-existing
			} else {
				if err := txn.SetEntry(makeScoreEntry(key, score, member, dbSlot)); err != nil {
					return changed + added, err
				}
				if err := txn.SetEntry(makeMemberEntry(key, score, member, dbSlot)); err != nil {
					return changed + added, err
				}
				count++
				added++
			}
		} else if existingErr != nil {
			return 0, existingErr
		} else {
			// Member exists
			if nx {
				continue
			}

			var oldScore float64
			if err := existingItem.Value(func(val []byte) error {
				oldScore = math.Float64frombits(binary.BigEndian.Uint64(val))
				return nil
			}); err != nil {
				return 0, err
			}

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
				if err := txn.Delete(internalScoreKey(key, oldScore, member, dbSlot)); err != nil {
					return changed + added, err
				}
				// Add new score entry
				if err := txn.SetEntry(makeScoreEntry(key, score, member, dbSlot)); err != nil {
					return changed + added, err
				}
				// Update member entry
				if err := txn.SetEntry(makeMemberEntry(key, score, member, dbSlot)); err != nil {
					return changed + added, err
				}
				changed++
			}
		}
	}

	if added > 0 || changed > 0 {
		if err := writeZSetCount(txn, key, count, dbSlot); err != nil {
			return changed + added, err
		}
	}

	if ch {
		return changed + added, nil
	}
	return added, nil
}

func zcard(txn *badger.Txn, conn redcon.Conn, key []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	count, err := readZSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func zscore(txn *badger.Txn, conn redcon.Conn, key, member []byte) (float64, bool, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	item, err := txn.Get(internalMemberFullKey(key, member, dbSlot))
	if err == badger.ErrKeyNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	var score float64
	if err := item.Value(func(val []byte) error {
		score = math.Float64frombits(binary.BigEndian.Uint64(val))
		return nil
	}); err != nil {
		return 0, false, err
	}
	return score, true, nil
}

func zrem(txn *badger.Txn, conn redcon.Conn, key []byte, members ...[]byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	count, err := readZSetCount(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, member := range members {
		memberKey := internalMemberFullKey(key, member, dbSlot)
		item, err := txn.Get(memberKey)
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return removed, err
		}

		var score float64
		if err := item.Value(func(val []byte) error {
			score = math.Float64frombits(binary.BigEndian.Uint64(val))
			return nil
		}); err != nil {
			return removed, err
		}

		if err := txn.Delete(memberKey); err != nil {
			return removed, err
		}
		if err := txn.Delete(internalScoreKey(key, score, member, dbSlot)); err != nil {
			return removed, err
		}
		count--
		removed++
	}

	if count == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return removed, writeZSetCount(txn, key, count, dbSlot)
}

func zrange(txn *badger.Txn, conn redcon.Conn, key []byte, start, stop int, withScores bool) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	n := len(entries)
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
		return [][]byte{}, nil
	}

	size := stop - start + 1
	if withScores {
		result := make([][]byte, 0, size*2)
		for i := start; i <= stop; i++ {
			result = append(result, entries[i].member)
			result = append(result, []byte(strconv.FormatFloat(entries[i].score, 'f', -1, 64)))
		}
		return result, nil
	}

	result := make([][]byte, size)
	for i := start; i <= stop; i++ {
		result[i-start] = entries[i].member
	}
	return result, nil
}

func zrevrange(txn *badger.Txn, conn redcon.Conn, key []byte, start, stop int, withScores bool) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	n := len(entries)
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
		return [][]byte{}, nil
	}

	// Reverse: we iterate from (n-1-start) down to (n-1-stop) in the entries
	hi := n - 1 - start
	lo := n - 1 - stop
	if lo < 0 {
		lo = 0
	}

	size := hi - lo + 1
	if withScores {
		result := make([][]byte, 0, size*2)
		for i := hi; i >= lo; i-- {
			result = append(result, entries[i].member)
			result = append(result, []byte(strconv.FormatFloat(entries[i].score, 'f', -1, 64)))
		}
		return result, nil
	}

	result := make([][]byte, size)
	idx := 0
	for i := hi; i >= lo; i-- {
		result[idx] = entries[i].member
		idx++
	}
	return result, nil
}

func zrank(txn *badger.Txn, conn redcon.Conn, key, member []byte) (int, bool, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	score, found, err := zscore(txn, conn, key, member)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err != nil {
		return 0, false, err
	}

	for i, e := range entries {
		if bytes.Equal(e.member, member) && e.score == score {
			return i, true, nil
		}
	}
	return 0, false, nil
}

func zrevrank(txn *badger.Txn, conn redcon.Conn, key, member []byte) (int, bool, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	score, found, err := zscore(txn, conn, key, member)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err != nil {
		return 0, false, err
	}

	n := len(entries)
	for i, e := range entries {
		if bytes.Equal(e.member, member) && e.score == score {
			return n - 1 - i, true, nil
		}
	}
	return 0, false, nil
}

func zcount(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
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
		if minExcl && e.score <= minVal {
			continue
		}
		if !minExcl && e.score < minVal {
			continue
		}
		if maxExcl && e.score >= maxVal {
			continue
		}
		if !maxExcl && e.score > maxVal {
			continue
		}
		count++
	}
	return count, nil
}

func zincrby(txn *badger.Txn, conn redcon.Conn, key []byte, increment float64, member []byte) (float64, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	score, found, err := zscore(txn, conn, key, member)
	if err != nil {
		return 0, err
	}

	if !found {
		// Add new member with increment as score
		if err := txn.SetEntry(makeScoreEntry(key, increment, member, dbSlot)); err != nil {
			return 0, err
		}
		if err := txn.SetEntry(makeMemberEntry(key, increment, member, dbSlot)); err != nil {
			return 0, err
		}
		// Update count
		count, err := readZSetCount(txn, key, dbSlot)
		if err == badger.ErrKeyNotFound {
			count = 1
		} else if err != nil {
			return 0, err
		} else {
			count++
		}
		if err := writeZSetCount(txn, key, count, dbSlot); err != nil {
			return 0, err
		}
		return increment, nil
	}

	newScore := score + increment
	if math.IsNaN(newScore) {
		return 0, fmt.Errorf("ERR resulting score is not a valid float")
	}

	// Remove old score entry
	if err := txn.Delete(internalScoreKey(key, score, member, dbSlot)); err != nil {
		return 0, err
	}
	// Add new score entry
	if err := txn.SetEntry(makeScoreEntry(key, newScore, member, dbSlot)); err != nil {
		return 0, err
	}
	// Update member entry
	if err := txn.SetEntry(makeMemberEntry(key, newScore, member, dbSlot)); err != nil {
		return 0, err
	}

	return newScore, nil
}

func zrangebyscore(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string, withScores bool, limitOffset, limitCount int, hasLimit bool) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
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

	var filtered []memberScore
	for _, e := range entries {
		if minExcl && e.score <= minVal {
			continue
		}
		if !minExcl && e.score < minVal {
			continue
		}
		if maxExcl && e.score >= maxVal {
			continue
		}
		if !maxExcl && e.score > maxVal {
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
			result = append(result, e.member)
			result = append(result, []byte(strconv.FormatFloat(e.score, 'f', -1, 64)))
		}
		return result, nil
	}

	result := make([][]byte, len(filtered))
	for i, e := range filtered {
		result[i] = e.member
	}
	return result, nil
}

func zrevrangebyscore(txn *badger.Txn, conn redcon.Conn, key []byte, maxStr, minStr string, withScores bool, limitOffset, limitCount int, hasLimit bool) ([][]byte, error) {
	// Note: ZREVRANGEBYSCORE takes max first, then min
	result, err := zrangebyscore(txn, conn, key, minStr, maxStr, withScores, limitOffset, limitCount, hasLimit)
	if err != nil {
		return nil, err
	}

	// Reverse the result
	if withScores {
		for i, j := 0, len(result)-2; i < j; i, j = i+2, j-2 {
			result[i], result[j] = result[j], result[i]
			result[i+1], result[j+1] = result[j+1], result[i+1]
		}
	} else {
		for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
			result[i], result[j] = result[j], result[i]
		}
	}
	return result, nil
}

// --- Lexicographic commands ---

func loadLexMembers(txn *badger.Txn, setName []byte, dbSlot int) ([][]byte, error) {
	entries, err := loadAllSortedMembers(txn, setName, dbSlot)
	if err != nil {
		return nil, err
	}
	result := make([][]byte, len(entries))
	for i, e := range entries {
		result[i] = e.member
	}
	return result, nil
}

// lexRange filters members by the lexicographic range [min, max) or (min, max)
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

func zrangebylex(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string, limitOffset, limitCount int, hasLimit bool) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	members, err := loadLexMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
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

func zrevrangebylex(txn *badger.Txn, conn redcon.Conn, key []byte, maxStr, minStr string, limitOffset, limitCount int, hasLimit bool) ([][]byte, error) {
	result, err := zrangebylex(txn, conn, key, minStr, maxStr, limitOffset, limitCount, hasLimit)
	if err != nil {
		return nil, err
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result, nil
}

func zlexcount(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	members, err := loadLexMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	result, err := filterLexRange(members, minStr, maxStr)
	if err != nil {
		return 0, err
	}
	return len(result), nil
}

func zremrangebyrank(txn *badger.Txn, conn redcon.Conn, key []byte, start, stop int) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	n := len(entries)
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
		return 0, nil
	}

	removed := 0
	for i := start; i <= stop; i++ {
		e := entries[i]
		if err := txn.Delete(internalMemberFullKey(key, e.member, dbSlot)); err != nil {
			return removed, err
		}
		if err := txn.Delete(internalScoreKey(key, e.score, e.member, dbSlot)); err != nil {
			return removed, err
		}
		removed++
	}

	newCount := n - removed
	if newCount == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return removed, writeZSetCount(txn, key, uint32(newCount), dbSlot)
}

func zremrangebyscore(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
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
		if minExcl && e.score <= minVal {
			inRange = false
		}
		if !minExcl && e.score < minVal {
			inRange = false
		}
		if maxExcl && e.score >= maxVal {
			inRange = false
		}
		if !maxExcl && e.score > maxVal {
			inRange = false
		}
		if !inRange {
			continue
		}

		if err := txn.Delete(internalMemberFullKey(key, e.member, dbSlot)); err != nil {
			return removed, err
		}
		if err := txn.Delete(internalScoreKey(key, e.score, e.member, dbSlot)); err != nil {
			return removed, err
		}
		removed++
	}

	if removed == 0 {
		return 0, nil
	}

	newCount, err := readZSetCount(txn, key, dbSlot)
	if err != nil {
		return removed, err
	}
	newCount -= uint32(removed)
	if newCount == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return removed, writeZSetCount(txn, key, newCount, dbSlot)
}

func zremrangebylex(txn *badger.Txn, conn redcon.Conn, key []byte, minStr, maxStr string) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	members := make([][]byte, len(entries))
	for i, e := range entries {
		members[i] = e.member
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
		if !removeMap[string(e.member)] {
			continue
		}
		member := e.member
		if err := txn.Delete(internalMemberFullKey(key, member, dbSlot)); err != nil {
			return removed, err
		}
		if err := txn.Delete(internalScoreKey(key, e.score, member, dbSlot)); err != nil {
			return removed, err
		}
		removed++
	}

	if removed == 0 {
		return 0, nil
	}

	newCount, err := readZSetCount(txn, key, dbSlot)
	if err != nil {
		return removed, err
	}
	newCount -= uint32(removed)
	if newCount == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return removed, writeZSetCount(txn, key, newCount, dbSlot)
}

// --- Pop commands ---

func zpopmin(txn *badger.Txn, conn redcon.Conn, key []byte, count int) ([]memberScore, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if count > len(entries) {
		count = len(entries)
	}

	popped := entries[:count]
	for _, e := range popped {
		if err := txn.Delete(internalMemberFullKey(key, e.member, dbSlot)); err != nil {
			return popped, err
		}
		if err := txn.Delete(internalScoreKey(key, e.score, e.member, dbSlot)); err != nil {
			return popped, err
		}
	}

	newCount := len(entries) - count
	if newCount == 0 {
		return popped, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return popped, writeZSetCount(txn, key, uint32(newCount), dbSlot)
}

func zpopmax(txn *badger.Txn, conn redcon.Conn, key []byte, count int) ([]memberScore, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if count > len(entries) {
		count = len(entries)
	}

	popped := entries[len(entries)-count:]
	for _, e := range popped {
		if err := txn.Delete(internalMemberFullKey(key, e.member, dbSlot)); err != nil {
			return popped, err
		}
		if err := txn.Delete(internalScoreKey(key, e.score, e.member, dbSlot)); err != nil {
			return popped, err
		}
	}

	newCount := len(entries) - count
	if newCount == 0 {
		return popped, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}
	return popped, writeZSetCount(txn, key, uint32(newCount), dbSlot)
}

// --- Multi-score ---

func zmscore(txn *badger.Txn, conn redcon.Conn, key []byte, members ...[]byte) ([]float64, []bool, error) {
	scores := make([]float64, len(members))
	found := make([]bool, len(members))

	for i, member := range members {
		score, ok, err := zscore(txn, conn, key, member)
		if err != nil {
			return nil, nil, err
		}
		scores[i] = score
		found[i] = ok
	}

	return scores, found, nil
}

// --- Random member ---

func zrandmember(txn *badger.Txn, conn redcon.Conn, key []byte, count int) ([][]byte, []float64, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	entries, err := loadAllSortedMembers(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	n := len(entries)
	if count == 0 || n == 0 {
		return [][]byte{}, nil, nil
	}

	withScores := count < 0
	if count < 0 {
		count = -count
	}

	if count >= n {
		members := make([][]byte, n)
		scores := make([]float64, n)
		for i, e := range entries {
			members[i] = e.member
			scores[i] = e.score
		}
		return members, scores, nil
	}

	// Pick count random distinct entries
	perm := rand.Perm(n)
	resultMembers := make([][]byte, count)
	resultScores := make([]float64, count)
	for i := 0; i < count; i++ {
		resultMembers[i] = entries[perm[i]].member
		resultScores[i] = entries[perm[i]].score
	}

	if !withScores {
		return resultMembers, nil, nil
	}
	return resultMembers, resultScores, nil
}

// --- Set operations ---

func loadZSetMap(txn *badger.Txn, setName []byte, dbSlot int) (map[string]float64, error) {
	entries, err := loadAllSortedMembers(txn, setName, dbSlot)
	if err == badger.ErrKeyNotFound {
		return map[string]float64{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make(map[string]float64, len(entries))
	for _, e := range entries {
		result[string(e.member)] = e.score
	}
	return result, nil
}

func zsetToSlice(m map[string]float64) []memberScore {
	result := make([]memberScore, 0, len(m))
	for member, score := range m {
		result = append(result, memberScore{member: []byte(member), score: score})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].score != result[j].score {
			return result[i].score < result[j].score
		}
		return string(result[i].member) < string(result[j].member)
	})
	return result
}

func storeZSetResult(txn *badger.Txn, conn redcon.Conn, dest []byte, members []memberScore) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	_ = deleteAllInternalKeys(txn, dest, dbSlot)

	for _, e := range members {
		if err := txn.SetEntry(makeScoreEntry(dest, e.score, e.member, dbSlot)); err != nil {
			return 0, err
		}
		if err := txn.SetEntry(makeMemberEntry(dest, e.score, e.member, dbSlot)); err != nil {
			return 0, err
		}
	}

	return len(members), writeZSetCount(txn, dest, uint32(len(members)), dbSlot)
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

func zdiff(txn *badger.Txn, conn redcon.Conn, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	first, err := loadZSetMap(txn, keys[0], dbSlot)
	if err != nil {
		return nil, err
	}

	for _, key := range keys[1:] {
		other, err := loadZSetMap(txn, key, dbSlot)
		if err != nil {
			return nil, err
		}
		for m := range other {
			delete(first, m)
		}
	}
	return first, nil
}

func zinter(txn *badger.Txn, conn redcon.Conn, aggregate string, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	maps := make([]map[string]float64, len(keys))
	for i, key := range keys {
		m, err := loadZSetMap(txn, key, dbSlot)
		if err != nil {
			return nil, err
		}
		maps[i] = m
	}

	return intersectScores(aggregate, maps...), nil
}

func zunion(txn *badger.Txn, conn redcon.Conn, aggregate string, keys ...[]byte) (map[string]float64, error) {
	if len(keys) == 0 {
		return map[string]float64{}, nil
	}
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	maps := make([]map[string]float64, len(keys))
	for i, key := range keys {
		m, err := loadZSetMap(txn, key, dbSlot)
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

	return union, nil
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
