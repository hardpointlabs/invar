package redis

import (
	"math/bits"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

func handleSetBit(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if !checkExactArgs(conn, cmd, 4) {
		return
	}
	offset, ok := parseIntArg(conn, cmd.Args[2])
	if !ok {
		return
	}
	if offset < 0 {
		conn.WriteError("ERR bit offset is not an integer or out of range")
		return
	}
	value, ok := parseIntArg(conn, cmd.Args[3])
	if !ok {
		return
	}
	if value != 0 && value != 1 {
		conn.WriteError("ERR bit is not an integer or out of range")
		return
	}

	byteIndex := offset / 8
	bitPos := uint(7 - (offset % 8))
	mask := byte(1 << bitPos)

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
		var data []byte
		if err == badger.ErrKeyNotFound {
			data = make([]byte, byteIndex+1)
		} else if err != nil {
			return err
		} else {
			if item.UserMeta() != byte(RedisString) {
				conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
				return nil
			}
			data, err = copyItemValue(item)
			if err != nil {
				return err
			}
			if byteIndex >= len(data) {
				newData := make([]byte, byteIndex+1)
				copy(newData, data)
				data = newData
			}
		}

		oldBit := int((data[byteIndex] & mask) >> bitPos)

		if value == 1 {
			data[byteIndex] |= mask
		} else {
			data[byteIndex] &^= mask
		}

		e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), data).WithMeta(byte(RedisString))
		if err := txn.SetEntry(e); err != nil {
			return err
		}

		conn.WriteInt(oldBit)
		return nil
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	}
}

func handleGetBit(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if !checkExactArgs(conn, cmd, 3) {
		return
	}
	offset, ok := parseIntArg(conn, cmd.Args[2])
	if !ok {
		return
	}
	if offset < 0 {
		conn.WriteError("ERR bit offset is not an integer or out of range")
		return
	}

	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
		if err != nil {
			conn.WriteInt(0)
			return nil
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		data, err := copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return nil
		}

		byteIndex := offset / 8
		if byteIndex >= len(data) {
			conn.WriteInt(0)
			return nil
		}

		bitPos := uint(7 - (offset % 8))
		if data[byteIndex]&(1<<bitPos) != 0 {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return nil
	})
}

func handleBitCount(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	dbSlot := currentDb(conn)
	key := cmd.Args[1]

	useBit := false
	var startGiven, endGiven bool
	var startVal, endVal int
	i := 2
	if i < len(cmd.Args) {
		v, ok := parseIntArg(conn, cmd.Args[i])
		if ok {
			startVal = v
			startGiven = true
			i++
		}
	}
	if i < len(cmd.Args) {
		v, ok := parseIntArg(conn, cmd.Args[i])
		if ok {
			endVal = v
			endGiven = true
			i++
		}
	}
	if i < len(cmd.Args) {
		unit := strings.ToLower(string(cmd.Args[i]))
		if unit == "bit" {
			useBit = true
		} else if unit != "byte" {
			conn.WriteError("ERR syntax error")
			return
		}
		i++
	}
	if i < len(cmd.Args) {
		conn.WriteError("ERR syntax error")
		return
	}

	if startGiven != endGiven {
		conn.WriteError("ERR syntax error")
		return
	}

	var data []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, dbSlot))
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		data, err = copyItemValue(item)
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteInt(0)
			return
		}
		conn.WriteError("ERR " + err.Error())
		return
	}

	if !startGiven {
		count := 0
		for _, b := range data {
			count += bits.OnesCount8(b)
		}
		conn.WriteInt(count)
		return
	}

	if useBit {
		totalBits := len(data) * 8
		if startVal < 0 {
			startVal = totalBits + startVal
		}
		if endVal < 0 {
			endVal = totalBits + endVal
		}
		if startVal < 0 {
			startVal = 0
		}
		if endVal >= totalBits {
			endVal = totalBits - 1
		}
		if startVal > endVal || startVal >= totalBits {
			conn.WriteInt(0)
			return
		}
		count := 0
		for bit := startVal; bit <= endVal; bit++ {
			if data[bit/8]&(1<<(7-uint(bit%8))) != 0 {
				count++
			}
		}
		conn.WriteInt(count)
	} else {
		if startVal < 0 {
			startVal = len(data) + startVal
		}
		if endVal < 0 {
			endVal = len(data) + endVal
		}
		if startVal < 0 {
			startVal = 0
		}
		if endVal >= len(data) {
			endVal = len(data) - 1
		}
		if startVal > endVal || startVal >= len(data) {
			conn.WriteInt(0)
			return
		}
		count := 0
		for i := startVal; i <= endVal; i++ {
			count += bits.OnesCount8(data[i])
		}
		conn.WriteInt(count)
	}
}

func handleBitPos(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	dbSlot := currentDb(conn)
	key := cmd.Args[1]

	if !checkMinArgs(conn, cmd, 3) {
		return
	}
	bit, ok := parseIntArg(conn, cmd.Args[2])
	if !ok {
		return
	}
	if bit != 0 && bit != 1 {
		conn.WriteError("ERR bit is not an integer or out of range")
		return
	}

	useBit := false
	var startGiven bool
	var startVal, endVal int
	i := 3
	if i < len(cmd.Args) {
		v, ok := parseIntArg(conn, cmd.Args[i])
		if ok {
			startVal = v
			startGiven = true
			i++
		}
	}
	if i < len(cmd.Args) {
		v, ok := parseIntArg(conn, cmd.Args[i])
		if ok {
			endVal = v
			i++
		}
	}
	if i < len(cmd.Args) {
		unit := strings.ToLower(string(cmd.Args[i]))
		if unit == "bit" {
			useBit = true
		} else if unit != "byte" {
			conn.WriteError("ERR syntax error")
			return
		}
		i++
	}
	if i < len(cmd.Args) {
		conn.WriteError("ERR syntax error")
		return
	}

	var data []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, dbSlot))
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisString) {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}
		data, err = copyItemValue(item)
		return err
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if bit == 0 {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(-1)
		}
		return
	}

	if !startGiven {
		pos := bitPosInRange(data, 0, len(data)*8-1, bit, false)
		if pos >= 0 {
			conn.WriteInt(pos)
		} else if bit == 0 {
			conn.WriteInt(len(data) * 8)
		} else {
			conn.WriteInt(-1)
		}
		return
	}

	if useBit {
		totalBits := len(data) * 8
		if startVal < 0 {
			startVal = totalBits + startVal
		}
		if endVal < 0 {
			endVal = totalBits + endVal
		}
		if startVal < 0 {
			startVal = 0
		}
		if endVal >= totalBits {
			endVal = totalBits - 1
		}
		if startVal > endVal {
			conn.WriteInt(-1)
			return
		}
		pos := bitPosInRange(data, startVal, endVal, bit, false)
		if pos >= 0 {
			conn.WriteInt(pos)
		} else {
			conn.WriteInt(-1)
		}
	} else {
		if startVal < 0 {
			startVal = len(data) + startVal
		}
		if endVal < 0 {
			endVal = len(data) + endVal
		}
		if startVal < 0 {
			startVal = 0
		}
		if endVal >= len(data) {
			endVal = len(data) - 1
		}
		if startVal > endVal {
			conn.WriteInt(-1)
			return
		}
		startBit := startVal * 8
		endBit := (endVal * 8) + 7
		if endBit >= len(data)*8 {
			endBit = len(data)*8 - 1
		}
		pos := bitPosInRange(data, startBit, endBit, bit, false)
		if pos >= 0 {
			conn.WriteInt(pos)
		} else if bit == 0 {
			conn.WriteInt(-1)
		} else {
			conn.WriteInt(-1)
		}
	}
}

func bitPosInRange(data []byte, startBit, endBit int, bit int, ignoreTrailingZero bool) int {
	for byteIdx := startBit / 8; byteIdx <= endBit/8 && byteIdx < len(data); byteIdx++ {
		b := data[byteIdx]
		bitStart := 0
		if byteIdx == startBit/8 {
			bitStart = startBit % 8
		}
		bitEnd := 7
		if byteIdx == endBit/8 {
			bitEnd = endBit % 8
		}
		for bitPos := bitStart; bitPos <= bitEnd; bitPos++ {
			mask := byte(1 << (7 - uint(bitPos)))
			isSet := (b & mask) != 0
			if (bit == 1 && isSet) || (bit == 0 && !isSet) {
				return byteIdx*8 + bitPos
			}
		}
	}
	return -1
}

type bitOpType int

const (
	bitOpAND   bitOpType = iota
	bitOpOR
	bitOpXOR
	bitOpNOT
	bitOpDIFF
	bitOpDIFF1
	bitOpANDOR
	bitOpONE
)

func parseBitOp(op string) (bitOpType, bool) {
	switch strings.ToUpper(op) {
	case "AND":
		return bitOpAND, true
	case "OR":
		return bitOpOR, true
	case "XOR":
		return bitOpXOR, true
	case "NOT":
		return bitOpNOT, true
	case "DIFF":
		return bitOpDIFF, true
	case "DIFF1":
		return bitOpDIFF1, true
	case "ANDOR":
		return bitOpANDOR, true
	case "ONE":
		return bitOpONE, true
	default:
		return 0, false
	}
}

func handleBitOp(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'bitop' command")
		return
	}

	op, ok := parseBitOp(string(cmd.Args[1]))
	if !ok {
		conn.WriteError("ERR syntax error")
		return
	}

	if op == bitOpNOT && len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for 'bitop' command")
		return
	}

	destKey := cmd.Args[2]
	srcKeys := cmd.Args[3:]
	dbSlot := currentDb(conn)

	var sources [][]byte
	err := db.View(func(txn *badger.Txn) error {
		for _, sk := range srcKeys {
			item, err := txn.Get(rawKeyPrefix(sk, dbSlot))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					sources = append(sources, nil)
					continue
				}
				return err
			}
			if item.UserMeta() != byte(RedisString) {
				conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
				return nil
			}
			data, err := copyItemValue(item)
			if err != nil {
				return err
			}
			sources = append(sources, data)
		}
		return nil
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	if len(sources) == 0 {
		conn.WriteError("ERR wrong number of arguments for 'bitop' command")
		return
	}

	maxLen := 0
	for _, s := range sources {
		if s != nil && len(s) > maxLen {
			maxLen = len(s)
		}
	}

	result := make([]byte, maxLen)
	switch op {
	case bitOpAND:
		for i := 0; i < maxLen; i++ {
			result[i] = 0xFF
		}
		for _, s := range sources {
			if s == nil {
				for j := 0; j < maxLen; j++ {
					result[j] = 0
				}
				break
			}
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					result[j] &= s[j]
				} else {
					result[j] = 0
				}
			}
		}
	case bitOpOR:
		for _, s := range sources {
			if s == nil {
				continue
			}
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					result[j] |= s[j]
				}
			}
		}
	case bitOpXOR:
		for _, s := range sources {
			if s == nil {
				continue
			}
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					result[j] ^= s[j]
				}
			}
		}
	case bitOpNOT:
		s := sources[0]
		if s == nil {
			for j := 0; j < maxLen; j++ {
				result[j] = 0xFF
			}
		} else {
			for j := 0; j < maxLen; j++ {
				result[j] = ^s[j]
			}
		}
	case bitOpDIFF:
		if sources[0] != nil {
			copy(result, sources[0])
		}
		for i := 1; i < len(sources); i++ {
			s := sources[i]
			if s == nil {
				continue
			}
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					result[j] &^= s[j]
				}
			}
		}
	case bitOpDIFF1:
		if sources[0] != nil {
			for j := 0; j < maxLen; j++ {
				if j < len(sources[0]) {
					result[j] = ^sources[0][j]
				} else {
					result[j] = 0xFF
				}
			}
		} else {
			for j := 0; j < maxLen; j++ {
				result[j] = 0xFF
			}
		}
		hasOne := false
		for i := 1; i < len(sources); i++ {
			s := sources[i]
			if s == nil {
				continue
			}
			hasOne = true
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					result[j] &= s[j]
				} else {
					result[j] = 0
				}
			}
		}
		if !hasOne {
			for j := 0; j < maxLen; j++ {
				result[j] = 0
			}
		}
	case bitOpANDOR:
		if sources[0] != nil {
			copy(result, sources[0])
		}
		orAccum := make([]byte, maxLen)
		hasOne := false
		for i := 1; i < len(sources); i++ {
			s := sources[i]
			if s == nil {
				continue
			}
			hasOne = true
			for j := 0; j < maxLen; j++ {
				if j < len(s) {
					orAccum[j] |= s[j]
				}
			}
		}
		if !hasOne {
			for j := 0; j < maxLen; j++ {
				result[j] = 0
			}
		} else {
			for j := 0; j < maxLen; j++ {
				result[j] &= orAccum[j]
			}
		}
	case bitOpONE:
		for bitPos := 0; bitPos < maxLen*8; bitPos++ {
			count := 0
			for _, s := range sources {
				if s == nil {
					continue
				}
				byteIdx := bitPos / 8
				if byteIdx >= len(s) {
					continue
				}
				if s[byteIdx]&(1<<(7-uint(bitPos%8))) != 0 {
					count++
				}
			}
			if count == 1 {
				result[bitPos/8] |= 1 << (7 - uint(bitPos%8))
			}
		}
	}

	err = db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(destKey, dbSlot), result).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	conn.WriteInt(maxLen)
}
