package bitmap

import (
	"math/bits"
	"strings"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

type BitOpType int

const (
	BitOpAND   BitOpType = iota
	BitOpOR
	BitOpXOR
	BitOpNOT
	BitOpDIFF
	BitOpDIFF1
	BitOpANDOR
	BitOpONE
)

func ParseBitOp(op string) (BitOpType, bool) {
	switch strings.ToUpper(op) {
	case "AND":
		return BitOpAND, true
	case "OR":
		return BitOpOR, true
	case "XOR":
		return BitOpXOR, true
	case "NOT":
		return BitOpNOT, true
	case "DIFF":
		return BitOpDIFF, true
	case "DIFF1":
		return BitOpDIFF1, true
	case "ANDOR":
		return BitOpANDOR, true
	case "ONE":
		return BitOpONE, true
	default:
		return 0, false
	}
}

func BitPosInRange(data []byte, startBit, endBit int, bit int, ignoreTrailingZero bool) int {
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

func SetBit(session *common.Session, key []byte, offset, value int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		byteIndex := offset / 8
		bitPos := uint(7 - (offset % 8))
		mask := byte(1 << bitPos)

		item, err := tx.Get(session.PublicKey(key))
		var data []byte
		if err == kv.ErrKeyNotFound {
			data = make([]byte, byteIndex+1)
		} else if err != nil {
			return 0, err
		} else {
			data, err = item.Value()
			if err != nil {
				return 0, err
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

		entry := session.NewPublicEntry(key, data).Metadata(byte(common.RedisString))
		if err := tx.Set(entry); err != nil {
			return 0, err
		}

		return oldBit, nil
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

func GetBit(session *common.Session, key []byte, offset int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			return 0, nil
		}
		data, err := item.Value()
		if err != nil {
			return 0, err
		}

		byteIndex := offset / 8
		if byteIndex >= len(data) {
			return 0, nil
		}

		bitPos := uint(7 - (offset % 8))
		if data[byteIndex]&(1<<bitPos) != 0 {
			return 1, nil
		}
		return 0, nil
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

func BitCount(session *common.Session, key []byte, startGiven, endGiven bool, startVal, endVal int, useBit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			return 0, nil
		}
		data, err := item.Value()
		if err != nil {
			return 0, err
		}

		if !startGiven {
			count := 0
			for _, b := range data {
				count += bits.OnesCount8(b)
			}
			return count, nil
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
				return 0, nil
			}
			count := 0
			for bit := startVal; bit <= endVal; bit++ {
				if data[bit/8]&(1<<(7-uint(bit%8))) != 0 {
					count++
				}
			}
			return count, nil
		}

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
			return 0, nil
		}
		count := 0
		for i := startVal; i <= endVal; i++ {
			count += bits.OnesCount8(data[i])
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

func BitPos(session *common.Session, key []byte, bit int, startGiven bool, startVal, endVal int, useBit bool) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			if bit == 0 {
				return 0, nil
			}
			return -1, nil
		}
		data, err := item.Value()
		if err != nil {
			return 0, err
		}

		if !startGiven {
			pos := BitPosInRange(data, 0, len(data)*8-1, bit, false)
			if pos >= 0 {
				return pos, nil
			}
			if bit == 0 {
				return len(data) * 8, nil
			}
			return -1, nil
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
				return -1, nil
			}
			pos := BitPosInRange(data, startVal, endVal, bit, false)
			if pos >= 0 {
				return pos, nil
			}
			return -1, nil
		}

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
			return -1, nil
		}
		startBit := startVal * 8
		endBit := (endVal * 8) + 7
		if endBit >= len(data)*8 {
			endBit = len(data)*8 - 1
		}
		pos := BitPosInRange(data, startBit, endBit, bit, false)
		if pos >= 0 {
			return pos, nil
		}
		return -1, nil
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

func BitOp(session *common.Session, destKey []byte, op BitOpType, srcKeys [][]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		sources := make([][]byte, len(srcKeys))
		for i, sk := range srcKeys {
			item, err := tx.Get(session.PublicKey(sk))
			if err != nil {
				continue
			}
			data, err := item.Value()
			if err != nil {
				return 0, err
			}
			sources[i] = data
		}

		maxLen := 0
		for _, s := range sources {
			if s != nil && len(s) > maxLen {
				maxLen = len(s)
			}
		}

		result := make([]byte, maxLen)
		switch op {
		case BitOpAND:
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
		case BitOpOR:
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
		case BitOpXOR:
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
		case BitOpNOT:
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
		case BitOpDIFF:
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
		case BitOpDIFF1:
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
		case BitOpANDOR:
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
		case BitOpONE:
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

		entry := session.NewPublicEntry(destKey, result).Metadata(byte(common.RedisString))
		if err := tx.Set(entry); err != nil {
			return 0, err
		}

		return maxLen, nil
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
