package redis

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

const (
	HLL_P           = 14
	HLL_Q           = 64 - HLL_P
	HLL_REGISTERS   = 1 << HLL_P
	HLL_P_MASK      = HLL_REGISTERS - 1
	HLL_BITS        = 6
	HLL_REGISTER_MAX = (1 << HLL_BITS) - 1
	HLL_HDR_SIZE    = 16
	HLL_DENSE_SIZE  = HLL_HDR_SIZE + (HLL_REGISTERS*HLL_BITS+7)/8
	HLL_DENSE       = 0
	HLL_ALPHA_INF   = 0.721347520444481703680
)

func createHLL() []byte {
	data := make([]byte, HLL_DENSE_SIZE)
	copy(data[0:4], "HYLL")
	data[4] = HLL_DENSE
	return data
}

func isValidHLL(data []byte) bool {
	if len(data) != HLL_DENSE_SIZE {
		return false
	}
	if string(data[0:4]) != "HYLL" {
		return false
	}
	if data[4] != HLL_DENSE {
		return false
	}
	return true
}

func hllInvalidateCache(data []byte) {
	data[15] |= 1 << 7
}

func hllGetCachedCount(data []byte) (uint64, bool) {
	if data[15]&(1<<7) != 0 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(data[8:16]), true
}

func hllSetCachedCount(data []byte, count uint64) {
	count &^= 1 << 63
	binary.LittleEndian.PutUint64(data[8:16], count)
}

func getRegister(p []byte, regnum int) uint8 {
	b := regnum * HLL_BITS / 8
	fb := regnum * HLL_BITS & 7
	b0 := int(p[b]) >> fb
	if b+1 >= len(p) {
		return uint8(b0 & HLL_REGISTER_MAX)
	}
	fb8 := 8 - fb
	return uint8((b0 | (int(p[b+1]) << fb8)) & HLL_REGISTER_MAX)
}

func setRegister(p []byte, regnum int, val uint8) {
	b := regnum * HLL_BITS / 8
	fb := regnum * HLL_BITS & 7
	v := int(val)
	mask1 := int(HLL_REGISTER_MAX) << fb
	p[b] = uint8((int(p[b]) & ^mask1) | (v << fb))
	if b+1 >= len(p) {
		return
	}
	fb8 := 8 - fb
	mask2 := int(HLL_REGISTER_MAX) >> fb8
	p[b+1] = uint8((int(p[b+1]) & ^mask2) | (v >> fb8))
}

func murmurHash64A(data []byte, seed uint64) uint64 {
	const m uint64 = 0xc6a4a7935bd1e995
	const r = 47

	h := uint64(seed) ^ (uint64(len(data)) * m)

	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data)
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
		data = data[8:]
	}

	switch len(data) {
	case 7:
		h ^= uint64(data[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(data[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(data[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(data[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

func hllPatLen(ele []byte) (index int, count int) {
	hash := murmurHash64A(ele, 0xadc83b19)
	index = int(hash & HLL_P_MASK)
	hash >>= HLL_P
	hash |= 1 << HLL_Q
	count = bits.TrailingZeros64(hash) + 1
	return
}

func hllDenseSet(registers []byte, index int, count uint8) bool {
	oldcount := getRegister(registers, index)
	if count > oldcount {
		setRegister(registers, index, count)
		return true
	}
	return false
}

func hllDenseAdd(registers []byte, ele []byte) bool {
	index, count := hllPatLen(ele)
	return hllDenseSet(registers, index, uint8(count))
}

func hllSigma(x float64) float64 {
	if x == 1.0 {
		return math.Inf(1)
	}
	var zPrime float64
	y := 1.0
	z := x
	for {
		x *= x
		zPrime = z
		z += x * y
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

func hllTau(x float64) float64 {
	if x == 0.0 || x == 1.0 {
		return 0.0
	}
	var zPrime float64
	y := 1.0
	z := 1.0 - x
	for {
		x = math.Sqrt(x)
		zPrime = z
		y *= 0.5
		z -= math.Pow(1.0-x, 2.0) * y
		if zPrime == z {
			break
		}
	}
	return z / 3.0
}

func hllDenseRegHisto(registers []byte, reghisto []int) {
	// Unrolled loop matching Redis for HLL_REGISTERS == 16384 && HLL_BITS == 6
	var h0, h1, h2, h3 [64]int

	for j := 0; j < 1024; j++ {
		r := registers[j*12:]

		r0 := int(r[0]) & 63
		r1 := (int(r[0])>>6 | int(r[1])<<2) & 63
		r2 := (int(r[1])>>4 | int(r[2])<<4) & 63
		r3 := (int(r[2]) >> 2) & 63
		r4 := int(r[3]) & 63
		r5 := (int(r[3])>>6 | int(r[4])<<2) & 63
		r6 := (int(r[4])>>4 | int(r[5])<<4) & 63
		r7 := (int(r[5]) >> 2) & 63
		r8 := int(r[6]) & 63
		r9 := (int(r[6])>>6 | int(r[7])<<2) & 63
		r10 := (int(r[7])>>4 | int(r[8])<<4) & 63
		r11 := (int(r[8]) >> 2) & 63
		r12 := int(r[9]) & 63
		r13 := (int(r[9])>>6 | int(r[10])<<2) & 63
		r14 := (int(r[10])>>4 | int(r[11])<<4) & 63
		r15 := (int(r[11]) >> 2) & 63

		h0[r0]++
		h1[r1]++
		h2[r2]++
		h3[r3]++
		h0[r4]++
		h1[r5]++
		h2[r6]++
		h3[r7]++
		h0[r8]++
		h1[r9]++
		h2[r10]++
		h3[r11]++
		h0[r12]++
		h1[r13]++
		h2[r14]++
		h3[r15]++
	}

	for j := 0; j < 64; j++ {
		reghisto[j] = h0[j] + h1[j] + h2[j] + h3[j]
	}
}

func hllCount(data []byte) uint64 {
	if cached, ok := hllGetCachedCount(data); ok {
		return cached
	}

	registers := data[HLL_HDR_SIZE:]
	m := float64(HLL_REGISTERS)
	var reghisto [64]int

	hllDenseRegHisto(registers, reghisto[:])

	z := m * hllTau((m-float64(reghisto[HLL_Q+1]))/m)
	for j := HLL_Q; j >= 1; j-- {
		z += float64(reghisto[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(reghisto[0]) / m)
	count := uint64(math.Round(HLL_ALPHA_INF * m * m / z))

	hllSetCachedCount(data, count)
	return count
}

func hllMergeToRaw(raw []byte, sources ...[]byte) {
	for _, src := range sources {
		srcRegisters := src[HLL_HDR_SIZE:]
		for i := 0; i < HLL_REGISTERS; i++ {
			val := getRegister(srcRegisters, i)
			if val > raw[i] {
				raw[i] = val
			}
		}
	}
}

func hllRawToDense(destDense []byte, raw []byte) {
	destRegisters := destDense[HLL_HDR_SIZE:]
	for i := 0; i < HLL_REGISTERS; i++ {
		setRegister(destRegisters, i, raw[i])
	}
	hllInvalidateCache(destDense)
}

func pfadd(txn *badger.Txn, conn redcon.Conn, key []byte, elements ...[]byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	var hllData []byte
	item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
	if err == badger.ErrKeyNotFound {
		hllData = createHLL()
	} else if err != nil {
		return 0, err
	} else {
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return 0, err
		}
		hllData = valCopy
	}

	if !isValidHLL(hllData) {
		return 0, nil
	}

	registers := hllData[HLL_HDR_SIZE:]
	updated := 0
	for _, ele := range elements {
		if hllDenseAdd(registers, ele) {
			updated = 1
		}
	}

	if updated == 1 {
		hllInvalidateCache(hllData)
		entry := badger.NewEntry(rawKeyPrefixWithDb(key, dbSlot), hllData).WithMeta(byte(RedisString))
		if err := txn.SetEntry(entry); err != nil {
			return 0, err
		}
	}

	return updated, nil
}

func pfcount(txn *badger.Txn, conn redcon.Conn, keys ...[]byte) (uint64, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	if len(keys) == 0 {
		return 0, nil
	}

	if len(keys) == 1 {
		item, err := txn.Get(rawKeyPrefixWithDb(keys[0], dbSlot))
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return 0, err
		}
		if !isValidHLL(valCopy) {
			return 0, nil
		}
		return hllCount(valCopy), nil
	}

	raw := make([]byte, HLL_REGISTERS)
	for _, key := range keys {
		item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return 0, err
		}
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return 0, err
		}
		if !isValidHLL(valCopy) {
			continue
		}
		hllMergeToRaw(raw, valCopy)
	}

	dense := createHLL()
	hllRawToDense(dense, raw)
	return hllCount(dense), nil
}

func pfmerge(txn *badger.Txn, conn redcon.Conn, dest []byte, sources ...[]byte) error {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}

	raw := make([]byte, HLL_REGISTERS)
	for _, key := range sources {
		item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		if !isValidHLL(valCopy) {
			continue
		}
		hllMergeToRaw(raw, valCopy)
	}

	dense := createHLL()
	hllRawToDense(dense, raw)
	return txn.SetEntry(badger.NewEntry(rawKeyPrefixWithDb(dest, dbSlot), dense).WithMeta(byte(RedisString)))
}
