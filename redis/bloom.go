package redis

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

const (
	bloomPageSize   = 4096
	bloomPageBits   = bloomPageSize * 8
	bloomDefCap     = 100
	bloomDefErr     = 0.01
	bloomDefExp     = 2
	bloomMetaHeader = 4
	bloomFilterMeta = 60
	bloomMaxHashes  = 64
)

type subFilterMeta struct {
	ID        uint64
	Capacity  uint64
	Inserted  uint64
	ErrorRate float64
	NumHashes uint32
	NumBits   uint64
	Seed1     uint64
	Seed2     uint64
}

type bloomMeta struct {
	Expansion  uint8
	NonScaling bool
	Filters    []subFilterMeta
}

func readBloomMeta(txn *badger.Txn, key []byte, dbSlot int) (*bloomMeta, error) {
	item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
	if err != nil {
		return nil, err
	}
	var val []byte
	err = item.Value(func(v []byte) error {
		val = append([]byte{}, v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return decodeBloomMeta(val)
}

func readBloomMetaOrNil(txn *badger.Txn, key []byte, dbSlot int) (*bloomMeta, error) {
	m, err := readBloomMeta(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

func writeBloomMeta(txn *badger.Txn, key []byte, m *bloomMeta, dbSlot int) error {
	data, err := encodeBloomMeta(m)
	if err != nil {
		return err
	}
	return txn.SetEntry(badger.NewEntry(rawKeyPrefixWithDb(key, dbSlot), data).WithMeta(byte(RedisBloom)))
}

func encodeBloomMeta(m *bloomMeta) ([]byte, error) {
	n := len(m.Filters)
	if n > 65535 {
		n = 65535
	}
	buf := make([]byte, bloomMetaHeader+n*bloomFilterMeta)
	flags := byte(0)
	if m.NonScaling {
		flags |= 1
	}
	buf[0] = flags
	buf[1] = m.Expansion
	binary.BigEndian.PutUint16(buf[2:4], uint16(n))
	for i, f := range m.Filters {
		off := bloomMetaHeader + i*bloomFilterMeta
		binary.LittleEndian.PutUint64(buf[off:], f.ID)
		binary.LittleEndian.PutUint64(buf[off+8:], f.Capacity)
		binary.LittleEndian.PutUint64(buf[off+16:], f.Inserted)
		binary.LittleEndian.PutUint64(buf[off+24:], math.Float64bits(f.ErrorRate))
		binary.LittleEndian.PutUint32(buf[off+32:], f.NumHashes)
		binary.LittleEndian.PutUint64(buf[off+36:], f.NumBits)
		binary.LittleEndian.PutUint64(buf[off+44:], f.Seed1)
		binary.LittleEndian.PutUint64(buf[off+52:], f.Seed2)
	}
	return buf, nil
}

func decodeBloomMeta(data []byte) (*bloomMeta, error) {
	if len(data) < bloomMetaHeader {
		return nil, badger.ErrKeyNotFound
	}
	m := &bloomMeta{}
	m.NonScaling = (data[0] & 1) != 0
	m.Expansion = data[1]
	n := int(binary.BigEndian.Uint16(data[2:4]))
	if len(data) < bloomMetaHeader+n*bloomFilterMeta {
		return nil, badger.ErrKeyNotFound
	}
	m.Filters = make([]subFilterMeta, n)
	for i := 0; i < n; i++ {
		off := bloomMetaHeader + i*bloomFilterMeta
		m.Filters[i].ID = binary.LittleEndian.Uint64(data[off:])
		m.Filters[i].Capacity = binary.LittleEndian.Uint64(data[off+8:])
		m.Filters[i].Inserted = binary.LittleEndian.Uint64(data[off+16:])
		m.Filters[i].ErrorRate = math.Float64frombits(binary.LittleEndian.Uint64(data[off+24:]))
		m.Filters[i].NumHashes = binary.LittleEndian.Uint32(data[off+32:])
		m.Filters[i].NumBits = binary.LittleEndian.Uint64(data[off+36:])
		m.Filters[i].Seed1 = binary.LittleEndian.Uint64(data[off+44:])
		m.Filters[i].Seed2 = binary.LittleEndian.Uint64(data[off+52:])
	}
	return m, nil
}

func internalBloomPageKey(name []byte, filterID, pageNum uint64, dbSlot int) []byte {
	s := internalPrefix + strconv.Itoa(dbSlot) + prefixSeparator + string(name) + "\x00bf:" +
		strconv.FormatUint(filterID, 10) + ":p:" + strconv.FormatUint(pageNum, 10)
	return []byte(s)
}

func bloomHash(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], seed)
	h.Write(sb[:])
	h.Write(data)
	return h.Sum64()
}

func computeBloomParams(capacity uint64, errorRate float64) (uint64, uint32) {
	if errorRate <= 0 {
		errorRate = bloomDefErr
	}
	if capacity < 1 {
		capacity = 1
	}
	ln2 := math.Ln2
	numBits := uint64(math.Ceil(-float64(capacity) * math.Log(errorRate) / (ln2 * ln2)))
	if numBits < 1 {
		numBits = 1
	}
	numHashes := uint32(math.Round(float64(numBits) / float64(capacity) * ln2))
	if numHashes < 1 {
		numHashes = 1
	}
	if numHashes > bloomMaxHashes {
		numHashes = bloomMaxHashes
	}
	return numBits, numHashes
}

func subFilterSeeds(filterID uint64) (uint64, uint64) {
	h := fnv.New64a()
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], filterID)
	binary.LittleEndian.PutUint64(buf[8:16], 0)
	h.Write(buf[:])
	seed1 := h.Sum64()

	h.Reset()
	binary.LittleEndian.PutUint64(buf[8:16], 1)
	h.Write(buf[:])
	seed2 := h.Sum64()
	return seed1, seed2
}

func subFilterErrorRate(targetRate float64, index int) float64 {
	return targetRate * math.Pow(0.5, float64(index+1))
}

func subFilterCapacity(baseCap uint64, expansion int, index int) uint64 {
	return baseCap * uint64(math.Pow(float64(expansion), float64(index)))
}

func newSubFilter(baseCapacity uint64, errorRate float64, expansion int, filterID uint64) subFilterMeta {
	idx := int(filterID)
	cap := subFilterCapacity(baseCapacity, expansion, idx)
	errRate := subFilterErrorRate(errorRate, idx)
	numBits, numHashes := computeBloomParams(cap, errRate)
	s1, s2 := subFilterSeeds(filterID)
	return subFilterMeta{
		ID:        filterID,
		Capacity:  cap,
		Inserted:  0,
		ErrorRate: errRate,
		NumHashes: numHashes,
		NumBits:   numBits,
		Seed1:     s1,
		Seed2:     s2,
	}
}

func newInitialSubFilter(capacity uint64, errorRate float64, expansion int, nonScaling bool) subFilterMeta {
	if nonScaling {
		numBits, numHashes := computeBloomParams(capacity, errorRate)
		s1, s2 := subFilterSeeds(0)
		return subFilterMeta{
			ID:        0,
			Capacity:  capacity,
			Inserted:  0,
			ErrorRate: errorRate,
			NumHashes: numHashes,
			NumBits:   numBits,
			Seed1:     s1,
			Seed2:     s2,
		}
	}
	return newSubFilter(capacity, errorRate, expansion, 0)
}

func testBit(page []byte, idx uint64) bool {
	return page[idx/8]&(1<<(idx%8)) != 0
}

func setBit(page []byte, idx uint64) {
	page[idx/8] |= 1 << (idx % 8)
}

func readPage(txn *badger.Txn, name []byte, filterID, pageNum uint64, dbSlot int) ([]byte, error) {
	key := internalBloomPageKey(name, filterID, pageNum, dbSlot)
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return make([]byte, bloomPageSize), nil
	}
	if err != nil {
		return nil, err
	}
	var val []byte
	err = item.Value(func(v []byte) error {
		val = append([]byte{}, v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func writePage(txn *badger.Txn, name []byte, filterID, pageNum uint64, data []byte, dbSlot int) error {
	key := internalBloomPageKey(name, filterID, pageNum, dbSlot)
	return txn.SetEntry(badger.NewEntry(key, data).WithMeta(byte(RedisBloom)))
}

func hashPositions(item []byte, numHashes uint32, numBits uint64, seed1, seed2 uint64) []uint64 {
	h1 := bloomHash(item, seed1)
	h2 := bloomHash(item, seed2)
	positions := make([]uint64, numHashes)
	for i := uint32(0); i < numHashes; i++ {
		positions[i] = (h1 + uint64(i)*h2) % numBits
	}
	return positions
}

var errKeyExists = &errStr{"key already exists"}

type errStr struct{ msg string }

func (e *errStr) Error() string { return e.msg }

func bfreserve(txn *badger.Txn, conn redcon.Conn, key []byte, errorRate float64, capacity uint64, expansion int, nonScaling bool) error {
	dbSlot := currentDb(conn)

	_, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
	if err == nil {
		return errKeyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	if expansion < 1 {
		expansion = bloomDefExp
	}
	filter := newInitialSubFilter(capacity, errorRate, expansion, nonScaling)
	meta := &bloomMeta{
		Expansion:  uint8(expansion),
		NonScaling: nonScaling,
		Filters:    []subFilterMeta{filter},
	}
	return writeBloomMeta(txn, key, meta, dbSlot)
}

func bfadd(txn *badger.Txn, conn redcon.Conn, key, item []byte) (int, error) {
	dbSlot := currentDb(conn)

	meta, err := readBloomMetaOrNil(txn, key, dbSlot)
	if err != nil {
		return 0, err
	}
	if meta == nil {
		filter := newInitialSubFilter(bloomDefCap, bloomDefErr, bloomDefExp, false)
		meta = &bloomMeta{
			Expansion:  bloomDefExp,
			NonScaling: false,
			Filters:    []subFilterMeta{filter},
		}
		if err := writeBloomMeta(txn, key, meta, dbSlot); err != nil {
			return 0, err
		}
	}

	filter := &meta.Filters[len(meta.Filters)-1]

	if filter.Inserted >= filter.Capacity && !meta.NonScaling {
		newID := filter.ID + 1
		newF := newSubFilter(filter.Capacity, filter.ErrorRate*2, int(meta.Expansion), newID)
		meta.Filters = append(meta.Filters, newF)
		filter = &meta.Filters[len(meta.Filters)-1]
	}

	positions := hashPositions(item, filter.NumHashes, filter.NumBits, filter.Seed1, filter.Seed2)

	type pageRef struct {
		num   uint64
		data  []byte
		dirty bool
	}
	pm := make(map[uint64]*pageRef)

	for _, pos := range positions {
		pageNum := pos / bloomPageBits
		if _, ok := pm[pageNum]; !ok {
			pdata, err := readPage(txn, key, filter.ID, pageNum, dbSlot)
			if err != nil {
				return 0, err
			}
			pm[pageNum] = &pageRef{num: pageNum, data: pdata}
		}
	}

	allSet := true
	for _, pos := range positions {
		pageNum := pos / bloomPageBits
		bitOff := pos % bloomPageBits
		if !testBit(pm[pageNum].data, bitOff) {
			allSet = false
			break
		}
	}

	if allSet {
		return 0, nil
	}

	for _, pos := range positions {
		pageNum := pos / bloomPageBits
		bitOff := pos % bloomPageBits
		pr := pm[pageNum]
		if !testBit(pr.data, bitOff) {
			setBit(pr.data, bitOff)
			pr.dirty = true
		}
	}

	for _, pr := range pm {
		if pr.dirty {
			if err := writePage(txn, key, filter.ID, pr.num, pr.data, dbSlot); err != nil {
				return 0, err
			}
		}
	}

	filter.Inserted++

	if err := writeBloomMeta(txn, key, meta, dbSlot); err != nil {
		return 0, err
	}

	return 1, nil
}

func bfexists(txn *badger.Txn, conn redcon.Conn, key, item []byte) (bool, error) {
	dbSlot := currentDb(conn)

	meta, err := readBloomMetaOrNil(txn, key, dbSlot)
	if err != nil {
		return false, err
	}
	if meta == nil {
		return false, nil
	}

	for i := len(meta.Filters) - 1; i >= 0; i-- {
		f := meta.Filters[i]
		positions := hashPositions(item, f.NumHashes, f.NumBits, f.Seed1, f.Seed2)
		found := true
		for _, pos := range positions {
			pageNum := pos / bloomPageBits
			pageData, err := readPage(txn, key, f.ID, pageNum, dbSlot)
			if err != nil {
				return false, err
			}
			if !testBit(pageData, pos%bloomPageBits) {
				found = false
				break
			}
		}
		if found {
			return true, nil
		}
	}

	return false, nil
}

func bfmadd(txn *badger.Txn, conn redcon.Conn, key []byte, items [][]byte) ([]int, error) {
	results := make([]int, len(items))
	for i, item := range items {
		r, err := bfadd(txn, conn, key, item)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

func bfmexists(txn *badger.Txn, conn redcon.Conn, key []byte, items [][]byte) ([]int, error) {
	results := make([]int, len(items))
	for i, item := range items {
		exists, err := bfexists(txn, conn, key, item)
		if err != nil {
			return nil, err
		}
		if exists {
			results[i] = 1
		} else {
			results[i] = 0
		}
	}
	return results, nil
}

type bfInsertInfo struct {
	Capacity   uint64
	Error      float64
	Expansion  int
	NoCreate   bool
	NonScaling bool
	Items      [][]byte
}

func bfinsert(txn *badger.Txn, conn redcon.Conn, key []byte, info *bfInsertInfo) ([]int, error) {
	dbSlot := currentDb(conn)

	meta, err := readBloomMetaOrNil(txn, key, dbSlot)
	if err != nil {
		return nil, err
	}

	if meta == nil {
		if info.NoCreate {
			return nil, badger.ErrKeyNotFound
		}
		cap := info.Capacity
		if cap == 0 {
			cap = bloomDefCap
		}
		errRate := info.Error
		if errRate <= 0 {
			errRate = bloomDefErr
		}
		exp := info.Expansion
		if exp < 1 {
			exp = bloomDefExp
		}
		filter := newInitialSubFilter(cap, errRate, exp, info.NonScaling)
		meta = &bloomMeta{
			Expansion:  uint8(exp),
			NonScaling: info.NonScaling,
			Filters:    []subFilterMeta{filter},
		}
		if err := writeBloomMeta(txn, key, meta, dbSlot); err != nil {
			return nil, err
		}
	}

	return bfmadd(txn, conn, key, info.Items)
}

func bfinfo(txn *badger.Txn, conn redcon.Conn, key []byte) (map[string]interface{}, error) {
	dbSlot := currentDb(conn)

	meta, err := readBloomMetaOrNil(txn, key, dbSlot)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, badger.ErrKeyNotFound
	}

	var totalInserted uint64
	var totalCapacity uint64
	var totalBits uint64

	for _, f := range meta.Filters {
		totalInserted += f.Inserted
		totalCapacity += f.Capacity
		totalBits += f.NumBits
	}

	info := map[string]interface{}{
		"Capacity":                 totalCapacity,
		"Size":                     totalBits,
		"Number of filters":        len(meta.Filters),
		"Number of items inserted": totalInserted,
		"Expansion rate":           int(meta.Expansion),
	}
	return info, nil
}
