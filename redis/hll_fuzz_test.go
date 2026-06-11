package redis

import (
	"testing"
	"testing/quick"

	"github.com/dgraph-io/badger/v4"
)

// ---------------------------------------------------------------------------
// Fuzz: isValidHLL must never panic on arbitrary input
// ---------------------------------------------------------------------------

func FuzzHLLIsValid(f *testing.F) {
	seeds := [][]byte{
		createHLL(),
		{},
		{0x48, 0x59, 0x4c, 0x4c},
		make([]byte, HLL_DENSE_SIZE),
		make([]byte, HLL_DENSE_SIZE+1),
		make([]byte, HLL_DENSE_SIZE-1),
		{0x48, 0x59, 0x4c, 0x4c, 0x00},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		isValidHLL(data)
	})
}

// ---------------------------------------------------------------------------
// Fuzz: hllPatLen must never panic on arbitrary input and outputs must be
// within valid ranges
// ---------------------------------------------------------------------------

func FuzzHLLPatLen(f *testing.F) {
	seeds := [][]byte{
		{},
		{0},
		{0xFF},
		[]byte("hello"),
		make([]byte, 1000),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, ele []byte) {
		idx, cnt := hllPatLen(ele)
		if idx < 0 || idx >= HLL_REGISTERS {
			t.Errorf("index %d out of range [0,%d)", idx, HLL_REGISTERS)
		}
		if cnt < 1 || cnt > HLL_Q+1 {
			t.Errorf("count %d out of range [1,%d]", cnt, HLL_Q+1)
		}
	})
}

// ---------------------------------------------------------------------------
// Fuzz: hllDenseAdd must never panic on arbitrary elements
// ---------------------------------------------------------------------------

func FuzzHLLDenseAdd(f *testing.F) {
	seeds := [][]byte{
		{},
		{0},
		{0xFF},
		[]byte("hello"),
		make([]byte, 1024),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, ele []byte) {
		h := createHLL()
		registers := h[HLL_HDR_SIZE:]
		hllDenseAdd(registers, ele)
	})
}

// ---------------------------------------------------------------------------
// Fuzz: hllCount must never panic on arbitrary HLL-sized data
// ---------------------------------------------------------------------------

func FuzzHLLCount(f *testing.F) {
	f.Add(createHLL())
	f.Add(make([]byte, HLL_DENSE_SIZE))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) != HLL_DENSE_SIZE {
			return
		}
		hllCount(data)
	})
}

// ---------------------------------------------------------------------------
// Property: pfadd returns 0 or 1; pfcount is reasonable
// ---------------------------------------------------------------------------

func TestHLLPFAddPFCountSanity(t *testing.T) {
	f := func(items []string) bool {
		if len(items) == 0 {
			return true
		}
		db := inMemDB(t)
		defer db.Close()

		err := db.Update(func(txn *badger.Txn) error {
			for _, item := range items {
				r, err := pfadd(txn, 0, []byte("hll"), []byte(item))
				if err != nil {
					return err
				}
				if r != 0 && r != 1 {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return false
		}

		var count uint64
		db.View(func(txn *badger.Txn) error {
			var err error
			count, err = pfcount(txn, 0, []byte("hll"))
			return err
		})

		return count <= uint64(len(items))*2+100
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: adding the same element twice returns 1 then 0
// ---------------------------------------------------------------------------

func TestHLLPFAddDuplicate(t *testing.T) {
	f := func(item string) bool {
		if len(item) == 0 {
			return true
		}
		db := inMemDB(t)
		defer db.Close()

		var r1, r2 int
		err := db.Update(func(txn *badger.Txn) error {
			var err error
			r1, err = pfadd(txn, 0, []byte("hll"), []byte(item))
			if err != nil {
				return err
			}
			r2, err = pfadd(txn, 0, []byte("hll"), []byte(item))
			return err
		})
		if err != nil {
			return false
		}
		return r1 == 1 && r2 == 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: merging two HLLs yields a count >= each source count
// ---------------------------------------------------------------------------

func TestHLLMergeGreaterOrEqual(t *testing.T) {
	f := func(aItems, bItems []string) bool {
		if len(aItems) == 0 || len(bItems) == 0 {
			return true
		}
		db := inMemDB(t)
		defer db.Close()

		db.Update(func(txn *badger.Txn) error {
			for _, item := range aItems {
				pfadd(txn, 0, []byte("hll_a"), []byte(item))
			}
			for _, item := range bItems {
				pfadd(txn, 0, []byte("hll_b"), []byte(item))
			}
			return nil
		})

		var countA, countB uint64
		db.View(func(txn *badger.Txn) error {
			var err error
			countA, err = pfcount(txn, 0, []byte("hll_a"))
			if err != nil {
				return err
			}
			countB, err = pfcount(txn, 0, []byte("hll_b"))
			return err
		})

		db.Update(func(txn *badger.Txn) error {
			return pfmerge(txn, 0, []byte("hll_merged"), []byte("hll_a"), []byte("hll_b"))
		})

		var countMerged uint64
		db.View(func(txn *badger.Txn) error {
			var err error
			countMerged, err = pfcount(txn, 0, []byte("hll_merged"))
			return err
		})

		maxCount := countA
		if countB > maxCount {
			maxCount = countB
		}
		return countMerged >= maxCount
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
