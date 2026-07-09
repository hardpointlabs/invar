package hll

import (
	"log"
	"testing"
	"testing/quick"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
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

func DBTestEnvironment(t *testing.T, fn func(session *common.Session, kvs kv.KeyValueStore)) {
	var session *common.Session
	session = common.NewSession()
	kvs := kv.InMemoryBadger(t)
	defer kvs.Close()

	log.Printf("CDB: %d\n", session.CurrentDB())

	fn(session, kvs)
}

// ---------------------------------------------------------------------------
// Property: pfadd returns 0 or 1; pfcount is reasonable
// ---------------------------------------------------------------------------

func TestHLLPFAddPFCountSanity(t *testing.T) {
	DBTestEnvironment(t, func(session *common.Session, kvs kv.KeyValueStore) {
		log.Printf("CDB: %d\n", session.CurrentDB())
		f := func(items []string) bool {
			if len(items) == 0 {
				return true
			}
			kvs := kv.InMemoryBadger(t)
			defer kvs.Close()

			err := kvs.Update(func(tx kv.Tx) error {
				for _, item := range items {
					op := Pfadd(nil, []byte("hll"), []byte(item)).DbOp
					r, err := op(tx)
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

			val, err := kvs.Read(func(tx kv.Tx) (any, error) {
				op := Pfcount(session, []byte("hll")).DbOp
				count, err := op(tx)
				if err != nil {
					return nil, err
				}
				return count, nil
			})

			if err != nil {
				return false
			}

			count := val.(uint64)
			return count <= uint64(len(items))*2+100
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}

	})
}

// ---------------------------------------------------------------------------
// Property: adding the same element twice returns 1 then 0
// ---------------------------------------------------------------------------

func TestHLLPFAddDuplicate(t *testing.T) {
	DBTestEnvironment(t, func(session *common.Session, kvs kv.KeyValueStore) {
		f := func(item string) bool {
			if len(item) == 0 {
				return true
			}

			var r1, r2 int
			err := kvs.Update(func(tx kv.Tx) error {
				var err error
				op1 := Pfadd(session, []byte("hll"), []byte(item)).DbOp
				val, err := op1(tx)
				if err != nil {
					return err
				}
				r1 = val.(int)

				op2 := Pfadd(session, []byte("hll"), []byte(item)).DbOp
				val, err = op2(tx)
				if err != nil {
					return err
				}
				r2 = val.(int)
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
	})
}

// ---------------------------------------------------------------------------
// Property: merging two HLLs yields a count >= each source count
// ---------------------------------------------------------------------------

func TestHLLMergeGreaterOrEqual(t *testing.T) {
	DBTestEnvironment(t, func(session *common.Session, kvs kv.KeyValueStore) {
		f := func(aItems, bItems []string) bool {
			if len(aItems) == 0 || len(bItems) == 0 {
				return true
			}

			kvs.Update(func(tx kv.Tx) error {
				for _, item := range aItems {
					Pfadd(session, []byte("hll_a"), []byte(item))
				}
				for _, item := range bItems {
					Pfadd(session, []byte("hll_b"), []byte(item))
				}
				return nil
			})

			var countA, countB uint64
			kvs.Read(func(tx kv.Tx) (any, error) {
				var err error
				opA := Pfcount(session, []byte("hll_a")).DbOp
				val, err := opA(tx)
				countA = val.(uint64)
				if err != nil {
					return nil, err
				}
				opB := Pfcount(session, []byte("hll_b")).DbOp
				val, err = opB(tx)
				countB = val.(uint64)
				return nil, err
			})

			kvs.Update(func(tx kv.Tx) error {
				op := Pfmerge(session, []byte("hll_merged"), []byte("hll_a"), []byte("hll_b")).DbOp
				_, err := op(tx)
				return err
			})

			var countMerged uint64
			kvs.Read(func(tx kv.Tx) (any, error) {
				op := Pfcount(session, []byte("hll_merged")).DbOp
				_, err := op(tx)
				return nil, err
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
	})
}
