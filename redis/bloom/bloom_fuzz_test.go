package bloom

import (
	"testing"
	"testing/quick"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/testutil"
)

// ---------------------------------------------------------------------------
// Fuzz: decodeBloomMeta must never panic on arbitrary input
// ---------------------------------------------------------------------------

func FuzzBloomDecodeMeta(f *testing.F) {
	seeds := [][]byte{
		{},
		{0, 0, 0, 0},
		{1, 2, 3, 4},
		make([]byte, 100),
		make([]byte, bloomMetaHeader+bloomFilterMeta),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		decodeBloomMeta(data)
	})
}

// ---------------------------------------------------------------------------
// Fuzz: subFilterSeeds must never panic and must return non-zero seeds
// ---------------------------------------------------------------------------

func FuzzBloomSubFilterSeeds(f *testing.F) {
	seeds := []uint64{0, 1, 1 << 63, 1<<64 - 1}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, filterID uint64) {
		s1, s2 := subFilterSeeds(filterID)
		if s1 == 0 || s2 == 0 {
			t.Errorf("zero seed returned for filterID=%d: s1=%d s2=%d", filterID, s1, s2)
		}
	})
}

// ---------------------------------------------------------------------------
// Property: bloom filters never have false negatives
// ---------------------------------------------------------------------------

func TestBloomAddExistsNoFalseNegative(t *testing.T) {
	f := func(item string) bool {
		if len(item) == 0 {
			return true
		}
		session, kvs := testutil.NewTestSession(t)

		err := kvs.Update(func(tx kv.Tx) error {
			_, err := Bfadd(session, []byte("bf"), []byte(item)).DbOp(tx)
			return err
		})
		if err != nil {
			return false
		}

		val, err := kvs.Read(func(tx kv.Tx) (any, error) {
			return Bfexists(session, []byte("bf"), []byte(item)).DbOp(tx)
		})
		if err != nil {
			return false
		}
		return val.(bool)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: adding the same item twice returns 1 (new) then 0 (already
// present)
// ---------------------------------------------------------------------------

func TestBloomAddDuplicateReturnsZero(t *testing.T) {
	f := func(item string) bool {
		if len(item) == 0 {
			return true
		}
		session, kvs := testutil.NewTestSession(t)

		var r1, r2 int
		err := kvs.Update(func(tx kv.Tx) error {
			val, err := Bfadd(session, []byte("bf"), []byte(item)).DbOp(tx)
			if err != nil {
				return err
			}
			r1 = val.(int)

			val, err = Bfadd(session, []byte("bf"), []byte(item)).DbOp(tx)
			if err != nil {
				return err
			}
			r2 = val.(int)
			return nil
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
// Property: bfmadd returns 0/1 per item; bfmexists finds every added item
// ---------------------------------------------------------------------------

func TestBloomMAddMExistsConsistency(t *testing.T) {
	f := func(items []string) bool {
		if len(items) == 0 {
			return true
		}
		session, kvs := testutil.NewTestSession(t)

		byteItems := make([][]byte, len(items))
		for i, item := range items {
			byteItems[i] = []byte(item)
		}

		var addResults []int
		err := kvs.Update(func(tx kv.Tx) error {
			val, err := Bfmadd(session, []byte("bf"), byteItems).DbOp(tx)
			if err != nil {
				return err
			}
			addResults = val.([]int)
			return nil
		})
		if err != nil {
			return false
		}

		for _, r := range addResults {
			if r != 0 && r != 1 {
				return false
			}
		}

		val, err := kvs.Read(func(tx kv.Tx) (any, error) {
			return Bfmexists(session, []byte("bf"), byteItems).DbOp(tx)
		})
		if err != nil {
			return false
		}
		existsResults := val.([]int)

		for _, r := range existsResults {
			if r != 1 {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
