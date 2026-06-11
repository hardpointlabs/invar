package redis

import (
	"testing"
	"testing/quick"

	"github.com/dgraph-io/badger/v4"
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
		db := inMemDB(t)
		defer db.Close()

		err := db.Update(func(txn *badger.Txn) error {
			_, err := bfadd(txn, 0, []byte("bf"), []byte(item))
			return err
		})
		if err != nil {
			return false
		}

		var exists bool
		db.View(func(txn *badger.Txn) error {
			var err error
			exists, err = bfexists(txn, 0, []byte("bf"), []byte(item))
			return err
		})
		return exists
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
		db := inMemDB(t)
		defer db.Close()

		var r1, r2 int
		err := db.Update(func(txn *badger.Txn) error {
			var err error
			r1, err = bfadd(txn, 0, []byte("bf"), []byte(item))
			if err != nil {
				return err
			}
			r2, err = bfadd(txn, 0, []byte("bf"), []byte(item))
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
// Property: bfmadd returns 0/1 per item; bfmexists finds every added item
// ---------------------------------------------------------------------------

func TestBloomMAddMExistsConsistency(t *testing.T) {
	f := func(items []string) bool {
		if len(items) == 0 {
			return true
		}
		db := inMemDB(t)
		defer db.Close()

		byteItems := make([][]byte, len(items))
		for i, item := range items {
			byteItems[i] = []byte(item)
		}

		var addResults []int
		err := db.Update(func(txn *badger.Txn) error {
			var err error
			addResults, err = bfmadd(txn, 0, []byte("bf"), byteItems)
			return err
		})
		if err != nil {
			return false
		}

		for _, r := range addResults {
			if r != 0 && r != 1 {
				return false
			}
		}

		var existsResults []int
		db.View(func(txn *badger.Txn) error {
			var err error
			existsResults, err = bfmexists(txn, 0, []byte("bf"), byteItems)
			return err
		})

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
