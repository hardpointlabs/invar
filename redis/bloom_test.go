package redis

import (
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func inMemDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestBloomReserve(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("mybloom"), 0.01, 1000, 2, false)
	})
	if err != nil {
		t.Fatal(err)
	}

	db.View(func(txn *badger.Txn) error {
		meta, err := readBloomMeta(txn, []byte("mybloom"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(meta.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(meta.Filters))
		}
		if meta.Filters[0].Capacity != 1000 {
			t.Fatalf("expected capacity 1000, got %d", meta.Filters[0].Capacity)
		}
		if meta.Filters[0].Inserted != 0 {
			t.Fatalf("expected inserted 0, got %d", meta.Filters[0].Inserted)
		}
		if meta.Expansion != 2 {
			t.Fatalf("expected expansion 2, got %d", meta.Expansion)
		}
		if meta.NonScaling {
			t.Fatal("expected nonScaling false")
		}
		return nil
	})
}

func TestBloomReserveKeyExists(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("dupbloom"), 0.01, 100, 2, false)
	})

	err := db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("dupbloom"), 0.01, 100, 2, false)
	})
	if err == nil {
		t.Fatal("expected error for duplicate reserve")
	}
	if err.Error() != "key already exists" {
		t.Fatalf("expected 'key already exists', got %q", err.Error())
	}
}

func TestBloomAddExists(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		r, err := bfadd(txn, nil, []byte("bf"), []byte("hello"))
		if err != nil {
			return err
		}
		if r != 1 {
			t.Errorf("expected 1 (new), got %d", r)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		exists, err := bfexists(txn, nil, []byte("bf"), []byte("hello"))
		if err != nil {
			return err
		}
		if !exists {
			t.Error("expected hello to exist")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBloomAddDuplicate(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})

	db.Update(func(txn *badger.Txn) error {
		r, err := bfadd(txn, nil, []byte("bf"), []byte("hello"))
		if r != 1 || err != nil {
			t.Fatal("first add should succeed")
		}
		return nil
	})

	db.Update(func(txn *badger.Txn) error {
		r, err := bfadd(txn, nil, []byte("bf"), []byte("hello"))
		if err != nil {
			return err
		}
		if r != 0 {
			t.Errorf("expected 0 for duplicate, got %d", r)
		}
		return nil
	})
}

func TestBloomExistsNonexistent(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})

	db.View(func(txn *badger.Txn) error {
		exists, err := bfexists(txn, nil, []byte("bf"), []byte("nope"))
		if err != nil {
			return err
		}
		if exists {
			t.Error("expected false for non-existent element")
		}
		return nil
	})
}

func TestBloomExistsNonExistentKey(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.View(func(txn *badger.Txn) error {
		exists, err := bfexists(txn, nil, []byte("nokey"), []byte("x"))
		if err != nil {
			return err
		}
		if exists {
			t.Error("expected false for non-existent key")
		}
		return nil
	})
}

func TestBloomAddCreatesDefault(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		r, err := bfadd(txn, nil, []byte("auto"), []byte("item"))
		if err != nil {
			return err
		}
		if r != 1 {
			t.Errorf("expected 1, got %d", r)
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		meta, err := readBloomMeta(txn, []byte("auto"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(meta.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(meta.Filters))
		}
		// default capacity is 100
		if meta.Filters[0].Capacity != 100 {
			t.Fatalf("expected capacity 100 for auto-created, got %d", meta.Filters[0].Capacity)
		}
		return nil
	})
}

func TestBloomMAdd(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})

	db.Update(func(txn *badger.Txn) error {
		results, err := bfmadd(txn, nil, []byte("bf"), [][]byte{[]byte("a"), []byte("b"), []byte("c")})
		if err != nil {
			return err
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
		for i, r := range results {
			if r != 1 {
				t.Errorf("result[%d] expected 1, got %d", i, r)
			}
		}
		return nil
	})
}

func TestBloomMExists(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})
	db.Update(func(txn *badger.Txn) error {
		bfadd(txn, nil, []byte("bf"), []byte("a"))
		bfadd(txn, nil, []byte("bf"), []byte("b"))
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		results, err := bfmexists(txn, nil, []byte("bf"), [][]byte{[]byte("a"), []byte("x"), []byte("b")})
		if err != nil {
			return err
		}
		expected := []int{1, 0, 1}
		for i, r := range results {
			if r != expected[i] {
				t.Errorf("result[%d] expected %d, got %d", i, expected[i], r)
			}
		}
		return nil
	})
}

func TestBloomInfo(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 1000, 2, false)
	})
	db.Update(func(txn *badger.Txn) error {
		bfadd(txn, nil, []byte("bf"), []byte("a"))
		bfadd(txn, nil, []byte("bf"), []byte("b"))
		bfadd(txn, nil, []byte("bf"), []byte("c"))
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		info, err := bfinfo(txn, nil, []byte("bf"))
		if err != nil {
			t.Fatal(err)
		}
		if v := info["Number of items inserted"].(uint64); v != 3 {
			t.Errorf("expected 3 items inserted, got %d", v)
		}
		if v := info["Number of filters"].(int); v != 1 {
			t.Errorf("expected 1 filter, got %d", v)
		}
		if v := info["Capacity"].(uint64); v != 1000 {
			t.Errorf("expected capacity 1000, got %d", v)
		}
		if v := info["Expansion rate"].(int); v != 2 {
			t.Errorf("expected expansion 2, got %d", v)
		}
		return nil
	})
}

func TestBloomInfoNonExistent(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.View(func(txn *badger.Txn) error {
		_, err := bfinfo(txn, nil, []byte("nokey"))
		if err != badger.ErrKeyNotFound {
			t.Fatalf("expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
}

func TestBloomInsert(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		info := &bfInsertInfo{
			Capacity:  500,
			Error:     0.01,
			Expansion: 2,
			Items:     [][]byte{[]byte("x"), []byte("y")},
		}
		results, err := bfinsert(txn, nil, []byte("bf"), info)
		if err != nil {
			return err
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		for i, r := range results {
			if r != 1 {
				t.Errorf("result[%d] expected 1, got %d", i, r)
			}
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		meta, err := readBloomMeta(txn, []byte("bf"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if meta.Filters[0].Capacity != 500 {
			t.Fatalf("expected capacity 500, got %d", meta.Filters[0].Capacity)
		}
		return nil
	})
}

func TestBloomInsertNoCreate(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		info := &bfInsertInfo{
			NoCreate: true,
			Items:    [][]byte{[]byte("x")},
		}
		_, err := bfinsert(txn, nil, []byte("bf"), info)
		return err
	})
	if err != badger.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound with NOCREATE, got %v", err)
	}
}

func TestBloomScaling(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	// Tiny capacity to force scaling quickly
	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 5, 2, false)
	})

	// Insert enough items to trigger scaling
	db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 20; i++ {
			item := []byte{byte(i)}
			if _, err := bfadd(txn, nil, []byte("bf"), item); err != nil {
				return err
			}
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		meta, err := readBloomMeta(txn, []byte("bf"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(meta.Filters) < 2 {
			t.Fatalf("expected at least 2 filters after scaling, got %d", len(meta.Filters))
		}
		// Check that all items are still found
		items := [][]byte{{0}, {5}, {10}, {15}}
		for _, item := range items {
			exists, err := bfexists(txn, nil, []byte("bf"), item)
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("item %d disappeared after scaling", item[0])
			}
		}
		return nil
	})
}

func TestBloomNonScaling(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	// NONSCALING with small capacity
	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.01, 5, 2, true)
	})

	// Insert many items - should NOT create new filter
	db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 100; i++ {
			item := []byte{byte(i)}
			if _, err := bfadd(txn, nil, []byte("bf"), item); err != nil {
				return err
			}
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		meta, err := readBloomMeta(txn, []byte("bf"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(meta.Filters) != 1 {
			t.Fatalf("expected only 1 filter with NONSCALING, got %d", len(meta.Filters))
		}
		return nil
	})
}

func TestBloomComputeParams(t *testing.T) {
	tests := []struct {
		capacity   uint64
		errRate    float64
		minBits    uint64
		minHashes  uint32
		maxHashes  uint32
	}{
		{100, 0.01, 1, 1, 20},
		{1000, 0.01, 1, 1, 20},
		{1000000, 0.01, 1, 1, 64},
	}
	for _, tt := range tests {
		bits, hashes := computeBloomParams(tt.capacity, tt.errRate)
		if bits < tt.minBits {
			t.Errorf("capacity=%d rate=%f: bits=%d < minBits=%d", tt.capacity, tt.errRate, bits, tt.minBits)
		}
		if hashes < tt.minHashes || hashes > tt.maxHashes {
			t.Errorf("capacity=%d rate=%f: hashes=%d out of range [%d,%d]", tt.capacity, tt.errRate, hashes, tt.minHashes, tt.maxHashes)
		}
	}
}

func TestBloomSubFilterErrorRate(t *testing.T) {
	target := 0.01
	// Index 0: p = target * 0.5^1 = 0.005
	// Index 1: p = target * 0.5^2 = 0.0025
	// Index 2: p = target * 0.5^3 = 0.00125
	p0 := subFilterErrorRate(target, 0)
	if math.Abs(p0-0.005) > 1e-10 {
		t.Fatalf("expected p0=0.005, got %f", p0)
	}
	p1 := subFilterErrorRate(target, 1)
	if math.Abs(p1-0.0025) > 1e-10 {
		t.Fatalf("expected p1=0.0025, got %f", p1)
	}
	p2 := subFilterErrorRate(target, 2)
	if math.Abs(p2-0.00125) > 1e-10 {
		t.Fatalf("expected p2=0.00125, got %f", p2)
	}
}

func TestBloomSubFilterCapacity(t *testing.T) {
	base := uint64(1000)
	c0 := subFilterCapacity(base, 2, 0)
	if c0 != 1000 {
		t.Fatalf("expected cap0=1000, got %d", c0)
	}
	c1 := subFilterCapacity(base, 2, 1)
	if c1 != 2000 {
		t.Fatalf("expected cap1=2000, got %d", c1)
	}
	c2 := subFilterCapacity(base, 2, 2)
	if c2 != 4000 {
		t.Fatalf("expected cap2=4000, got %d", c2)
	}
}

func TestBloomHashDeterministic(t *testing.T) {
	h1 := bloomHash([]byte("hello"), 42)
	h2 := bloomHash([]byte("hello"), 42)
	if h1 != h2 {
		t.Fatal("hash should be deterministic")
	}
	h3 := bloomHash([]byte("world"), 42)
	if h1 == h3 {
		t.Fatal("different inputs should produce different hashes")
	}
}

func TestBloomHashPositions(t *testing.T) {
	positions := hashPositions([]byte("hello"), 10, 1000, 42, 99)
	if len(positions) != 10 {
		t.Fatalf("expected 10 positions, got %d", len(positions))
	}
	for i, pos := range positions {
		if pos >= 1000 {
			t.Errorf("position[%d]=%d out of range", i, pos)
		}
	}
}

func TestBloomMetaRoundTrip(t *testing.T) {
	m := &bloomMeta{
		Expansion:  3,
		NonScaling: true,
		Filters: []subFilterMeta{
			{ID: 0, Capacity: 100, Inserted: 50, ErrorRate: 0.01, NumHashes: 7, NumBits: 1000, Seed1: 123, Seed2: 456},
			{ID: 1, Capacity: 200, Inserted: 10, ErrorRate: 0.005, NumHashes: 8, NumBits: 2000, Seed1: 789, Seed2: 101112},
		},
	}
	data, err := encodeBloomMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := decodeBloomMeta(data)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Expansion != m.Expansion {
		t.Errorf("expansion: got %d, want %d", m2.Expansion, m.Expansion)
	}
	if m2.NonScaling != m.NonScaling {
		t.Errorf("nonScaling: got %v, want %v", m2.NonScaling, m.NonScaling)
	}
	if len(m2.Filters) != len(m.Filters) {
		t.Fatalf("filter count: got %d, want %d", len(m2.Filters), len(m.Filters))
	}
	for i := range m.Filters {
		if m2.Filters[i].ID != m.Filters[i].ID {
			t.Errorf("filter[%d].ID: got %d, want %d", i, m2.Filters[i].ID, m.Filters[i].ID)
		}
		if m2.Filters[i].Capacity != m.Filters[i].Capacity {
			t.Errorf("filter[%d].capacity: got %d, want %d", i, m2.Filters[i].Capacity, m.Filters[i].Capacity)
		}
		if m2.Filters[i].Inserted != m.Filters[i].Inserted {
			t.Errorf("filter[%d].inserted: got %d, want %d", i, m2.Filters[i].Inserted, m.Filters[i].Inserted)
		}
	}
}

func TestBloomPageKey(t *testing.T) {
	key := internalBloomPageKey([]byte("mybloom"), 0, 0, 0)
	expected := "-0:mybloom\x00bf:0:p:0"
	if string(key) != expected {
		t.Fatalf("expected %q, got %q", expected, string(key))
	}

	key2 := internalBloomPageKey([]byte("mybloom"), 3, 42, 0)
	expected2 := "-0:mybloom\x00bf:3:p:42"
	if string(key2) != expected2 {
		t.Fatalf("expected %q, got %q", expected2, string(key2))
	}
}

func TestBloomManyItems(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return bfreserve(txn, nil, []byte("bf"), 0.001, 5000, 2, false)
	})

	n := 1000
	for i := 0; i < n; i++ {
		item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
		db.Update(func(txn *badger.Txn) error {
			_, err := bfadd(txn, nil, []byte("bf"), item)
			return err
		})
	}

	falsePositives := 0
	db.View(func(txn *badger.Txn) error {
		for i := n; i < n*2; i++ {
			item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
			exists, err := bfexists(txn, nil, []byte("bf"), item)
			if err != nil {
				return err
			}
			if exists {
				falsePositives++
			}
		}
		return nil
	})

	fpRate := float64(falsePositives) / float64(n)
	if fpRate > 0.05 {
		t.Fatalf("false positive rate too high: %d/%d = %f", falsePositives, n, fpRate)
	}
	missed := 0
	db.View(func(txn *badger.Txn) error {
		for i := 0; i < n; i++ {
			item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
			exists, err := bfexists(txn, nil, []byte("bf"), item)
			if err != nil {
				return err
			}
			if !exists {
				missed++
			}
		}
		return nil
	})
	if missed > 0 {
		t.Fatalf("false negatives: %d / %d", missed, n)
	}
}
