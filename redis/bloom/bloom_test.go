package bloom

import (
	"math"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
)

func newTestSession(t *testing.T) (*common.Session, kv.KeyValueStore) {
	t.Helper()
	kvs := kv.InMemoryBadger(t)
	t.Cleanup(func() { kvs.Close() })
	session := common.NewSession(kvs)
	return session, kvs
}

func TestBloomReserve(t *testing.T) {
	session, kvs := newTestSession(t)

	err := kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("mybloom"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return bloomReadMeta(tx, session, []byte("mybloom"))
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := val.(*bloomMeta)
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
}

func TestBloomReserveKeyExists(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("dupbloom"), 0.01, 100, 2, false).DbOp(tx)
		return err
	})

	err := kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("dupbloom"), 0.01, 100, 2, false).DbOp(tx)
		return err
	})
	if err == nil {
		t.Fatal("expected error for duplicate reserve")
	}
	if err.Error() != "key already exists" {
		t.Fatalf("expected 'key already exists', got %q", err.Error())
	}
}

func TestBloomAddExists(t *testing.T) {
	session, kvs := newTestSession(t)

	err := kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = kvs.Update(func(tx kv.Tx) error {
		val, err := Bfadd(session, []byte("bf"), []byte("hello")).DbOp(tx)
		if err != nil {
			return err
		}
		r := val.(int)
		if r != 1 {
			t.Errorf("expected 1 (new), got %d", r)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfexists(session, []byte("bf"), []byte("hello")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !val.(bool) {
		t.Error("expected hello to exist")
	}
}

func TestBloomAddDuplicate(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})

	kvs.Update(func(tx kv.Tx) error {
		val, err := Bfadd(session, []byte("bf"), []byte("hello")).DbOp(tx)
		if err != nil {
			return err
		}
		r := val.(int)
		if r != 1 {
			t.Fatal("first add should succeed")
		}
		return nil
	})

	kvs.Update(func(tx kv.Tx) error {
		val, err := Bfadd(session, []byte("bf"), []byte("hello")).DbOp(tx)
		if err != nil {
			return err
		}
		r := val.(int)
		if r != 0 {
			t.Errorf("expected 0 for duplicate, got %d", r)
		}
		return nil
	})
}

func TestBloomExistsNonexistent(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfexists(session, []byte("bf"), []byte("nope")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if val.(bool) {
		t.Error("expected false for non-existent element")
	}
}

func TestBloomExistsNonExistentKey(t *testing.T) {
	session, kvs := newTestSession(t)

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfexists(session, []byte("nokey"), []byte("x")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if val.(bool) {
		t.Error("expected false for non-existent key")
	}
}

func TestBloomAddCreatesDefault(t *testing.T) {
	session, kvs := newTestSession(t)

	err := kvs.Update(func(tx kv.Tx) error {
		val, err := Bfadd(session, []byte("auto"), []byte("item")).DbOp(tx)
		if err != nil {
			return err
		}
		r := val.(int)
		if r != 1 {
			t.Errorf("expected 1, got %d", r)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return bloomReadMeta(tx, session, []byte("auto"))
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := val.(*bloomMeta)
	if len(meta.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(meta.Filters))
	}
	if meta.Filters[0].Capacity != 100 {
		t.Fatalf("expected capacity 100 for auto-created, got %d", meta.Filters[0].Capacity)
	}
}

func TestBloomMAdd(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})

	err := kvs.Update(func(tx kv.Tx) error {
		val, err := Bfmadd(session, []byte("bf"), [][]byte{[]byte("a"), []byte("b"), []byte("c")}).DbOp(tx)
		if err != nil {
			return err
		}
		results := val.([]int)
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
	if err != nil {
		t.Fatal(err)
	}
}

func TestBloomMExists(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})
	kvs.Update(func(tx kv.Tx) error {
		Bfadd(session, []byte("bf"), []byte("a")).DbOp(tx)
		Bfadd(session, []byte("bf"), []byte("b")).DbOp(tx)
		return nil
	})

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfmexists(session, []byte("bf"), [][]byte{[]byte("a"), []byte("x"), []byte("b")}).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	results := val.([]int)
	expected := []int{1, 0, 1}
	for i, r := range results {
		if r != expected[i] {
			t.Errorf("result[%d] expected %d, got %d", i, expected[i], r)
		}
	}
}

func TestBloomInfo(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 1000, 2, false).DbOp(tx)
		return err
	})
	kvs.Update(func(tx kv.Tx) error {
		Bfadd(session, []byte("bf"), []byte("a")).DbOp(tx)
		Bfadd(session, []byte("bf"), []byte("b")).DbOp(tx)
		Bfadd(session, []byte("bf"), []byte("c")).DbOp(tx)
		return nil
	})

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfinfo(session, []byte("bf")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	info := val.(map[string]interface{})
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
}

func TestBloomInfoNonExistent(t *testing.T) {
	session, kvs := newTestSession(t)

	_, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Bfinfo(session, []byte("nokey")).DbOp(tx)
	})
	if err != kv.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestBloomInsert(t *testing.T) {
	session, kvs := newTestSession(t)

	err := kvs.Update(func(tx kv.Tx) error {
		info := &InsertInfo{
			Capacity:  500,
			Error:     0.01,
			Expansion: 2,
			Items:     [][]byte{[]byte("x"), []byte("y")},
		}
		val, err := Bfinsert(session, []byte("bf"), info).DbOp(tx)
		if err != nil {
			return err
		}
		results := val.([]int)
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
	if err != nil {
		t.Fatal(err)
	}

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return bloomReadMeta(tx, session, []byte("bf"))
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := val.(*bloomMeta)
	if meta.Filters[0].Capacity != 500 {
		t.Fatalf("expected capacity 500, got %d", meta.Filters[0].Capacity)
	}
}

func TestBloomInsertNoCreate(t *testing.T) {
	session, kvs := newTestSession(t)

	err := kvs.Update(func(tx kv.Tx) error {
		info := &InsertInfo{
			NoCreate: true,
			Items:    [][]byte{[]byte("x")},
		}
		_, err := Bfinsert(session, []byte("bf"), info).DbOp(tx)
		return err
	})
	if err != kv.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound with NOCREATE, got %v", err)
	}
}

func TestBloomScaling(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 5, 2, false).DbOp(tx)
		return err
	})

	kvs.Update(func(tx kv.Tx) error {
		for i := 0; i < 20; i++ {
			item := []byte{byte(i)}
			if _, err := bloomAddOne(tx, session, []byte("bf"), item); err != nil {
				return err
			}
		}
		return nil
	})

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return bloomReadMeta(tx, session, []byte("bf"))
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := val.(*bloomMeta)
	if len(meta.Filters) < 2 {
		t.Fatalf("expected at least 2 filters after scaling, got %d", len(meta.Filters))
	}
	items := [][]byte{{0}, {5}, {10}, {15}}
	for _, item := range items {
		val, err := kvs.Read(func(tx kv.Tx) (any, error) {
			return bloomExistsOne(tx, session, []byte("bf"), item)
		})
		if err != nil {
			t.Fatal(err)
		}
		if !val.(bool) {
			t.Errorf("item %d disappeared after scaling", item[0])
		}
	}
}

func TestBloomNonScaling(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.01, 5, 2, true).DbOp(tx)
		return err
	})

	kvs.Update(func(tx kv.Tx) error {
		for i := 0; i < 100; i++ {
			item := []byte{byte(i)}
			if _, err := bloomAddOne(tx, session, []byte("bf"), item); err != nil {
				return err
			}
		}
		return nil
	})

	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return bloomReadMeta(tx, session, []byte("bf"))
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := val.(*bloomMeta)
	if len(meta.Filters) != 1 {
		t.Fatalf("expected only 1 filter with NONSCALING, got %d", len(meta.Filters))
	}
}

func TestBloomComputeParams(t *testing.T) {
	tests := []struct {
		capacity  uint64
		errRate   float64
		minBits   uint64
		minHashes uint32
		maxHashes uint32
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
	session, _ := newTestSession(t)

	suffix := bloomPageSuffix([]byte("mybloom"), 0, 0)
	key := session.PrivateKey(suffix)
	expected := []byte("-0:mybloom\x00bf:0:p:0")
	if string(key) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, string(key))
	}

	suffix2 := bloomPageSuffix([]byte("mybloom"), 3, 42)
	key2 := session.PrivateKey(suffix2)
	expected2 := []byte("-0:mybloom\x00bf:3:p:42")
	if string(key2) != string(expected2) {
		t.Fatalf("expected %q, got %q", expected2, string(key2))
	}
}

func TestBloomManyItems(t *testing.T) {
	session, kvs := newTestSession(t)

	kvs.Update(func(tx kv.Tx) error {
		_, err := Bfreserve(session, []byte("bf"), 0.001, 5000, 2, false).DbOp(tx)
		return err
	})

	n := 1000
	for i := 0; i < n; i++ {
		item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
		kvs.Update(func(tx kv.Tx) error {
			_, err := bloomAddOne(tx, session, []byte("bf"), item)
			return err
		})
	}

	falsePositives := 0
	val, err := kvs.Read(func(tx kv.Tx) (any, error) {
		fp := 0
		for i := n; i < n*2; i++ {
			item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
			exists, err := bloomExistsOne(tx, session, []byte("bf"), item)
			if err != nil {
				return nil, err
			}
			if exists {
				fp++
			}
		}
		return fp, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	falsePositives = val.(int)

	fpRate := float64(falsePositives) / float64(n)
	if fpRate > 0.05 {
		t.Fatalf("false positive rate too high: %d/%d = %f", falsePositives, n, fpRate)
	}

	missed := 0
	val, err = kvs.Read(func(tx kv.Tx) (any, error) {
		m := 0
		for i := 0; i < n; i++ {
			item := []byte{byte(i >> 8), byte(i), byte(i & 0xFF)}
			exists, err := bloomExistsOne(tx, session, []byte("bf"), item)
			if err != nil {
				return nil, err
			}
			if !exists {
				m++
			}
		}
		return m, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	missed = val.(int)

	if missed > 0 {
		t.Fatalf("false negatives: %d / %d", missed, n)
	}
}
