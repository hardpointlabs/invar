package redis

import (
	"testing"
)

func TestCreateHLL(t *testing.T) {
	h := createHLL()
	if len(h) != HLL_DENSE_SIZE {
		t.Fatalf("expected %d bytes, got %d", HLL_DENSE_SIZE, len(h))
	}
	if !isValidHLL(h) {
		t.Fatal("created HLL should be valid")
	}
}

func TestHLLCountEmpty(t *testing.T) {
	h := createHLL()
	c := hllCount(h)
	if c != 0 {
		t.Fatalf("expected 0 for empty HLL, got %d", c)
	}
}

func TestHLLAddSingle(t *testing.T) {
	h := createHLL()
	registers := h[HLL_HDR_SIZE:]
	ok := hllDenseAdd(registers, []byte("hello"))
	if !ok {
		t.Fatal("expected true on first add")
	}
	ok2 := hllDenseAdd(registers, []byte("hello"))
	if ok2 {
		t.Fatal("expected false on duplicate add")
	}
	hllInvalidateCache(h)
	c := hllCount(h)
	if c != 1 {
		t.Fatalf("expected count 1, got %d", c)
	}
}

func TestHLLAddMultiple(t *testing.T) {
	h := createHLL()
	registers := h[HLL_HDR_SIZE:]
	items := []string{"one", "two", "three", "four", "five"}
	for _, item := range items {
		hllDenseAdd(registers, []byte(item))
	}
	hllInvalidateCache(h)
	c := hllCount(h)
	if c < 3 || c > 7 {
		t.Fatalf("expected count ~5, got %d", c)
	}
}

func TestHLLAddDifferent(t *testing.T) {
	h := createHLL()
	registers := h[HLL_HDR_SIZE:]
	if !hllDenseAdd(registers, []byte("a")) {
		t.Fatal("expected true adding unique element")
	}
	if !hllDenseAdd(registers, []byte("b")) {
		t.Fatal("expected true adding unique element")
	}
	if hllDenseAdd(registers, []byte("a")) {
		t.Fatal("expected false adding duplicate")
	}
}

func TestHLLCache(t *testing.T) {
	h := createHLL()
	// Freshly created HLL has all zeros; cache is valid (MSB=0) with value 0
	cached, ok := hllGetCachedCount(h)
	if !ok {
		t.Fatal("cache should be valid after creation (all zeros = valid + count 0)")
	}
	if cached != 0 {
		t.Fatalf("cached count should be 0, got %d", cached)
	}
	// hllCount should return cached 0
	if hllCount(h) != 0 {
		t.Fatal("hllCount should return 0 for empty HLL")
	}

	// Invalidate to simulate modification
	hllInvalidateCache(h)
	if _, ok := hllGetCachedCount(h); ok {
		t.Fatal("cache should be invalid after invalidation")
	}

	// After recomputing, cache should be valid again
	cnt := hllCount(h)
	cached, ok = hllGetCachedCount(h)
	if !ok {
		t.Fatal("cache should be valid after counting")
	}
	if cached != cnt {
		t.Fatalf("cached count %d doesn't match computed count %d", cached, cnt)
	}
}

func TestHLLMerge(t *testing.T) {
	h1 := createHLL()
	h2 := createHLL()
	r1 := h1[HLL_HDR_SIZE:]
	r2 := h2[HLL_HDR_SIZE:]
	hllDenseAdd(r1, []byte("a"))
	hllDenseAdd(r1, []byte("b"))
	hllDenseAdd(r2, []byte("c"))
	hllDenseAdd(r2, []byte("d"))

	hllInvalidateCache(h1)
	hllInvalidateCache(h2)
	c1 := hllCount(h1)
	c2 := hllCount(h2)

	raw := make([]byte, HLL_REGISTERS)
	hllMergeToRaw(raw, h1, h2)

	dense := createHLL()
	hllRawToDense(dense, raw)
	cMerged := hllCount(dense)

	if cMerged < c1 || cMerged < c2 {
		t.Fatal("merged count should be >= individual counts")
	}
	if cMerged > uint64(c1+c2) {
		t.Fatal("merged count should not exceed sum of individual counts")
	}
}

func TestHLLMergeOverlap(t *testing.T) {
	h1 := createHLL()
	h2 := createHLL()
	r1 := h1[HLL_HDR_SIZE:]
	r2 := h2[HLL_HDR_SIZE:]
	hllDenseAdd(r1, []byte("a"))
	hllDenseAdd(r1, []byte("b"))
	hllDenseAdd(r2, []byte("b"))
	hllDenseAdd(r2, []byte("c"))

	raw := make([]byte, HLL_REGISTERS)
	hllMergeToRaw(raw, h1, h2)

	dense := createHLL()
	hllRawToDense(dense, raw)
	cMerged := hllCount(dense)

	separate := createHLL()
	sr := separate[HLL_HDR_SIZE:]
	hllDenseAdd(sr, []byte("a"))
	hllDenseAdd(sr, []byte("b"))
	hllDenseAdd(sr, []byte("c"))
	hllInvalidateCache(separate)
	cExpected := hllCount(separate)

	diff := int(cMerged) - int(cExpected)
	if diff < 0 {
		diff = -diff
	}
	if diff > 2 {
		t.Fatalf("merged count %d too far from direct count %d", cMerged, cExpected)
	}
}

func TestHLLMurmurHash64A(t *testing.T) {
	h := murmurHash64A([]byte(""), 0xadc83b19)
	if h == 0 {
		t.Fatal("hash should not be zero")
	}
	h2 := murmurHash64A([]byte("hello"), 0xadc83b19)
	if h2 == 0 {
		t.Fatal("hash should not be zero")
	}
	if h == h2 {
		t.Fatal("different inputs should produce different hashes")
	}
}

func TestHLLPatLen(t *testing.T) {
	index, count := hllPatLen([]byte("hello"))
	if index < 0 || index >= HLL_REGISTERS {
		t.Fatalf("index %d out of range", index)
	}
	if count < 1 || count > HLL_Q+1 {
		t.Fatalf("count %d out of range", count)
	}
}

func TestHLLDenseSize(t *testing.T) {
	if HLL_DENSE_SIZE != 12304 {
		t.Fatalf("HLL_DENSE_SIZE should be 12304, got %d", HLL_DENSE_SIZE)
	}
}

func TestHLLManyElements(t *testing.T) {
	h := createHLL()
	registers := h[HLL_HDR_SIZE:]
	n := 1000
	for i := 0; i < n; i++ {
		ele := []byte{byte(i >> 8), byte(i), 0}
		hllDenseAdd(registers, ele)
	}
	hllInvalidateCache(h)
	c := hllCount(h)
	ratio := float64(c) / float64(n)
	if ratio < 0.9 || ratio > 1.1 {
		t.Fatalf("expected ~%d, got %d (ratio %f)", n, c, ratio)
	}
}

func TestHLLCacheInvalidation(t *testing.T) {
	h := createHLL()
	registers := h[HLL_HDR_SIZE:]
	hllDenseAdd(registers, []byte("hello"))
	hllInvalidateCache(h) // mark stale

	if _, ok := hllGetCachedCount(h); ok {
		t.Fatal("cache should be invalid after caller manually invalidates")
	}

	c := hllCount(h)
	if c == 0 {
		t.Fatal("count should be recomputed, got 0")
	}

	// After recomputation, cache is valid again.
	if _, ok := hllGetCachedCount(h); !ok {
		t.Fatal("cache should be valid after recomputation")
	}
	if hllCount(h) != c {
		t.Fatal("subsequent calls should return cached value")
	}
}
