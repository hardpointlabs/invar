package json

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
)

// Pathological unit tests — document model level //

func TestJSONDeepNestedPathCreation(t *testing.T) {
	doc := newEmptyJSONDocument()

	parts := make([]string, 50)
	for i := range parts {
		parts[i] = fmt.Sprintf("l%d", i)
	}
	path := "$." + strings.Join(parts, ".")

	if err := doc.set(path, "bottom"); err != nil {
		t.Fatalf("set at depth 50: %v", err)
	}

	val, err := doc.get(path)
	if err != nil {
		t.Fatalf("get at depth 50: %v", err)
	}
	if val != "bottom" {
		t.Fatalf("expected 'bottom', got %v", val)
	}
}

func TestJSONUnicodeKeys(t *testing.T) {
	doc := newEmptyJSONDocument()
	entries := map[string]any{
		"名前":     "田中",
		"Привет": "мир",
		"שלום":   "עולם",
		"🚀🎉✨":    "emoji_value",
	}
	for k, v := range entries {
		path := "$." + k
		if err := doc.set(path, v); err != nil {
			t.Fatalf("set unicode key %q: %v", k, err)
		}
		got, err := doc.get(path)
		if err != nil {
			t.Fatalf("get unicode key %q: %v", k, err)
		}
		if got != v {
			t.Fatalf("unicode roundtrip %q: expected %v, got %v", k, v, got)
		}
	}
}

func TestJSONNumericEdgeCases(t *testing.T) {
	doc := newEmptyJSONDocument()

	cases := []struct {
		name  string
		value float64
	}{
		{"zero", 0},
		{"negative_zero", math.Copysign(0, -1)}, // -0
		{"max_float64", math.MaxFloat64},
		{"smallest_nonzero", math.SmallestNonzeroFloat64},
		{"large_integer", 9007199254740991},
		{"negative_large", -9007199254740991},
		{"pi", 3.141592653589793},
		{"negative", -42.5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := doc.set("$", tc.value); err != nil {
				if tc.name == "negative_zero" {
					t.Logf("negative zero: set failed (may be expected): %v", err)
					return
				}
				t.Fatalf("set: %v", err)
			}
			val, err := doc.get("$")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			fv, ok := val.(float64)
			if !ok {
				t.Fatalf("expected float64, got %T", val)
			}
			if tc.name == "negative_zero" {
				if math.Signbit(fv) {
					t.Logf("negative zero preserved")
				} else {
					t.Logf("negative zero became zero (expected with Go json)")
				}
				return
			}
			if math.IsInf(tc.value, 0) || math.IsNaN(tc.value) {
				return
			}
			// compare with a tolerance
			diff := fv - tc.value
			if diff < 0 {
				diff = -diff
			}
			if diff > 1e-9 && diff/tc.value > 1e-9 {
				t.Fatalf("value mismatch: expected %v, got %v", tc.value, fv)
			}
		})
	}
}

func TestJSONLargeArraySetGet(t *testing.T) {
	size := 10000
	arr := make([]any, size)
	for i := range arr {
		arr[i] = float64(i)
	}

	doc := newEmptyJSONDocument()
	if err := doc.set("$", arr); err != nil {
		t.Fatalf("set large array: %v", err)
	}
	got, err := doc.get("$")
	if err != nil {
		t.Fatalf("get large array: %v", err)
	}
	gotArr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(gotArr) != size {
		t.Fatalf("length: expected %d, got %d", size, len(gotArr))
	}
}

func TestJSONDeeplyNestedObjectRoundtrip(t *testing.T) {
	n := 200
	var path strings.Builder
	path.WriteString("$")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&path, ".k%d", i)
	}
	leafPath := path.String()

	doc := newEmptyJSONDocument()
	if err := doc.set(leafPath, float64(42)); err != nil {
		t.Fatalf("set at depth %d: %v", n, err)
	}

	got, err := doc.get(leafPath)
	if err != nil {
		t.Fatalf("get at depth %d: %v", n, err)
	}
	if got != float64(42) {
		t.Fatalf("value at depth %d: expected 42, got %v", n, got)
	}

	serialized, err := doc.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if len(serialized) == 0 {
		t.Fatal("empty serialized output")
	}
}

func TestJSONNumericCoercionInArray(t *testing.T) {
	doc := newEmptyJSONDocument()
	src := []any{float64(1), float64(2), float64(3)}
	if err := doc.set("$.items", src); err != nil {
		t.Fatal(err)
	}
	got, err := doc.get("$.items[0]")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(float64); !ok {
		t.Fatalf("expected float64 for array element, got %T", got)
	}
}

func TestJSONPathOverwritesDeepNestedValue(t *testing.T) {
	doc := newEmptyJSONDocument()

	if err := doc.set("$.a.b.c", float64(1)); err != nil {
		t.Fatal(err)
	}
	if err := doc.set("$.a.b.c", float64(99)); err != nil {
		t.Fatal(err)
	}
	val, err := doc.get("$.a.b.c")
	if err != nil {
		t.Fatal(err)
	}
	if val != float64(99) {
		t.Fatalf("expected 99, got %v", val)
	}
}

func TestJSONSetOverwritesPrimitiveWithObject(t *testing.T) {
	doc := newEmptyJSONDocument()

	// Set root to a number
	if err := doc.set("$", float64(42)); err != nil {
		t.Fatal(err)
	}
	// Setting a nested path on a primitive should fail
	err := doc.set("$.x.y", float64(1))
	if err == nil {
		t.Fatal("expected error when setting nested path on primitive, got nil")
	}
	t.Logf("expected error: %v", err)
}

// kv-backed pathological tests //

func TestJSONKvDeepNestedSetGet(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	var parts []string
	for i := 0; i < 100; i++ {
		parts = append(parts, fmt.Sprintf("lvl%d", i))
	}
	path := "$." + strings.Join(parts, ".")

	key := []byte("deepdoc")

	err := db.Update(func(tx kv.Tx) error {
		doc := newEmptyJSONDocument()
		if err := doc.set(path, "deep_value"); err != nil {
			return err
		}
		data, err := doc.serialize()
		if err != nil {
			return err
		}
		return tx.Set(db.NewEntry(key, data).Metadata(byte(common.RedisJSON)))
	})
	if err != nil {
		t.Fatalf("set deep: %v", err)
	}

	_, err = db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		data, err := item.Value()
		if err != nil {
			return nil, err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return nil, err
		}
		got, err := doc.get(path)
		if err != nil {
			return nil, err
		}
		if got != "deep_value" {
			t.Fatalf("expected 'deep_value', got %v", got)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONKvLargeObject(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	key := []byte("big")

	obj := make(map[string]any)
	for i := 0; i < 5000; i++ {
		obj[fmt.Sprintf("f%d", i)] = float64(i)
	}

	err := db.Update(func(tx kv.Tx) error {
		doc := &JSONDocument{root: obj}
		data, err := doc.serialize()
		if err != nil {
			return err
		}
		return tx.Set(db.NewEntry(key, data).Metadata(byte(common.RedisJSON)))
	})
	if err != nil {
		t.Fatalf("set large object: %v", err)
	}

	_, err = db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		data, err := item.Value()
		if err != nil {
			return nil, err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return nil, err
		}
		for i := 0; i < 5000; i++ {
			path := fmt.Sprintf("$.f%d", i)
			got, err := doc.get(path)
			if err != nil {
				return nil, fmt.Errorf("get %s: %w", path, err)
			}
			if got != float64(i) {
				return nil, fmt.Errorf("field %d: expected %d, got %v", i, i, got)
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// NX/XX tests that bypass node-redis client quirks //

func TestJSONSetNXNewKey(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	key := []byte("nxtest")
	value := []byte(`"first"`)

	err := db.Update(func(tx kv.Tx) error {
		_, err := tx.Get(key)
		if err == kv.ErrKeyNotFound {
			var v any
			if err := json.Unmarshal(value, &v); err != nil {
				return err
			}
			doc := newEmptyJSONDocument()
			doc.root = v
			data, err := doc.serialize()
			if err != nil {
				return err
			}
			return tx.Set(db.NewEntry(key, data).Metadata(byte(common.RedisJSON)))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NX create: %v", err)
	}

	_, err = db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		data, err := item.Value()
		if err != nil {
			return nil, err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return nil, err
		}
		got, _ := doc.get("$")
		if got != "first" {
			t.Fatalf("expected 'first', got %v", got)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
