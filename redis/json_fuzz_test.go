package redis

import (
	"encoding/json"
	"testing"
	"testing/quick"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// validPath returns true when parsePath accepts the string without error.
func validPath(s string) bool {
	_, err := parsePath(s)
	return err == nil
}

// docFromObject returns a JSONDocument whose root is a map with the given
// key set to value.  It is a convenience wrapper used across several tests.
func docFromObject(key string, value any) *JSONDocument {
	return &JSONDocument{root: map[string]any{key: value}}
}

// ---------------------------------------------------------------------------
// Fuzz: parsePath must never panic on arbitrary input
// ---------------------------------------------------------------------------

func FuzzParsePath(f *testing.F) {
	// Seed corpus: paths that exercise every branch of the parser.
	seeds := []string{
		"$",
		"$.name",
		"$.a.b.c",
		"$.items[0]",
		"$.items[-1]",
		"$.items[*]",
		"$..name",
		"$..*.id",
		"$.store.book[0].title",
		".name",
		"[0].name",
		"$.a[\"b.c\"]",
		// invalid — must not panic
		"",
		"name",
		"$.items[]",
		"$.items[abc]",
		"$.[",
		"$.items[\"unclosed",
		"$[*",
		"$.",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, path string) {
		// Must not panic; error is perfectly acceptable.
		parts, err := parsePath(path)
		if err != nil {
			return
		}
		// If parsing succeeded every part must have a recognised type.
		for _, p := range parts {
			switch p.typ {
			case partRoot, partKey, partIndex, partWildcard, partRecursive:
				// ok
			default:
				t.Errorf("parsePath(%q) returned unknown part type %d", path, p.typ)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Fuzz: serialize → newJSONDocument round-trip
//
// Property: any value produced by json.Marshal can be stored and reloaded
// without loss.  We construct a small object around the fuzzed bytes so that
// we always have a well-formed JSON root document to work with.
// ---------------------------------------------------------------------------

func FuzzJSONSerializeRoundTrip(f *testing.F) {
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`{"x":"hello","y":[1,2,3]}`))
	f.Add([]byte(`42`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(`true`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		// Only run with bytes that are valid JSON — invalid JSON is tested
		// elsewhere; here we care about the serialize round-trip.
		var parsed any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return
		}

		doc := &JSONDocument{root: parsed}
		serialized, err := doc.serialize()
		if err != nil {
			t.Fatalf("serialize failed on valid JSON input: %v", err)
		}

		doc2, err := newJSONDocument(serialized)
		if err != nil {
			t.Fatalf("newJSONDocument failed on serialize output: %v", err)
		}

		// Structural equality: re-serialize doc2 and compare JSON bytes.
		serialized2, err := doc2.serialize()
		if err != nil {
			t.Fatalf("second serialize failed: %v", err)
		}

		var v1, v2 any
		json.Unmarshal(serialized, &v1)
		json.Unmarshal(serialized2, &v2)
		b1, _ := json.Marshal(v1)
		b2, _ := json.Marshal(v2)
		if string(b1) != string(b2) {
			t.Errorf("round-trip mismatch:\n  first:  %s\n  second: %s", b1, b2)
		}
	})
}

// ---------------------------------------------------------------------------
// Property: get → set → get identity
//
// For any valid path that resolves successfully, writing a new value then
// reading it back must return exactly what was written.
// ---------------------------------------------------------------------------

func TestJSONGetSetGetIdentity(t *testing.T) {
	type sample struct {
		key   string
		value any
	}

	// Fixed cases covering all JSON scalar and composite types.
	cases := []sample{
		{"str", "hello"},
		{"num", 42.0},
		{"neg", -7.5},
		{"zero", 0.0},
		{"bool_t", true},
		{"bool_f", false},
		{"null_val", nil},
		{"arr", []any{1.0, 2.0, 3.0}},
		{"obj", map[string]any{"nested": "value"}},
	}

	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			doc := docFromObject(c.key, "original")

			path := "$." + c.key
			if err := doc.set(path, c.value); err != nil {
				t.Fatalf("set(%q) failed: %v", path, err)
			}

			got, err := doc.get(path)
			if err != nil {
				t.Fatalf("get(%q) after set failed: %v", path, err)
			}

			// Compare via JSON marshalling to handle nil and float precision.
			wantJSON, _ := json.Marshal(c.value)
			gotJSON, _ := json.Marshal(got)
			if string(wantJSON) != string(gotJSON) {
				t.Errorf("get after set: want %s, got %s", wantJSON, gotJSON)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Property-based: get → set → get identity over random string values
// ---------------------------------------------------------------------------

func TestJSONGetSetGetIdentityQuick(t *testing.T) {
	f := func(key string, value string) bool {
		if key == "" {
			return true // parsePath rejects empty key segments
		}
		// Exclude characters that would break the $.key path syntax.
		for _, ch := range key {
			if ch == '.' || ch == '[' || ch == ']' || ch == '$' || ch == '*' || ch == 0 {
				return true
			}
		}

		doc := docFromObject(key, "original")
		path := "$." + key

		if err := doc.set(path, value); err != nil {
			return true // path might not be settable; not a failure
		}
		got, err := doc.get(path)
		if err != nil {
			return false // should be readable after a successful set
		}
		s, ok := got.(string)
		if !ok {
			return false
		}
		return s == value
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: delete removes exactly the target path, nothing else
// ---------------------------------------------------------------------------

func TestJSONDeleteIsolation(t *testing.T) {
	f := func(deleteKey string, keepKey string) bool {
		if deleteKey == "" || keepKey == "" || deleteKey == keepKey {
			return true
		}
		// Exclude path-syntax characters.
		bad := func(s string) bool {
			for _, ch := range s {
				if ch == '.' || ch == '[' || ch == ']' || ch == '$' || ch == '*' || ch == 0 {
					return true
				}
			}
			return false
		}
		if bad(deleteKey) || bad(keepKey) {
			return true
		}

		doc := &JSONDocument{root: map[string]any{
			deleteKey: "gone",
			keepKey:   "stays",
		}}

		if err := doc.delete("$." + deleteKey); err != nil {
			return true // delete failed cleanly; path may not be navigable
		}

		// Deleted key must not exist.
		if _, err := doc.get("$." + deleteKey); err == nil {
			return false
		}

		// Sibling key must be untouched.
		got, err := doc.get("$." + keepKey)
		if err != nil {
			return false
		}
		s, ok := got.(string)
		return ok && s == "stays"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: arrAppend increases length by exactly the number of appended items
// ---------------------------------------------------------------------------

func TestJSONArrAppendLengthInvariant(t *testing.T) {
	f := func(initial []float64, extra []float64) bool {
		if initial == nil {
			initial = []float64{}
		}
		if extra == nil {
			extra = []float64{}
		}

		// Build initial document.
		arr := make([]any, len(initial))
		for i, v := range initial {
			arr[i] = v
		}
		doc := &JSONDocument{root: map[string]any{"arr": arr}}

		beforeLen, err := doc.arrLen("$.arr")
		if err != nil {
			return false
		}

		if len(extra) == 0 {
			return beforeLen == len(initial)
		}

		values := make([]any, len(extra))
		for i, v := range extra {
			values[i] = v
		}
		newLen, err := doc.arrAppend("$.arr", values...)
		if err != nil {
			return false
		}

		afterLen, err := doc.arrLen("$.arr")
		if err != nil {
			return false
		}

		return newLen == len(initial)+len(extra) &&
			afterLen == newLen
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: strAppend increases byte-length by exactly len(suffix)
// ---------------------------------------------------------------------------

func TestJSONStrAppendLengthInvariant(t *testing.T) {
	f := func(base string, suffix string) bool {
		doc := docFromObject("s", base)

		beforeLen, err := doc.strLen("$.s")
		if err != nil {
			return false
		}

		newLen, err := doc.strAppend("$.s", suffix)
		if err != nil {
			return false
		}

		return newLen == beforeLen+len(suffix)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: numIncrBy is commutative — (start+a)+b == (start+b)+a
//
// Bounded to [-1e9, 1e9] to stay well within the region where IEEE-754
// double addition is commutative in terms of exact bit representation.
// Commutativity does not hold in general for float64 due to rounding, so we
// restrict to integers in a safe range where a+b == b+a is exact.
// ---------------------------------------------------------------------------

func TestJSONNumIncrByCommutativity(t *testing.T) {
	f := func(startRaw, aRaw, bRaw int32) bool {
		start := float64(startRaw)
		a := float64(aRaw)
		b := float64(bRaw)

		doc1 := docFromObject("n", start)
		doc2 := docFromObject("n", start)

		if _, err := doc1.numIncrBy("$.n", a); err != nil {
			return false
		}
		if _, err := doc1.numIncrBy("$.n", b); err != nil {
			return false
		}

		if _, err := doc2.numIncrBy("$.n", b); err != nil {
			return false
		}
		if _, err := doc2.numIncrBy("$.n", a); err != nil {
			return false
		}

		got1, _ := doc1.get("$.n")
		got2, _ := doc2.get("$.n")

		n1, ok1 := got1.(float64)
		n2, ok2 := got2.(float64)
		if !ok1 || !ok2 {
			return false
		}
		return n1 == n2
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: numIncrBy(delta) then numIncrBy(-delta) returns to start
//
// Restricted to int32-valued deltas so that the add/subtract pair is exact
// in float64 (integers up to 2^53 round-trip perfectly).
// ---------------------------------------------------------------------------

func TestJSONNumIncrByInverse(t *testing.T) {
	f := func(startRaw, deltaRaw int32) bool {
		start := float64(startRaw)
		delta := float64(deltaRaw)

		doc := docFromObject("n", start)

		if _, err := doc.numIncrBy("$.n", delta); err != nil {
			return false
		}
		if _, err := doc.numIncrBy("$.n", -delta); err != nil {
			return false
		}

		got, err := doc.get("$.n")
		if err != nil {
			return false
		}
		n, ok := got.(float64)
		if !ok {
			return false
		}
		return n == start
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Property: typeOf returns a stable, non-empty string for every JSON value
// ---------------------------------------------------------------------------

func TestJSONTypeOfStability(t *testing.T) {
	type tc struct {
		name  string
		value any
		want  string
	}
	cases := []tc{
		{"null", nil, "null"},
		{"bool_true", true, "boolean"},
		{"bool_false", false, "boolean"},
		{"float", 42.0, "number"},
		{"neg_float", -3.14, "number"},
		{"zero", 0.0, "number"},
		{"string", "hello", "string"},
		{"empty_string", "", "string"},
		{"array", []any{1.0, 2.0}, "array"},
		{"empty_array", []any{}, "array"},
		{"object", map[string]any{"k": "v"}, "object"},
		{"empty_object", map[string]any{}, "object"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doc := &JSONDocument{root: c.value}
			got, err := doc.typeOf("$")
			if err != nil {
				t.Fatalf("typeOf($) unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("typeOf: got %q, want %q", got, c.want)
			}
			// Calling typeOf twice must return the same result (no mutation).
			got2, _ := doc.typeOf("$")
			if got2 != got {
				t.Errorf("typeOf not stable: first %q, second %q", got, got2)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fuzz: newJSONDocument must never panic on arbitrary bytes
// ---------------------------------------------------------------------------

func FuzzNewJSONDocument(f *testing.F) {
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte{})
	f.Add([]byte(`{bad json`))
	f.Add([]byte("\x00\x01\x02\xff"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		// Must not panic; invalid JSON should return an error.
		doc, err := newJSONDocument(raw)
		if err != nil {
			return
		}
		// If construction succeeded, serialize must not panic.
		if _, err := doc.serialize(); err != nil {
			t.Errorf("serialize failed on document built from valid JSON: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Fuzz: get on arbitrary paths against a fixed document must not panic
// ---------------------------------------------------------------------------

func FuzzJSONGet(f *testing.F) {
	f.Add("$.name")
	f.Add("$.items[0]")
	f.Add("$..name")
	f.Add("$.x.y.z")
	f.Add("")
	f.Add("$")
	f.Add("$.items[-1]")

	doc := &JSONDocument{root: map[string]any{
		"name":  "Alice",
		"age":   30.0,
		"items": []any{"a", "b", "c"},
		"nested": map[string]any{
			"deep": "value",
		},
	}}

	f.Fuzz(func(t *testing.T, path string) {
		// Must not panic; errors are fine.
		_ , _ = doc.get(path)
	})
}

// ---------------------------------------------------------------------------
// Fuzz: set on arbitrary paths against a fixed document must not panic
// ---------------------------------------------------------------------------

func FuzzJSONSet(f *testing.F) {
	f.Add("$.name", `"Bob"`)
	f.Add("$.items[0]", `99`)
	f.Add("$.new.key", `true`)
	f.Add("$", `{"replaced":true}`)
	f.Add("$.items[-1]", `"last"`)

	f.Fuzz(func(t *testing.T, path string, valueJSON string) {
		// Fresh document each time so mutations don't bleed across iterations.
		doc := &JSONDocument{root: map[string]any{
			"name":  "Alice",
			"items": []any{1.0, 2.0, 3.0},
		}}

		var value any
		if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
			return // skip invalid JSON values
		}

		// Must not panic.
		_ = doc.set(path, value)
	})
}

// ---------------------------------------------------------------------------
// Fuzz: arrAppend on arbitrary path + values must not panic
// ---------------------------------------------------------------------------

func FuzzJSONArrAppend(f *testing.F) {
	f.Add("$.tags", `"hello"`)
	f.Add("$.tags", `42`)
	f.Add("$.tags", `null`)
	f.Add("$.tags", `[1,2]`)

	f.Fuzz(func(t *testing.T, path string, valueJSON string) {
		doc := &JSONDocument{root: map[string]any{
			"tags":   []any{"existing"},
			"num":    42.0,
			"nested": map[string]any{"arr": []any{1.0}},
		}}

		var value any
		if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
			return
		}

		// Must not panic.
		_, _ = doc.arrAppend(path, value)
	})
}
