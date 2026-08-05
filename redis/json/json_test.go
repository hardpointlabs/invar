package json

import (
	"encoding/json"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
)

func TestJSONPathParsing(t *testing.T) {
	tests := []struct {
		path    string
		wantErr bool
		desc    string
	}{
		{"$", false, "root"},
		{"$.name", false, "simple key"},
		{"$.user.name", false, "nested keys"},
		{"$.items[0]", false, "array index"},
		{"$.items[-1]", false, "negative index"},
		{"$.items[*]", false, "wildcard"},
		{"$..name", false, "recursive descent"},
		{"$.store.book[0].title", false, "complex path"},
		{".name", false, "legacy dot path"},
		{"[0].name", false, "legacy bracket path"},
		{"", true, "empty path"},
		{"name", true, "no leading $ or ."},
		{"$.items[]", true, "empty brackets"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := parsePath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePath(%q) error = %v, wantErr = %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestJSONDocumentNewEmpty(t *testing.T) {
	doc := newEmptyJSONDocument()
	if doc.root == nil {
		t.Fatal("expected non-nil root")
	}
	data, err := doc.serialize()
	if err != nil {
		t.Fatal(err)
	}
	var result any
	json.Unmarshal(data, &result)
	m, ok := result.(map[string]any)
	if !ok || len(m) != 0 {
		t.Fatalf("expected empty object, got %v", result)
	}
}

func TestJSONDocumentNewFromBytes(t *testing.T) {
	raw := []byte(`{"name": "Alice", "age": 30}`)
	doc, err := newJSONDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	name, err := doc.get("$.name")
	if err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected 'Alice', got %v", name)
	}
	age, err := doc.get("$.age")
	if err != nil {
		t.Fatal(err)
	}
	if age != 30.0 {
		t.Fatalf("expected 30, got %v", age)
	}
}

func TestJSONDocumentGetRoot(t *testing.T) {
	doc := newEmptyJSONDocument()
	doc.root = "hello"
	val, err := doc.get("$")
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %v", val)
	}
}

func TestJSONDocumentGetNested(t *testing.T) {
	raw := []byte(`{"a": {"b": {"c": 42}}}`)
	doc, err := newJSONDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	val, err := doc.get("$.a.b.c")
	if err != nil {
		t.Fatal(err)
	}
	if val != 42.0 {
		t.Fatalf("expected 42, got %v", val)
	}
}

func TestJSONDocumentGetArray(t *testing.T) {
	raw := []byte(`{"items": [10, 20, 30]}`)
	doc, err := newJSONDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	val, err := doc.get("$.items[1]")
	if err != nil {
		t.Fatal(err)
	}
	if val != 20.0 {
		t.Fatalf("expected 20, got %v", val)
	}

	val, err = doc.get("$.items[-1]")
	if err != nil {
		t.Fatal(err)
	}
	if val != 30.0 {
		t.Fatalf("expected 30, got %v", val)
	}
}

func TestJSONDocumentGetMissingPath(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	doc, err := newJSONDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = doc.get("$.b")
	if err == nil {
		t.Fatal("expected error for missing path")
	}
}

func TestJSONDocumentSetRoot(t *testing.T) {
	doc := newEmptyJSONDocument()
	err := doc.set("$", map[string]any{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := doc.root.(map[string]any)
	if !ok {
		t.Fatal("expected map root")
	}
	if m["key"] != "value" {
		t.Fatalf("expected 'value', got %v", m["key"])
	}
}

func TestJSONDocumentSetNested(t *testing.T) {
	doc := newEmptyJSONDocument()
	err := doc.set("$.user.name", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	err = doc.set("$.user.age", float64(25))
	if err != nil {
		t.Fatal(err)
	}
	name, _ := doc.get("$.user.name")
	if name != "Bob" {
		t.Fatalf("expected 'Bob', got %v", name)
	}
	age, _ := doc.get("$.user.age")
	if age != float64(25) {
		t.Fatalf("expected 25, got %v (%T)", age, age)
	}
}

func TestJSONDocumentSetArrayIndex(t *testing.T) {
	raw := []byte(`{"arr": [1, 2, 3]}`)
	doc, _ := newJSONDocument(raw)
	err := doc.set("$.arr[1]", float64(99))
	if err != nil {
		t.Fatal(err)
	}
	val, _ := doc.get("$.arr[1]")
	if val != float64(99) {
		t.Fatalf("expected 99, got %v (%T)", val, val)
	}
}

func TestJSONDocumentSetCreatesIntermediate(t *testing.T) {
	doc := newEmptyJSONDocument()
	err := doc.set("$.a.b.c.d", "deep")
	if err != nil {
		t.Fatal(err)
	}
	val, _ := doc.get("$.a.b.c.d")
	if val != "deep" {
		t.Fatalf("expected 'deep', got %v", val)
	}
}

func TestJSONDocumentDeleteKey(t *testing.T) {
	raw := []byte(`{"a": 1, "b": 2}`)
	doc, _ := newJSONDocument(raw)
	err := doc.delete("$.a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = doc.get("$.a")
	if err == nil {
		t.Fatal("expected error after delete")
	}
	val, _ := doc.get("$.b")
	if val != 2.0 {
		t.Fatalf("expected 2, got %v", val)
	}
}

func TestJSONDocumentDeleteArrayIndex(t *testing.T) {
	raw := []byte(`{"arr": [10, 20, 30]}`)
	doc, _ := newJSONDocument(raw)
	err := doc.delete("$.arr[1]")
	if err != nil {
		t.Fatal(err)
	}

	results, _ := doc.get("$.arr")
	arr, ok := results.([]any)
	if !ok {
		t.Fatal("expected array")
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
	if arr[0] != 10.0 || arr[1] != 30.0 {
		t.Fatalf("expected [10, 30], got %v", arr)
	}
}

func TestJSONDocumentDeleteRoot(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	doc, _ := newJSONDocument(raw)
	err := doc.delete("$")
	if err != nil {
		t.Fatal(err)
	}
	if doc.root != nil {
		t.Fatal("expected nil root after delete")
	}
}

func TestJSONDocumentTypeOf(t *testing.T) {
	raw := []byte(`{
		"n": null,
		"b": true,
		"num": 42,
		"s": "hello",
		"arr": [1,2,3],
		"obj": {"k": "v"}
	}`)
	doc, _ := newJSONDocument(raw)

	tests := []struct {
		path string
		want string
	}{
		{"$.n", "null"},
		{"$.b", "boolean"},
		{"$.num", "number"},
		{"$.s", "string"},
		{"$.arr", "array"},
		{"$.obj", "object"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got, err := doc.typeOf(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestJSONDocumentArrAppend(t *testing.T) {
	raw := []byte(`{"tags": ["a", "b"]}`)
	doc, _ := newJSONDocument(raw)
	newLen, err := doc.arrAppend("$.tags", "c", "d")
	if err != nil {
		t.Fatal(err)
	}
	if newLen != 4 {
		t.Fatalf("expected 4, got %d", newLen)
	}
	val, _ := doc.get("$.tags")
	arr, ok := val.([]any)
	if !ok || len(arr) != 4 {
		t.Fatalf("expected array of 4, got %v", val)
	}
}

func TestJSONDocumentArrIndex(t *testing.T) {
	raw := []byte(`{"arr": [10, 20, 30]}`)
	doc, _ := newJSONDocument(raw)
	idx, err := doc.arrIndex("$.arr", 20.0)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Fatalf("expected 1, got %d", idx)
	}

	idx, err = doc.arrIndex("$.arr", 99.0)
	if err != nil {
		t.Fatal(err)
	}
	if idx != -1 {
		t.Fatalf("expected -1, got %d", idx)
	}
}

func TestJSONDocumentArrLen(t *testing.T) {
	raw := []byte(`{"arr": [1, 2, 3, 4, 5]}`)
	doc, _ := newJSONDocument(raw)
	n, err := doc.arrLen("$.arr")
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("expected 5, got %d", n)
	}
}

func TestJSONDocumentNumIncrBy(t *testing.T) {
	raw := []byte(`{"counter": 10}`)
	doc, _ := newJSONDocument(raw)
	newVal, err := doc.numIncrBy("$.counter", 5)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 15.0 {
		t.Fatalf("expected 15, got %f", newVal)
	}
	val, _ := doc.get("$.counter")
	if val != 15.0 {
		t.Fatalf("expected 15, got %v", val)
	}
}

func TestJSONDocumentNumMultBy(t *testing.T) {
	raw := []byte(`{"value": 10}`)
	doc, _ := newJSONDocument(raw)
	newVal, err := doc.numMultBy("$.value", 3)
	if err != nil {
		t.Fatal(err)
	}
	if newVal != 30.0 {
		t.Fatalf("expected 30, got %f", newVal)
	}
}

func TestJSONDocumentObjKeys(t *testing.T) {
	raw := []byte(`{"a": 1, "b": 2, "c": 3}`)
	doc, _ := newJSONDocument(raw)
	keys, err := doc.objKeys("$")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	m := make(map[string]bool)
	for _, k := range keys {
		m[k] = true
	}
	if !m["a"] || !m["b"] || !m["c"] {
		t.Fatalf("unexpected keys: %v", keys)
	}
}

func TestJSONDocumentObjLen(t *testing.T) {
	raw := []byte(`{"a": 1, "b": 2}`)
	doc, _ := newJSONDocument(raw)
	n, err := doc.objLen("$")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}

func TestJSONDocumentStrAppend(t *testing.T) {
	raw := []byte(`{"s": "hello"}`)
	doc, _ := newJSONDocument(raw)
	newLen, err := doc.strAppend("$.s", " world")
	if err != nil {
		t.Fatal(err)
	}
	if newLen != 11 {
		t.Fatalf("expected 11, got %d", newLen)
	}
	val, _ := doc.get("$.s")
	if val != "hello world" {
		t.Fatalf("expected 'hello world', got %v", val)
	}
}

func TestJSONDocumentStrLen(t *testing.T) {
	raw := []byte(`{"s": "hello"}`)
	doc, _ := newJSONDocument(raw)
	n, err := doc.strLen("$.s")
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("expected 5, got %d", n)
	}
}

func TestJSONWildcardGet(t *testing.T) {
	raw := []byte(`{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}`)
	doc, _ := newJSONDocument(raw)
	results, err := resolveValue(doc.root, []pathPart{
		{typ: partRoot},
		{typ: partKey, key: "items"},
		{typ: partWildcard},
		{typ: partKey, key: "id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestJSONDocumentSerialize(t *testing.T) {
	raw := []byte(`{"a": 1, "b": [1, 2, 3], "c": {"d": "e"}}`)
	doc, _ := newJSONDocument(raw)
	data, err := doc.serialize()
	if err != nil {
		t.Fatal(err)
	}
	var result any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != 1.0 {
		t.Fatalf("expected 1, got %v", m["a"])
	}
}

// ---------------------------------------------------------------------------
// kv-backed tests
// ---------------------------------------------------------------------------

func setJSONMeta(t *testing.T, db kv.KeyValueStore, key []byte, value []byte, meta byte) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(db.NewEntry(key, value).Metadata(meta))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONKvSetAndGet(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	docJSON := []byte(`{"name":"Alice","age":30}`)
	key := []byte("myjson")
	setJSONMeta(t, db, key, docJSON, byte(common.RedisJSON))

	_, err := db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		if item.Metadata() != byte(common.RedisJSON) {
			t.Fatalf("expected RedisJSON type, got %d", item.Metadata())
		}
		data, err := item.Value()
		if err != nil {
			return nil, err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return nil, err
		}
		name, _ := doc.get("$.name")
		if name != "Alice" {
			t.Fatalf("expected 'Alice', got %v", name)
		}
		age, _ := doc.get("$.age")
		if age != 30.0 {
			t.Fatalf("expected 30, got %v", age)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONKvUpdatePath(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	key := []byte("doc")
	setJSONMeta(t, db, key, []byte(`{"counter": 10}`), byte(common.RedisJSON))

	err := db.Update(func(tx kv.Tx) error {
		item, err := tx.Get(key)
		if err != nil {
			return err
		}
		data, err := item.Value()
		if err != nil {
			return err
		}
		doc, _ := newJSONDocument(data)
		if err := doc.set("$.counter", 25); err != nil {
			return err
		}
		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		return tx.Set(db.NewEntry(key, newData).Metadata(byte(common.RedisJSON)))
	})
	if err != nil {
		t.Fatal(err)
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
		doc, _ := newJSONDocument(data)
		val, _ := doc.get("$.counter")
		if val != 25.0 {
			t.Fatalf("expected 25, got %v", val)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONKvDelete(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	key := []byte("doc")
	setJSONMeta(t, db, key, []byte(`{"a":1,"b":2}`), byte(common.RedisJSON))

	err := db.Update(func(tx kv.Tx) error {
		return tx.Delete(key)
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Read(func(tx kv.Tx) (any, error) {
		_, err := tx.Get(key)
		return nil, err
	})
	if err != kv.ErrKeyNotFound {
		t.Fatal("expected key to be deleted")
	}
}

func TestJSONKvWrongType(t *testing.T) {
	db := kv.InMemoryBadger(t)
	defer db.Close()

	key := []byte("doc")
	setJSONMeta(t, db, key, []byte("not json"), byte(common.RedisString))

	_, err := db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		if item.Metadata() != byte(common.RedisJSON) {
			return nil, errWrongType
		}
		return nil, nil
	})
	if err != errWrongType {
		t.Fatalf("expected wrong type error, got %v", err)
	}
}
