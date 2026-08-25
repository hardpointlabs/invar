package hash

import (
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
)

func TestHSetAndGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("f1"), []byte("v1"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HGet(session, key, []byte("f1"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	val := result.([]byte)
	if string(val) != "v1" {
		t.Errorf("got %q, want %q", val, "v1")
	}
}

func TestHSetMultipleFields(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("3"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 3 {
		t.Errorf("hlen: got %d, want 3", result.(int))
	}

	for _, tc := range []struct {
		field, want string
	}{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	} {
		result, err = db.Read(func(tx kv.Tx) (any, error) {
			op := HGet(session, key, []byte(tc.field))
			return op.DbOp(tx)
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(result.([]byte)) != tc.want {
			t.Errorf("hget %q: got %q, want %q", tc.field, result, tc.want)
		}
	}
}

func TestHSetOverwrite(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("f1"), []byte("old"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("f1"), []byte("new"))
		added, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if added != 0 {
			t.Errorf("expected 0 added on overwrite, got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HGet(session, key, []byte("f1"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.([]byte)) != "new" {
		t.Errorf("got %q, want %q", result, "new")
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 1 {
		t.Errorf("hlen: got %d, want 1", result.(int))
	}
}

func TestHSetNX(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSetNX(session, key, []byte("f1"), []byte("v1"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 1 {
			t.Errorf("hsetnx: got %d, want 1", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HSetNX(session, key, []byte("f1"), []byte("v2"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 0 {
			t.Errorf("hsetnx overwrite: got %d, want 0", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HGet(session, key, []byte("f1"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.([]byte)) != "v1" {
		t.Errorf("got %q, want %q", result, "v1")
	}
}

func TestHDel(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("3"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HDel(session, key, []byte("b"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 1 {
			t.Errorf("hdel: got %d, want 1", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 2 {
		t.Errorf("hlen: got %d, want 2", result.(int))
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HDel(session, key, []byte("a"), []byte("c"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 2 {
			t.Errorf("hdel remaining: got %d, want 2", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("hlen after delete all: got %d, want 0", result.(int))
	}
}

func TestHDelNonexistent(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	err := db.Update(func(tx kv.Tx) error {
		op := HDel(session, []byte("noexist"), []byte("f1"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 0 {
			t.Errorf("hdel nonexistent: got %d, want 0", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHExists(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("f1"), []byte("v1"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HExists(session, key, []byte("f1"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 1 {
		t.Errorf("hexists existing: got %d, want 1", result)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HExists(session, key, []byte("noexist"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("hexists non-existing: got %d, want 0", result)
	}
}

func TestHLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("hlen empty: got %d, want 0", result)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 2 {
		t.Errorf("hlen: got %d, want 2", result)
	}
}

func TestHMGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HMGet(session, key, []byte("a"), []byte("missing"), []byte("b"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	results := result.([][]byte)
	if len(results) != 3 {
		t.Fatalf("hmget: got %d results, want 3", len(results))
	}
	if string(results[0]) != "1" {
		t.Errorf("hmget[0]: got %q, want %q", results[0], "1")
	}
	if results[1] != nil {
		t.Errorf("hmget[1]: got %v, want nil", results[1])
	}
	if string(results[2]) != "2" {
		t.Errorf("hmget[2]: got %q, want %q", results[2], "2")
	}
}

func TestHMSet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HMSet(session, key, []byte("x"), []byte("10"), []byte("y"), []byte("20"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HGet(session, key, []byte("x"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.([]byte)) != "10" {
		t.Errorf("hmset then hget: got %q, want %q", result, "10")
	}
}

func TestHKeys(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("c"), []byte("3"), []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HKeys(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := result.([][]byte)
	if len(keys) != 3 {
		t.Fatalf("hkeys: got %d keys, want 3", len(keys))
	}
	fieldSet := make(map[string]bool)
	for _, k := range keys {
		fieldSet[string(k)] = true
	}
	for _, expected := range []string{"a", "b", "c"} {
		if !fieldSet[expected] {
			t.Errorf("hkeys: missing field %q", expected)
		}
	}
}

func TestHKeysEmpty(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HKeys(session, []byte("noexist"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := result.([][]byte)
	if len(keys) != 0 {
		t.Errorf("hkeys empty: got %d keys, want 0", len(keys))
	}
}

func TestHVals(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("alpha"), []byte("b"), []byte("bravo"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HVals(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	vals := result.([][]byte)
	if len(vals) != 2 {
		t.Fatalf("hvals: got %d vals, want 2", len(vals))
	}
	valSet := make(map[string]bool)
	for _, v := range vals {
		valSet[string(v)] = true
	}
	for _, expected := range []string{"alpha", "bravo"} {
		if !valSet[expected] {
			t.Errorf("hvals: missing value %q", expected)
		}
	}
}

func TestHGetAll(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("x"), []byte("1"), []byte("y"), []byte("2"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HGetAll(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	pairs := result.([][]byte)
	if len(pairs) != 4 {
		t.Fatalf("hgetall: got %d items, want 4", len(pairs))
	}
	pairMap := make(map[string]string)
	for i := 0; i < len(pairs); i += 2 {
		pairMap[string(pairs[i])] = string(pairs[i+1])
	}
	if pairMap["x"] != "1" || pairMap["y"] != "2" {
		t.Errorf("hgetall: got %v, want {x:1, y:2}", pairMap)
	}
}

func TestHGetAllEmpty(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HGetAll(session, []byte("noexist"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	pairs := result.([][]byte)
	if len(pairs) != 0 {
		t.Errorf("hgetall empty: got %d items, want 0", len(pairs))
	}
}

func TestHIncrBy(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HIncrBy(session, key, []byte("counter"), 5)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int64) != 5 {
			t.Errorf("hincrby: got %d, want 5", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HIncrBy(session, key, []byte("counter"), 3)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int64) != 8 {
			t.Errorf("hincrby: got %d, want 8", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HIncrBy(session, key, []byte("counter"), -10)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int64) != -2 {
			t.Errorf("hincrby negative: got %d, want -2", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHIncrByFloat(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HIncrByFloat(session, key, []byte("score"), 1.5)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(string) != "1.5" {
			t.Errorf("hincrbyfloat: got %q, want %q", result, "1.5")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := HIncrByFloat(session, key, []byte("score"), 2.5)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(string) != "4" {
			t.Errorf("hincrbyfloat: got %q, want %q", result, "4")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHStrLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("f1"), []byte("hello"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HStrLen(session, key, []byte("f1"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 5 {
		t.Errorf("hstrlen: got %d, want 5", result)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HStrLen(session, key, []byte("missing"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("hstrlen missing: got %d, want 0", result)
	}
}

func TestHRandField(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("3"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HRandField(session, key, 2, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	fields := result.([][]byte)
	if len(fields) != 2 {
		t.Fatalf("hrandfield: got %d fields, want 2", len(fields))
	}
	for _, f := range fields {
		s := string(f)
		if s != "a" && s != "b" && s != "c" {
			t.Errorf("hrandfield: unexpected field %q", s)
		}
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := HRandField(session, key, 10, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	fields = result.([][]byte)
	if len(fields) != 3 {
		t.Errorf("hrandfield over-request: got %d, want 3", len(fields))
	}
}

func TestHRandFieldWithValues(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key, []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HRandField(session, key, 1, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if len(items) != 2 {
		t.Fatalf("hrandfield withvalues: got %d items, want 2", len(items))
	}
}

func TestHRandFieldEmpty(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HRandField(session, []byte("empty"), 5, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("hrandfield non-existing key: got %v, want nil", result)
	}
}

func TestHScan(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key,
			[]byte("field1"), []byte("val1"),
			[]byte("field2"), []byte("val2"),
			[]byte("other1"), []byte("val3"),
		)
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HScan(session, key, "field*", 0)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	pairs := result.([][]byte)
	if len(pairs) != 4 {
		t.Fatalf("hscan: got %d items, want 4", len(pairs))
	}
}

func TestHScanWithCount(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("myhash")

	err := db.Update(func(tx kv.Tx) error {
		op := HSet(session, key,
			[]byte("a"), []byte("1"),
			[]byte("b"), []byte("2"),
			[]byte("c"), []byte("3"),
		)
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := HScan(session, key, "", 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	pairs := result.([][]byte)
	if len(pairs) != 2 {
		t.Errorf("hscan count=1: got %d items, want 2", len(pairs))
	}
}

func TestIteratorBasic(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	// Write some keys directly
	err := db.Update(func(tx kv.Tx) error {
		for i := 0; i < 5; i++ {
			key := session.PrivateKey([]byte("test\x00" + string(rune('a'+i))))
			entry := db.NewEntry(key, []byte("val"))
			if err := tx.Set(entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read them back via iterator
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		prefix := session.PrivateKey([]byte("test\x00"))
		it := tx.NewIterator(prefix)
		defer it.Close()

		var keys [][]byte
		for it.Next() {
			keys = append(keys, it.Item().Key())
		}
		return keys, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := result.([][]byte)
	if len(keys) != 5 {
		t.Errorf("iterator: got %d keys, want 5", len(keys))
	}
	t.Logf("keys found: %v", keys)
}

func TestMemberFromInternalKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want []byte
	}{
		{"normal", []byte("-0:h\x00field"), []byte("field")},
		{"no null", []byte("justakey"), nil},
		{"empty after null", []byte("-0:h\x00"), []byte{}},
		{"multiple nulls", []byte("-0:h\x00a\x00b"), []byte("b")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := common.MemberFromInternalKey(tt.key)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if string(got) != string(tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
