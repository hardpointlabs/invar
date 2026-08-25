package list

import (
	"bytes"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/testutil"
)

func TestMakeNewList(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	if list.size != 3 {
		t.Error("Expected list size 3, got", list.size)
	}
	if string(list.name) != "mylist" {
		t.Error("Expected list name 'mylist', got", string(list.name))
	}
	if string(list.head.value) != "value1" {
		t.Error("Expected head value 'value1', got", string(list.head.value))
	}
	if string(list.tail.value) != "value3" {
		t.Error("Expected tail value 'value3', got", string(list.tail.value))
	}
	if list.head.prev != nil {
		t.Error("Expected head.prev to be nil")
	}
	if list.tail.next != nil {
		t.Error("Expected tail.next to be nil")
	}
}

func TestListIteration(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	count := 0
	for item := range list.all() {
		if item.value == nil {
			t.Error("Expected non-nil value in list node")
		}
		if item.key == nil {
			t.Error("Expected non-nil key in list node")
		}
		count++
	}
	if count != 3 {
		t.Error("Expected 3 items, got", count)
	}
}

func TestAddFirst(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"))
	size := list.addFirst([]byte("newhead"))
	if size != 3 {
		t.Error("Expected size 3, got", size)
	}
	if string(list.head.value) != "newhead" {
		t.Error("Expected head value 'newhead', got", string(list.head.value))
	}
	if string(list.head.next.value) != "value1" {
		t.Error("Expected head.next.value 'value1', got", string(list.head.next.value))
	}
}

func TestAddLast(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"))
	size := list.addLast([]byte("newtail"))
	if size != 3 {
		t.Error("Expected size 3, got", size)
	}
	if string(list.tail.value) != "newtail" {
		t.Error("Expected tail value 'newtail', got", string(list.tail.value))
	}
	if string(list.tail.prev.value) != "value2" {
		t.Error("Expected tail.prev.value 'value2', got", string(list.tail.prev.value))
	}
}

func TestRemoveFirst(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	val := list.removeFirst()
	if string(val) != "value1" {
		t.Error("Expected 'value1', got", string(val))
	}
	if list.size != 2 {
		t.Error("Expected size 2, got", list.size)
	}
	if string(list.head.value) != "value2" {
		t.Error("Expected head 'value2', got", string(list.head.value))
	}
}

func TestRemoveLast(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	val := list.removeLast()
	if string(val) != "value3" {
		t.Error("Expected 'value3', got", string(val))
	}
	if list.size != 2 {
		t.Error("Expected size 2, got", list.size)
	}
	if string(list.tail.value) != "value2" {
		t.Error("Expected tail 'value2', got", string(list.tail.value))
	}
}

func TestLPushRPush(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Test LPUSH - push to head
	err := db.Update(func(tx kv.Tx) error {
		op := LPush(session, key, []byte("b"), []byte("a"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 2 {
			t.Errorf("Expected size 2, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify: list should be a -> b
	var ll *linkedList
	_, err = db.Read(func(tx kv.Tx) (any, error) {
		var err error
		ll, err = loadList(tx, session, key)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if ll.size != 2 {
		t.Errorf("Expected size 2, got %d", ll.size)
	}
	if string(ll.head.value) != "a" {
		t.Errorf("Expected head 'a', got %s", string(ll.head.value))
	}

	// Test RPUSH - push to tail
	err = db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("c"), []byte("d"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 4 {
			t.Errorf("Expected size 4, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify: list should be a -> b -> c -> d
	_, err = db.Read(func(tx kv.Tx) (any, error) {
		var err error
		ll, err = loadList(tx, session, key)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if ll.size != 4 {
		t.Errorf("Expected size 4, got %d", ll.size)
	}
}

func TestLPopRPop(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> c
	err := db.Update(func(tx kv.Tx) error {
		op := LPush(session, key, []byte("c"), []byte("b"), []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test LPOP
	var popped []byte
	err = db.Update(func(tx kv.Tx) error {
		op := LPop(session, key)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		popped = result.([]byte)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped) != "a" {
		t.Errorf("Expected 'a', got %s", string(popped))
	}

	// Test RPOP
	err = db.Update(func(tx kv.Tx) error {
		op := RPop(session, key)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		popped = result.([]byte)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped) != "c" {
		t.Errorf("Expected 'c', got %s", string(popped))
	}
}

func TestLPopOnMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	err := db.Update(func(tx kv.Tx) error {
		op := LPop(session, []byte("nonexistent"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result != nil {
			t.Errorf("Expected nil, got %s", string(result.([]byte)))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	// Non-existing list
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LLen(session, []byte("nonexistent"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("Expected 0, got %d", result)
	}

	// Create list
	err = db.Update(func(tx kv.Tx) error {
		op := LPush(session, []byte("mylist"), []byte("a"), []byte("b"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := LLen(session, []byte("mylist"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 2 {
		t.Errorf("Expected 2, got %d", result)
	}
}

func TestLRange(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> c -> d -> e
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test full range
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, key, 0, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if len(items) != 5 {
		t.Errorf("Expected 5 items, got %d", len(items))
	}

	// Test partial range
	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, key, 1, 3)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items = result.([][]byte)
	if len(items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(items))
	}
	if !bytes.Equal(items[0], []byte("b")) {
		t.Errorf("Expected 'b', got %s", string(items[0]))
	}
}

func TestLRangeOnMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, []byte("nonexistent"), 0, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if len(items) != 0 {
		t.Errorf("Expected 0 items, got %d", len(items))
	}
}

func TestLIndex(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> c
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test valid index
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LIndex(session, key, 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.([]byte), []byte("b")) {
		t.Errorf("Expected 'b', got %s", string(result.([]byte)))
	}

	// Test negative index
	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := LIndex(session, key, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.([]byte), []byte("c")) {
		t.Errorf("Expected 'c', got %s", string(result.([]byte)))
	}

	// Test out of range
	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := LIndex(session, key, 10)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %s", string(result.([]byte)))
	}
}

func TestLSet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> c
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Set valid index
	err = db.Update(func(tx kv.Tx) error {
		op := LSet(session, key, 1, []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LIndex(session, key, 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.([]byte), []byte("x")) {
		t.Errorf("Expected 'x', got %s", string(result.([]byte)))
	}

	// Out of range should error
	err = db.Update(func(tx kv.Tx) error {
		op := LSet(session, key, 10, []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err == nil {
		t.Error("Expected error for out of range index")
	}
}

func TestLRem(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> b -> c
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("b"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remove all 'b' occurrences
	var removed int
	err = db.Update(func(tx kv.Tx) error {
		op := LRem(session, key, 0, []byte("b"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		removed = result.(int)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}

	// Verify: list should be a -> c
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, key, 0, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if len(items) != 2 || string(items[0]) != "a" || string(items[1]) != "c" {
		t.Errorf("Expected [a c], got %v", items)
	}
}

func TestLTrim(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> b -> c -> d -> e
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := LTrim(session, key, 1, 3)
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, key, 0, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if len(items) != 3 || string(items[0]) != "b" || string(items[2]) != "d" {
		t.Errorf("Expected [b c d], got %v", items)
	}
}

func TestLInsert(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// Create list: a -> c
	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert before pivot
	var result any
	err = db.Update(func(tx kv.Tx) error {
		op := LInsert(session, key, true, []byte("c"), []byte("b"))
		result, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 3 {
		t.Errorf("Expected 3, got %d", result)
	}

	// Verify: a -> b -> c
	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := LRange(session, key, 0, -1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([][]byte)
	if string(items[1]) != "b" {
		t.Errorf("Expected 'b' at index 1, got %s", string(items[1]))
	}

	// Insert after pivot
	err = db.Update(func(tx kv.Tx) error {
		op := LInsert(session, key, false, []byte("c"), []byte("d"))
		result, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 4 {
		t.Errorf("Expected 4, got %d", result)
	}

	// Missing pivot returns -1
	err = db.Update(func(tx kv.Tx) error {
		op := LInsert(session, key, true, []byte("zzz"), []byte("x"))
		result, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != -1 {
		t.Errorf("Expected -1, got %d", result)
	}
}

func TestLPushXRPushX(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	// LPUSHX on missing key should be a no-op
	err := db.Update(func(tx kv.Tx) error {
		op := LPushX(session, key, []byte("a"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 0 {
			t.Errorf("Expected 0, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create list
	err = db.Update(func(tx kv.Tx) error {
		op := LPush(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// LPUSHX on existing key
	err = db.Update(func(tx kv.Tx) error {
		op := LPushX(session, key, []byte("x"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 2 {
			t.Errorf("Expected 2, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// RPUSHX on missing key should be a no-op
	err = db.Update(func(tx kv.Tx) error {
		op := RPushX(session, []byte("other"), []byte("a"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 0 {
			t.Errorf("Expected 0, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// RPUSHX on existing key
	err = db.Update(func(tx kv.Tx) error {
		op := RPushX(session, key, []byte("y"))
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		if result.(int) != 3 {
			t.Errorf("Expected 3, got %d", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteOnEmptyAfterPop(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	defer db.Close()

	key := []byte("mylist")

	err := db.Update(func(tx kv.Tx) error {
		op := RPush(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Pop the only element
	var popped []byte
	err = db.Update(func(tx kv.Tx) error {
		op := LPop(session, key)
		result, err := op.DbOp(tx)
		if err != nil {
			return err
		}
		popped = result.([]byte)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped) != "a" {
		t.Errorf("Expected 'a', got %s", string(popped))
	}

	// Key should be gone
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := LLen(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(int) != 0 {
		t.Errorf("Expected 0, got %d", result)
	}
}
