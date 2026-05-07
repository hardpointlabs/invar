package redis

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/badger/v4"
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
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test LPUSH - push to head
	err = db.Update(func(txn *badger.Txn) error {
		size, err := lpush(txn, nil, []byte("mylist"), []byte("b"), []byte("a"))
		if err != nil {
			return err
		}
		if size != 2 {
			t.Errorf("Expected size 2, got %d", size)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify: list should be a -> b
	err = db.View(func(txn *badger.Txn) error {
		list, err := loadList(txn, []byte("mylist"), 0)
		if err != nil {
			return err
		}
		if list.size != 2 {
			t.Errorf("Expected size 2, got %d", list.size)
		}
		if string(list.head.value) != "a" {
			t.Errorf("Expected head 'a', got %s", string(list.head.value))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test RPUSH - push to tail
	err = db.Update(func(txn *badger.Txn) error {
		size, err := rpush(txn, nil, []byte("mylist"), []byte("c"), []byte("d"))
		if err != nil {
			return err
		}
		if size != 4 {
			t.Errorf("Expected size 4, got %d", size)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify: list should be a -> b -> c -> d
	err = db.View(func(txn *badger.Txn) error {
		list, err := loadList(txn, []byte("mylist"), 0)
		if err != nil {
			return err
		}
		if list.size != 4 {
			t.Errorf("Expected size 4, got %d", list.size)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLPopRPop(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create list: a -> b -> c
	err = db.Update(func(txn *badger.Txn) error {
		_, err := lpush(txn, nil, []byte("mylist"), []byte("c"), []byte("b"), []byte("a"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test LPOP
	var popped []byte
	err = db.Update(func(txn *badger.Txn) error {
		popped, err = lpop(txn, nil, []byte("mylist"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped) != "a" {
		t.Errorf("Expected 'a', got %s", string(popped))
	}

	// Test RPOP
	err = db.Update(func(txn *badger.Txn) error {
		popped, err = rpop(txn, nil, []byte("mylist"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped) != "c" {
		t.Errorf("Expected 'c', got %s", string(popped))
	}
}

func TestLLen(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Non-existing list
	err = db.View(func(txn *badger.Txn) error {
		size, err := llen(txn, nil, []byte("nonexistent"))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if size != 0 {
			t.Errorf("Expected 0, got %d", size)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create list
	err = db.Update(func(txn *badger.Txn) error {
		_, err := lpush(txn, nil, []byte("mylist"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		size, err := llen(txn, nil, []byte("mylist"))
		if err != nil {
			return err
		}
		if size != 2 {
			t.Errorf("Expected 2, got %d", size)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLRange(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create list: a -> b -> c -> d -> e
	err = db.Update(func(txn *badger.Txn) error {
		_, err := rpush(txn, nil, []byte("mylist"), []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test full range
	err = db.View(func(txn *badger.Txn) error {
		items, err := lrange(txn, nil, []byte("mylist"), 0, -1)
		if err != nil {
			return err
		}
		if len(items) != 5 {
			t.Errorf("Expected 5 items, got %d", len(items))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test partial range
	err = db.View(func(txn *badger.Txn) error {
		items, err := lrange(txn, nil, []byte("mylist"), 1, 3)
		if err != nil {
			return err
		}
		if len(items) != 3 {
			t.Errorf("Expected 3 items, got %d", len(items))
		}
		if !bytes.Equal(items[0], []byte("b")) {
			t.Errorf("Expected 'b', got %s", string(items[0]))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLIndex(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create list: a -> b -> c
	err = db.Update(func(txn *badger.Txn) error {
		_, err := rpush(txn, nil, []byte("mylist"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test valid index
	err = db.View(func(txn *badger.Txn) error {
		val, err := lindex(txn, nil, []byte("mylist"), 1)
		if err != nil {
			return err
		}
		if !bytes.Equal(val, []byte("b")) {
			t.Errorf("Expected 'b', got %s", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test negative index
	err = db.View(func(txn *badger.Txn) error {
		val, err := lindex(txn, nil, []byte("mylist"), -1)
		if err != nil {
			return err
		}
		if !bytes.Equal(val, []byte("c")) {
			t.Errorf("Expected 'c', got %s", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test out of range
	err = db.View(func(txn *badger.Txn) error {
		val, err := lindex(txn, nil, []byte("mylist"), 10)
		if err != nil {
			return err
		}
		if val != nil {
			t.Errorf("Expected nil, got %s", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
