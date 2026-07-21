package kv

import (
	"errors"
	"testing"
	"time"
)

// --- KeyValueStore tests ---

func TestNewEntry(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	key := []byte("mykey")
	val := []byte("myval")
	entry := kvs.NewEntry(key, val)

	if string(entry.Key()) != string(key) {
		t.Errorf("Key(): got %q, want %q", entry.Key(), key)
	}
}

func TestNewEntryIsolation(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	e1 := kvs.NewEntry([]byte("a"), []byte("1"))
	e2 := kvs.NewEntry([]byte("b"), []byte("2"))

	if string(e1.Key()) != "a" {
		t.Errorf("e1.Key(): got %q, want %q", e1.Key(), "a")
	}
	if string(e2.Key()) != "b" {
		t.Errorf("e2.Key(): got %q, want %q", e2.Key(), "b")
	}
}

func TestEntryMetadata(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	entry := kvs.NewEntry([]byte("k"), []byte("v"))
	result := entry.Metadata(0x42)

	if result != entry {
		t.Error("Metadata() should return the same Entry for chaining")
	}

	// Write and read back with metadata
	err := kvs.Update(func(tx Tx) error {
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kvs.Update(func(tx Tx) error {
		item, err := tx.Get([]byte("k"))
		if err != nil {
			return err
		}
		// Verify value is retrievable
		val, err := item.Value()
		if err != nil {
			return err
		}
		if string(val) != "v" {
			t.Errorf("Value(): got %q, want %q", val, "v")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestEntryTTL(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	entry := kvs.NewEntry([]byte("k"), []byte("v"))
	result := entry.TTL(5 * time.Second)

	if result != entry {
		t.Error("TTL() should return the same Entry for chaining")
	}
}

func TestEntryChaining(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	entry := kvs.NewEntry([]byte("k"), []byte("v")).Metadata(0x01).TTL(10 * time.Second)

	err := kvs.Update(func(tx Tx) error {
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set with chained entry failed: %v", err)
	}

	// Confirm value stored
	val, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("k"))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val.([]byte)) != "v" {
		t.Errorf("value: got %q, want %q", val, "v")
	}
}

// --- Update / Read ---

func TestUpdateAndRead(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("hello"), []byte("world"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	result, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("hello"))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(result.([]byte)) != "world" {
		t.Errorf("got %q, want %q", result, "world")
	}
}

func TestUpdateRollback(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// First write succeeds
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("k"), []byte("v1"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("first Update failed: %v", err)
	}

	// Second write returns an error — transaction should be rolled back
	err = kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("k"), []byte("v2"))
		if err := tx.Set(entry); err != nil {
			return err
		}
		return errExplicit
	})
	if err == nil {
		t.Fatal("expected error from Update, got nil")
	}

	// Original value should still be there
	result, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("k"))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(result.([]byte)) != "v1" {
		t.Errorf("after rollback: got %q, want %q", result, "v1")
	}
}

func TestReadReturnValue(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Write a value
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("x"), []byte("42"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Read should return whatever the function returns
	result, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("x"))
		if err != nil {
			return nil, err
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		return string(val), nil
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if result.(string) != "42" {
		t.Errorf("got %q, want %q", result, "42")
	}
}

func TestReadErrorPropagation(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	_, err := kvs.Read(func(tx Tx) (any, error) {
		return nil, errExplicit
	})
	if err != errExplicit {
		t.Errorf("expected errExplicit, got %v", err)
	}
}

// --- Begin ---

func TestBeginReadonly(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Write data via Update
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("rkey"), []byte("rval"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Begin a read-only transaction
	tx := kvs.Begin(false)
	defer tx.Discard()

	item, err := tx.Get([]byte("rkey"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	val, err := item.Value()
	if err != nil {
		t.Fatalf("Value failed: %v", err)
	}
	if string(val) != "rval" {
		t.Errorf("got %q, want %q", val, "rval")
	}
}

func TestBeginMutating(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	tx := kvs.Begin(true)
	defer tx.Discard()

	entry := kvs.NewEntry([]byte("mkey"), []byte("mval"))
	err := tx.Set(entry)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify committed
	val, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("mkey"))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val.([]byte)) != "mval" {
		t.Errorf("got %q, want %q", val, "mval")
	}
}

func TestBeginDiscard(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	tx := kvs.Begin(true)
	entry := kvs.NewEntry([]byte("dkey"), []byte("dval"))
	_ = tx.Set(entry)
	tx.Discard()

	// Should not be readable
	_, err := kvs.Read(func(tx Tx) (any, error) {
		return tx.Get([]byte("dkey"))
	})
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after Discard, got %v", err)
	}
}

// --- Tx Get/Set/Delete ---

func TestTxSetAndGet(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("key1"), []byte("val1"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kvs.Update(func(tx Tx) error {
		item, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		if string(val) != "val1" {
			t.Errorf("got %q, want %q", val, "val1")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestTxGetNotFound(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	err := kvs.Update(func(tx Tx) error {
		_, err := tx.Get([]byte("nonexistent"))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
}

func TestTxDelete(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Insert
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("delme"), []byte("gone"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete
	err = kvs.Update(func(tx Tx) error {
		return tx.Delete([]byte("delme"))
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Confirm deleted
	err = kvs.Update(func(tx Tx) error {
		_, err := tx.Get([]byte("delme"))
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestTxOverwrite(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Write v1
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("ow"), []byte("v1"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set v1 failed: %v", err)
	}

	// Overwrite with v2
	err = kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("ow"), []byte("v2"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set v2 failed: %v", err)
	}

	// Read back
	val, err := kvs.Read(func(tx Tx) (any, error) {
		item, err := tx.Get([]byte("ow"))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val.([]byte)) != "v2" {
		t.Errorf("got %q, want %q", val, "v2")
	}
}

// --- Item ---

func TestItemKey(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("itemkey"), []byte("val"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kvs.Update(func(tx Tx) error {
		item, err := tx.Get([]byte("itemkey"))
		if err != nil {
			return err
		}
		if string(item.Key()) != "itemkey" {
			t.Errorf("Item.Key(): got %q, want %q", item.Key(), "itemkey")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestItemTTL(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Item without TTL
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("notls"), []byte("val"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kvs.Update(func(tx Tx) error {
		item, err := tx.Get([]byte("notls"))
		if err != nil {
			return err
		}
		ttl := item.TTL()
		if ttl != 0 {
			t.Errorf("TTL of non-expiring key: got %v, want 0", ttl)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestItemValueCopy(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("copytest"), []byte("original"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get value twice and verify they're independent copies
	err = kvs.Update(func(tx Tx) error {
		item, err := tx.Get([]byte("copytest"))
		if err != nil {
			return err
		}
		v1, _ := item.Value()
		v2, _ := item.Value()

		// Mutate v1
		v1[0] = 'X'

		// v2 should be unaffected
		if string(v2) != "original" {
			t.Errorf("values are not independent copies: v2 = %q", v2)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
}

// --- Multiple keys ---

func TestMultipleKeys(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	pairs := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	// Write all
	err := kvs.Update(func(tx Tx) error {
		for k, v := range pairs {
			entry := kvs.NewEntry([]byte(k), []byte(v))
			if err := tx.Set(entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("batch Set failed: %v", err)
	}

	// Read all
	err = kvs.Update(func(tx Tx) error {
		for k, want := range pairs {
			item, err := tx.Get([]byte(k))
			if err != nil {
				t.Errorf("Get(%q) failed: %v", k, err)
				continue
			}
			val, _ := item.Value()
			if string(val) != want {
				t.Errorf("Get(%q): got %q, want %q", k, val, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("batch Get failed: %v", err)
	}
}

// --- Close ---

func TestClose(t *testing.T) {
	kvs := InMemoryBadger(t)

	// Write something
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("closeme"), []byte("val"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Close
	err = kvs.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

// --- Badger() deprecated accessor ---

func TestBadgerAccessor(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	bdb := kvs.Badger()
	if bdb == nil {
		t.Fatal("Badger() returned nil")
	}
}

// --- NewIterator stub ---

func TestNewIteratorReturnsNil(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	tx := kvs.Begin(false)
	defer tx.Discard()

	it := tx.NewIterator([]byte("prefix"))
	if it == nil {
		t.Fatal("NewIterator should return a pointer to a non-nil iterator")
	}
	kvIt := *it
	if kvIt == nil {
		t.Fatal("NewIterator should return a pointer to a non-nil interface")
	}
	defer kvIt.Close()
}

// --- Merge stub ---

func TestMergeReturnsNil(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	handle, err := kvs.Merge([]byte("key"), []byte("delta"))
	if err != nil {
		t.Errorf("Merge() error: %v", err)
	}
	if handle != nil {
		t.Error("Merge() should return nil WriteHandle (not yet implemented)")
	}
}

// --- Concurrency ---

func TestConcurrentUpdate(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	// Pre-populate
	err := kvs.Update(func(tx Tx) error {
		entry := kvs.NewEntry([]byte("counter"), []byte("0"))
		return tx.Set(entry)
	})
	if err != nil {
		t.Fatalf("initial Set failed: %v", err)
	}

	// Run concurrent updates — they should not corrupt the store
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			_ = kvs.Update(func(tx Tx) error {
				entry := kvs.NewEntry([]byte("counter"), []byte("0"))
				return tx.Set(entry)
			})
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify key is still readable
	_, err = kvs.Read(func(tx Tx) (any, error) {
		return tx.Get([]byte("counter"))
	})
	if err != nil {
		t.Errorf("store corrupted after concurrent updates: %v", err)
	}
}

// --- helpers ---

var errExplicit = errors.New("explicit test error")
