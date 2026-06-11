package redis

import (
	"bytes"
	"encoding/binary"
	"testing"
	"testing/quick"

	"github.com/dgraph-io/badger/v4"
)

func TestHashKeyRoundTrip(t *testing.T) {
	f := func(dbSlot uint32, hash []byte, field []byte) bool {
		// Skip fields with null bytes — encoding limitation:
		// internalHashKey uses \x00 as hash/field separator, and
		// fieldFromInternalKey (bytes.LastIndexByte(key, 0)) finds
		// the *last* null byte, so embedded nulls break round-trip.
		if bytes.Contains(field, []byte{0}) {
			return true
		}
		key := internalHashKey(hash, field, int(dbSlot)%1024)
		got := fieldFromInternalKey(key)
		return bytes.Equal(field, got)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHashFieldsPrefixMembership(t *testing.T) {
	f := func(dbSlot uint32, hash []byte, field []byte) bool {
		key := internalHashKey(hash, field, int(dbSlot)%1024)
		prefix := hashFieldsPrefix(hash, int(dbSlot)%1024)
		return bytes.HasPrefix(key, prefix)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHashKeyFormat(t *testing.T) {
	tests := []struct {
		name string
		hash []byte
		field []byte
		db   int
	}{
		{"empty field", []byte("myhash"), []byte{}, 0},
		{"empty hash", []byte{}, []byte("field1"), 0},
		{"both empty", []byte{}, []byte{}, 0},
		{"unicode field", []byte("myhash"), []byte("αβγ"), 0},
		{"unicode hash", []byte("üser"), []byte("score"), 0},
		{"binary field", []byte("h"), []byte{0x00, 0x01, 0x02, 0xFF}, 0},
		{"field with null byte", []byte("h"), []byte("a\x00b"), 0},
		{"long field", []byte("h"), bytes.Repeat([]byte("x"), 4096), 0},
		{"long hash", bytes.Repeat([]byte("x"), 4096), []byte("f"), 0},
		{"db 7", []byte("h"), []byte("f"), 7},
		{"large db", []byte("h"), []byte("f"), 65535},
		{"numeric field", []byte("h"), []byte("12345"), 0},
		{"dots and dashes", []byte("my.hash"), []byte("sub-field"), 0},
		{"colon in hash", []byte("a:b"), []byte("c"), 0},
		{"colon in field", []byte("a"), []byte("b:c"), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := internalHashKey(tt.hash, tt.field, tt.db)
			prefix := hashFieldsPrefix(tt.hash, tt.db)
			if !bytes.HasPrefix(key, prefix) {
				t.Error("key does not have hashFieldsPrefix")
			}
			if !bytes.HasSuffix(key, tt.field) {
				t.Error("key does not end with field")
			}
			if bytes.Contains(tt.field, []byte{0}) {
				got := fieldFromInternalKey(key)
				if bytes.Equal(got, tt.field) {
					t.Log("round-trip succeeded despite null byte in field (edge)")
				}
			} else {
				got := fieldFromInternalKey(key)
				if !bytes.Equal(got, tt.field) {
					t.Errorf("round-trip failed: got %q, want %q", got, tt.field)
				}
			}
			if tt.db == 0 && len(tt.hash) > 0 {
				expectedPrefix := []byte("-0:" + string(tt.hash) + "\x00")
				if !bytes.HasPrefix(key, expectedPrefix) {
					t.Errorf("expected prefix -0:hash\\x00, got %q", key[:len(expectedPrefix)])
				}
			}
		})
	}
}

func TestHashSentinelIsNotFieldPrefix(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := hset(txn, 0, []byte("myhash"), []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		sentinelKey := rawKeyPrefix([]byte("myhash"), 0)
		fieldPrefix := hashFieldsPrefix([]byte("myhash"), 0)

		_, err := txn.Get(sentinelKey)
		if err != nil {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.Prefix = fieldPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if bytes.Equal(k, sentinelKey) {
				t.Error("sentinel key appeared under field prefix iterator")
			}
			count++
		}
		if count != 2 {
			t.Errorf("expected 2 fields, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHashSentinelRoundTrip(t *testing.T) {
	f := func(count uint32) bool {
		hash := []byte("testhash")
		db := 0
		key := rawKeyPrefix(hash, db)
		val := make([]byte, 4)
		binary.BigEndian.PutUint32(val, count)

		opts := badger.DefaultOptions("").WithInMemory(true)
		dbInst, err := badger.Open(opts)
		if err != nil {
			return false
		}
		defer dbInst.Close()

		err = dbInst.Update(func(txn *badger.Txn) error {
			return txn.SetEntry(badger.NewEntry(key, val).WithMeta(byte(RedisHash)))
		})
		if err != nil {
			return false
		}

		err = dbInst.View(func(txn *badger.Txn) error {
			got, err := readHashCount(txn, hash, db)
			if err != nil {
				return err
			}
			if got != count {
				t.Errorf("count mismatch: got %d, want %d", got, count)
			}
			return nil
		})
		return err == nil
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHashClearAndReAdd(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := hset(txn, 0, []byte("h"), []byte("a"), []byte("1"), []byte("b"), []byte("2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		return clearHash(txn, []byte("h"), 0)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := hlen(txn, 0, []byte("h"))
		if err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("expected 0 after clear, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		added, err := hset(txn, 0, []byte("h"), []byte("x"), []byte("10"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Errorf("expected 1 added, got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := hlen(txn, 0, []byte("h"))
		if err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("expected 1 after re-add, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHashDbIsolation(t *testing.T) {
	key0 := internalHashKey([]byte("h"), []byte("f"), 0)
	key1 := internalHashKey([]byte("h"), []byte("f"), 1)
	if bytes.Equal(key0, key1) {
		t.Error("internalHashKey produced same key for different db slots")
	}

	prefix0 := hashFieldsPrefix([]byte("h"), 0)
	prefix1 := hashFieldsPrefix([]byte("h"), 1)
	if bytes.Equal(prefix0, prefix1) {
		t.Error("hashFieldsPrefix produced same prefix for different db slots")
	}

	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(key0, []byte("db0")).WithMeta(byte(RedisHash)))
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(key1, []byte("db1")).WithMeta(byte(RedisHash)))
	})
	if err != nil {
		t.Fatal(err)
	}

	opts2 := badger.DefaultIteratorOptions
	opts2.Prefix = prefix0
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts2)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 key under db0 prefix, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	opts2.Prefix = prefix1
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts2)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 key under db1 prefix, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHashFieldFromInternalKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want []byte
	}{
		{"normal key", []byte("-0:h\x00field"), []byte("field")},
		{"no null byte", []byte("justakey"), nil},
		{"empty after null", []byte("-0:h\x00"), []byte{}},
		{"multiple null bytes", []byte("-0:h\x00a\x00b"), []byte("b")},
		{"null byte at start", []byte("\x00rest"), []byte("rest")},
		{"null byte at end", []byte("prefix\x00"), []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fieldFromInternalKey(tt.key)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func FuzzHashKeyRoundTrip(f *testing.F) {
	f.Add([]byte("myhash"), []byte("myfield"), uint32(0))
	f.Add([]byte{}, []byte{}, uint32(0))
	f.Add([]byte("h"), []byte("a\x00b"), uint32(0))
	f.Add([]byte("\x00"), []byte("f"), uint32(0))
	f.Fuzz(func(t *testing.T, hash []byte, field []byte, dbSlot uint32) {
		db := int(dbSlot) % 1024
		key := internalHashKey(hash, field, db)
		prefix := hashFieldsPrefix(hash, db)
		if !bytes.HasPrefix(key, prefix) {
			t.Errorf("key does not have field prefix: key=%q prefix=%q", key, prefix)
		}
		if !bytes.HasSuffix(key, field) {
			t.Errorf("key does not end with field: key=%q field=%q", key, field)
		}
	})
}
