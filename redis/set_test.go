package redis

import (
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestSAdd(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		added, err := sadd(txn, 0, []byte("myset"), []byte("a"), []byte("b"), []byte("c"))
		if err != nil {
			return err
		}
		if added != 3 {
			t.Errorf("Expected 3 added, got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("myset"))
		if err != nil {
			return err
		}
		if count != 3 {
			t.Errorf("Expected cardinality 3, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSAddDuplicates(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		added, err := sadd(txn, 0, []byte("set"), []byte("a"), []byte("a"), []byte("b"))
		if err != nil {
			return err
		}
		if added != 2 {
			t.Errorf("Expected 2 added (one duplicate), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("Expected cardinality 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSRem(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := srem(txn, 0, []byte("set"), []byte("a"), []byte("c"), []byte("nonexistent"))
		if err != nil {
			return err
		}
		if removed != 2 {
			t.Errorf("Expected 2 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("Expected cardinality 1, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSRemLastMember(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("only"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := srem(txn, 0, []byte("set"), []byte("only"))
		if err != nil {
			return err
		}
		if removed != 1 {
			t.Errorf("Expected 1 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("Expected cardinality 0, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSCardNonExistent(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("nonexistent"))
		if err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("Expected 0 for non-existent set, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSMembers(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("b"), []byte("a"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, err := smembers(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if len(members) != 3 {
			t.Errorf("Expected 3 members, got %d", len(members))
		}
		sort.Slice(members, func(i, j int) bool {
			return string(members[i]) < string(members[j])
		})
		if string(members[0]) != "a" || string(members[1]) != "b" || string(members[2]) != "c" {
			t.Errorf("Expected [a b c], got %v", members)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSMembersNonExistent(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(txn *badger.Txn) error {
		members, err := smembers(txn, 0, []byte("nonexistent"))
		if err != nil {
			return err
		}
		if len(members) != 0 {
			t.Errorf("Expected empty slice, got %v", members)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSIsMember(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("member1"), []byte("member2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		ok, err := sismember(txn, 0, []byte("set"), []byte("member1"))
		if err != nil {
			return err
		}
		if !ok {
			t.Error("Expected member1 to be in set")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		ok, err := sismember(txn, 0, []byte("set"), []byte("nonexistent"))
		if err != nil {
			return err
		}
		if ok {
			t.Error("Expected nonexistent to not be in set")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSPop(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var popped []byte
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		popped, err = spop(txn, 0, []byte("set"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if popped == nil {
		t.Fatal("Expected a popped member, got nil")
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("Expected cardinality 2 after pop, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSPopNonExistent(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		val, err := spop(txn, 0, []byte("nonexistent"))
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

func TestSRandMember(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, err := srandmember(txn, 0, []byte("set"), 1)
		if err != nil {
			return err
		}
		if len(members) != 1 {
			t.Errorf("Expected 1 member, got %d", len(members))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		// Verify set still has 3 members (no removal)
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 3 {
			t.Errorf("Expected cardinality 3, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSMove(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("src"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("dst"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var moved bool
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		moved, err = smove(txn, 0, []byte("src"), []byte("dst"), []byte("a"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !moved {
		t.Error("Expected smove to return true")
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("src"))
		if err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("Expected src cardinality 1, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		ok, err := sismember(txn, 0, []byte("dst"), []byte("a"))
		if err != nil {
			return err
		}
		if !ok {
			t.Error("Expected 'a' to be in dst after move")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSMoveNonExistentSource(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var moved bool
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		moved, err = smove(txn, 0, []byte("nonexistent"), []byte("dst"), []byte("x"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if moved {
		t.Error("Expected smove to return false when source doesn't exist")
	}
}

func TestSDiff(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"), []byte("c"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("c"), []byte("d"), []byte("e"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := sdiff(txn, 0, []byte("set1"), []byte("set2"))
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Errorf("Expected 2 members in diff, got %d", len(result))
		}
		sort.Slice(result, func(i, j int) bool {
			return string(result[i]) < string(result[j])
		})
		if string(result[0]) != "a" || string(result[1]) != "b" {
			t.Errorf("Expected [a b], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSInter(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("b"), []byte("c"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := sinter(txn, 0, []byte("set1"), []byte("set2"))
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Errorf("Expected 2 members in intersection, got %d", len(result))
		}
		sort.Slice(result, func(i, j int) bool {
			return string(result[i]) < string(result[j])
		})
		if string(result[0]) != "b" || string(result[1]) != "c" {
			t.Errorf("Expected [b c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSUnion(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := sunion(txn, 0, []byte("set1"), []byte("set2"))
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Errorf("Expected 3 members in union, got %d", len(result))
		}
		sort.Slice(result, func(i, j int) bool {
			return string(result[i]) < string(result[j])
		})
		if string(result[0]) != "a" || string(result[1]) != "b" || string(result[2]) != "c" {
			t.Errorf("Expected [a b c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSDiffStore(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		count, err = sdiffstore(txn, 0, []byte("dest"), []byte("set1"), []byte("set2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("Expected 2 members stored, got %d", count)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, err := smembers(txn, 0, []byte("dest"))
		if err != nil {
			return err
		}
		if len(members) != 2 {
			t.Errorf("Expected 2 members in dest, got %d", len(members))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSInterStore(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("b"), []byte("c"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		count, err = sinterstore(txn, 0, []byte("dest"), []byte("set1"), []byte("set2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("Expected 2 members stored, got %d", count)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, err := smembers(txn, 0, []byte("dest"))
		if err != nil {
			return err
		}
		if len(members) != 2 {
			t.Errorf("Expected 2 members in dest, got %d", len(members))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSUnionStore(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set1"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set2"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		count, err = sunionstore(txn, 0, []byte("dest"), []byte("set1"), []byte("set2"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Expected 3 members stored, got %d", count)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, err := smembers(txn, 0, []byte("dest"))
		if err != nil {
			return err
		}
		if len(members) != 3 {
			t.Errorf("Expected 3 members in dest, got %d", len(members))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSAddAfterDelete(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_, err := sadd(txn, 0, []byte("set"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		_, err := srem(txn, 0, []byte("set"), []byte("a"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		added, err := sadd(txn, 0, []byte("set"), []byte("x"), []byte("y"))
		if err != nil {
			return err
		}
		if added != 2 {
			t.Errorf("Expected 2 new members, got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := scard(txn, 0, []byte("set"))
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("Expected cardinality 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
