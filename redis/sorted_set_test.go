package redis

import (
	"math"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestZAddNewKey(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		added, err := zadd(txn, 0, []byte("z1"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"))
		if err != nil {
			return err
		}
		if added != 2 {
			t.Fatalf("expected 2 added, got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddUpdatesExisting(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z2"), []byte("1.0"), []byte("a"))
		if err != nil {
			return err
		}
		added, err := zadd(txn, 0, []byte("z2"), []byte("2.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 added (update), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddWithNx(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		added, err := zadd(txn, 0, []byte("z3"), []byte("nx"), []byte("1.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Fatalf("expected 1 added, got %d", added)
		}
		// Second NX add should not add
		added, err = zadd(txn, 0, []byte("z3"), []byte("nx"), []byte("2.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 added (NX existing), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddWithXx(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		// XX on non-existing key should add nothing
		added, err := zadd(txn, 0, []byte("z4"), []byte("xx"), []byte("1.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 added (XX non-existing), got %d", added)
		}
		// Add first without XX
		added, err = zadd(txn, 0, []byte("z4"), []byte("1.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Fatalf("expected 1 added, got %d", added)
		}
		// XX update existing
		added, err = zadd(txn, 0, []byte("z4"), []byte("xx"), []byte("2.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 added (XX update), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddWithCh(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		added, err := zadd(txn, 0, []byte("z5"), []byte("ch"), []byte("1.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Fatalf("expected 1 (CH new), got %d", added)
		}
		added, err = zadd(txn, 0, []byte("z5"), []byte("ch"), []byte("2.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Fatalf("expected 1 (CH update), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZCard(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z6"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcard(txn, 0, []byte("z6"))
		if err != nil {
			return err
		}
		if count != 3 {
			t.Fatalf("expected 3, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZCardNonExisting(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.View(func(txn *badger.Txn) error {
		count, err := zcard(txn, 0, []byte("nonexistent"))
		if err != nil {
			return err
		}
		if count != 0 {
			t.Fatalf("expected 0, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z7"), []byte("3.14"), []byte("pi"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		score, found, err := zscore(txn, 0, []byte("z7"), []byte("pi"))
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected found, got false")
		}
		if math.Abs(score-3.14) > 1e-10 {
			t.Fatalf("expected 3.14, got %f", score)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZScoreNonExisting(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.View(func(txn *badger.Txn) error {
		_, found, err := zscore(txn, 0, []byte("z8"), []byte("nonexistent"))
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected not found")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRem(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z9"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zrem(txn, 0, []byte("z9"), []byte("a"), []byte("c"))
		if err != nil {
			return err
		}
		if removed != 2 {
			t.Fatalf("expected 2 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcard(txn, 0, []byte("z9"))
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("expected 1, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRemLastMemberDeletesKey(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z10"), []byte("1.0"), []byte("a"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zrem(txn, 0, []byte("z10"), []byte("a"))
		if err != nil {
			return err
		}
		if removed != 1 {
			t.Fatalf("expected 1 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		_, found, err := zscore(txn, 0, []byte("z10"), []byte("a"))
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected member to be gone")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRange(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z11"),
			[]byte("1.0"), []byte("a"),
			[]byte("3.0"), []byte("c"),
			[]byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z11"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result))
		}
		if string(result[0]) != "a" || string(result[1]) != "b" || string(result[2]) != "c" {
			t.Fatalf("expected [a b c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeWithScores(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z12"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z12"), 0, -1, true)
		if err != nil {
			return err
		}
		if len(result) != 4 {
			t.Fatalf("expected 4 results (2 members + 2 scores), got %d", len(result))
		}
		if string(result[0]) != "a" || string(result[1]) != "1" {
			t.Fatalf("expected [a 1 ...], got [%s %s ...]", result[0], result[1])
		}
		if string(result[2]) != "b" || string(result[3]) != "2" {
			t.Fatalf("expected [b 2 ...], got [%s %s ...]", result[2], result[3])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeNegativeIndices(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z13"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z13"), -2, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 results, got %d", len(result))
		}
		if string(result[0]) != "b" || string(result[1]) != "c" {
			t.Fatalf("expected [b c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRevRange(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z14"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrevrange(txn, 0, []byte("z14"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result))
		}
		if string(result[0]) != "c" || string(result[1]) != "b" || string(result[2]) != "a" {
			t.Fatalf("expected [c b a], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRank(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z15"),
			[]byte("1.0"), []byte("a"),
			[]byte("3.0"), []byte("c"),
			[]byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		rank, found, err := zrank(txn, 0, []byte("z15"), []byte("b"))
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected found")
		}
		if rank != 1 {
			t.Fatalf("expected rank 1, got %d", rank)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRankNonExisting(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z16"), []byte("1.0"), []byte("a"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		_, found, err := zrank(txn, 0, []byte("z16"), []byte("nonexistent"))
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected not found")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRevRank(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z17"),
			[]byte("1.0"), []byte("a"),
			[]byte("3.0"), []byte("c"),
			[]byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		rank, found, err := zrevrank(txn, 0, []byte("z17"), []byte("b"))
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected found")
		}
		if rank != 1 {
			t.Fatalf("expected revrank 1, got %d", rank)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZCount(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z18"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"),
			[]byte("4.0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcount(txn, 0, []byte("z18"), "2.0", "3.0")
		if err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("expected 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZCountExclusive(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z19"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcount(txn, 0, []byte("z19"), "(1.0", "(3.0")
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("expected 1 (only b), got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZCountInf(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z20"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcount(txn, 0, []byte("z20"), "-inf", "+inf")
		if err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("expected 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZIncrByNewMember(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		newScore, err := zincrby(txn, 0, []byte("z21"), 5.0, []byte("a"))
		if err != nil {
			return err
		}
		if math.Abs(newScore-5.0) > 1e-10 {
			t.Fatalf("expected 5.0, got %f", newScore)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZIncrByExistingMember(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z22"), []byte("10.0"), []byte("a"))
		if err != nil {
			return err
		}
		newScore, err := zincrby(txn, 0, []byte("z22"), 5.0, []byte("a"))
		if err != nil {
			return err
		}
		if math.Abs(newScore-15.0) > 1e-10 {
			t.Fatalf("expected 15.0, got %f", newScore)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeByScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z23"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"),
			[]byte("4.0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrangebyscore(txn, 0, []byte("z23"), "2.0", "3.0", false, 0, 0, false)
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if string(result[0]) != "b" || string(result[1]) != "c" {
			t.Fatalf("expected [b c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeByScoreWithScoresLimit(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z24"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"),
			[]byte("4.0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrangebyscore(txn, 0, []byte("z24"), "-inf", "+inf", true, 1, 2, true)
		if err != nil {
			return err
		}
		if len(result) != 4 {
			t.Fatalf("expected 4 (2 members + 2 scores), got %d", len(result))
		}
		if string(result[0]) != "b" || string(result[1]) != "2" {
			t.Fatalf("expected [b 2 ...], got [%s %s ...]", result[0], result[1])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRevRangeByScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z25"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrevrangebyscore(txn, 0, []byte("z25"), "3.0", "1.0", false, 0, 0, false)
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Fatalf("expected 3, got %d", len(result))
		}
		if string(result[0]) != "c" || string(result[1]) != "b" || string(result[2]) != "a" {
			t.Fatalf("expected [c b a], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZPopMin(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z26"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		popped, err := zpopmin(txn, 0, []byte("z26"), 2)
		if err != nil {
			return err
		}
		if len(popped) != 2 {
			t.Fatalf("expected 2 popped, got %d", len(popped))
		}
		if string(popped[0].member) != "a" || math.Abs(popped[0].score-1.0) > 1e-10 {
			t.Fatalf("expected a/1.0, got %s/%f", popped[0].member, popped[0].score)
		}
		if string(popped[1].member) != "b" || math.Abs(popped[1].score-2.0) > 1e-10 {
			t.Fatalf("expected b/2.0, got %s/%f", popped[1].member, popped[1].score)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcard(txn, 0, []byte("z26"))
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("expected 1 remaining, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZPopMax(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z27"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		popped, err := zpopmax(txn, 0, []byte("z27"), 1)
		if err != nil {
			return err
		}
		if len(popped) != 1 {
			t.Fatalf("expected 1 popped, got %d", len(popped))
		}
		if string(popped[0].member) != "c" || math.Abs(popped[0].score-3.0) > 1e-10 {
			t.Fatalf("expected c/3.0, got %s/%f", popped[0].member, popped[0].score)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRemRangeByRank(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z28"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"),
			[]byte("4.0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zremrangebyrank(txn, 0, []byte("z28"), 1, 2)
		if err != nil {
			return err
		}
		if removed != 2 {
			t.Fatalf("expected 2 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z28"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if string(result[0]) != "a" || string(result[1]) != "d" {
			t.Fatalf("expected [a d], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRemRangeByScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z29"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zremrangebyscore(txn, 0, []byte("z29"), "1.0", "2.0")
		if err != nil {
			return err
		}
		if removed != 2 {
			t.Fatalf("expected 2 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z29"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 1 {
			t.Fatalf("expected 1, got %d", len(result))
		}
		if string(result[0]) != "c" {
			t.Fatalf("expected [c], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZMScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z30"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		scores, found, err := zmscore(txn, 0, []byte("z30"), []byte("a"), []byte("nonexistent"), []byte("c"))
		if err != nil {
			return err
		}
		if len(scores) != 3 {
			t.Fatalf("expected 3 results, got %d", len(scores))
		}
		if !found[0] || found[1] || !found[2] {
			t.Fatalf("expected [true false true], got %v", found)
		}
		if math.Abs(scores[0]-1.0) > 1e-10 || math.Abs(scores[2]-3.0) > 1e-10 {
			t.Fatalf("expected [1.0 ? 3.0], got %v", scores)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRandMember(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z31"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, scores, err := zrandmember(txn, 0, []byte("z31"), 2)
		if err != nil {
			return err
		}
		if len(members) != 2 {
			t.Fatalf("expected 2 members, got %d", len(members))
		}
		if scores != nil {
			t.Fatal("expected nil scores (positive count)")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRandMemberWithScores(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z32"),
			[]byte("1.0"), []byte("a"),
			[]byte("2.0"), []byte("b"),
			[]byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		members, scores, err := zrandmember(txn, 0, []byte("z32"), -2)
		if err != nil {
			return err
		}
		if len(members) != 2 {
			t.Fatalf("expected 2 members, got %d", len(members))
		}
		if len(scores) != 2 {
			t.Fatalf("expected 2 scores, got %d", len(scores))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZLexCount(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z33"),
			[]byte("0"), []byte("a"),
			[]byte("0"), []byte("b"),
			[]byte("0"), []byte("c"),
			[]byte("0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zlexcount(txn, 0, []byte("z33"), "[a", "[c")
		if err != nil {
			return err
		}
		if count != 3 {
			t.Fatalf("expected 3 (a, b, c), got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeByLex(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z34"),
			[]byte("0"), []byte("alpha"),
			[]byte("0"), []byte("bravo"),
			[]byte("0"), []byte("charlie"),
			[]byte("0"), []byte("delta"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrangebylex(txn, 0, []byte("z34"), "[alpha", "(charlie", 0, 0, false)
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if string(result[0]) != "alpha" || string(result[1]) != "bravo" {
			t.Fatalf("expected [alpha bravo], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRemRangeByLex(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z35"),
			[]byte("0"), []byte("a"),
			[]byte("0"), []byte("b"),
			[]byte("0"), []byte("c"),
			[]byte("0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zremrangebylex(txn, 0, []byte("z35"), "[b", "[c")
		if err != nil {
			return err
		}
		if removed != 2 {
			t.Fatalf("expected 2 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("z35"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if string(result[0]) != "a" || string(result[1]) != "d" {
			t.Fatalf("expected [a d], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRevRangeByLex(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z36"),
			[]byte("0"), []byte("a"),
			[]byte("0"), []byte("b"),
			[]byte("0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrevrangebylex(txn, 0, []byte("z36"), "[c", "[a", 0, 0, false)
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Fatalf("expected 3, got %d", len(result))
		}
		if string(result[0]) != "c" || string(result[1]) != "b" || string(result[2]) != "a" {
			t.Fatalf("expected [c b a], got %v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEncodeScoreSortOrder(t *testing.T) {
	scores := []float64{-100.0, -2.0, -1.0, -0.5, -0.1, 0.0, 0.1, 0.5, 1.0, 2.0, 100.0}
	for i := 1; i < len(scores); i++ {
		a := encodeScore(scores[i-1])
		b := encodeScore(scores[i])
		if string(a) >= string(b) {
			t.Fatalf("encodeScore(%f) >= encodeScore(%f)", scores[i-1], scores[i])
		}
	}
}

func TestEncodeScoreRoundTrip(t *testing.T) {
	scores := []float64{-100.5, -1.0, math.Copysign(0, -1), 0.0, 1.5, 3.14, 100.0, math.Inf(-1), math.Inf(1)}
	for _, s := range scores {
		enc := encodeScore(s)
		dec := decodeScore(enc)
		if math.IsInf(s, 1) && !math.IsInf(dec, 1) {
			t.Fatalf("round trip failed for +inf")
		}
		if math.IsInf(s, -1) && !math.IsInf(dec, -1) {
			t.Fatalf("round trip failed for -inf")
		}
		if !math.IsInf(s, 0) && math.Abs(s-dec) > 1e-15 {
			t.Fatalf("round trip failed for %f: got %f", s, dec)
		}
	}
}

func TestZAddWithGtLt(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("z37"), []byte("5.0"), []byte("a"))
		if err != nil {
			return err
		}
		// GT with lower score should not update
		added, err := zadd(txn, 0, []byte("z37"), []byte("gt"), []byte("3.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 (GT with lower), got %d", added)
		}
		// LT with higher score should not update
		added, err = zadd(txn, 0, []byte("z37"), []byte("lt"), []byte("7.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 0 {
			t.Fatalf("expected 0 (LT with higher), got %d", added)
		}
		// GT with higher score should update
		added, err = zadd(txn, 0, []byte("z37"), []byte("ch"), []byte("gt"), []byte("7.0"), []byte("a"))
		if err != nil {
			return err
		}
		if added != 1 {
			t.Fatalf("expected 1 (GT with higher, CH), got %d", added)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZDiff(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("za"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c"))
		if err != nil {
			return err
		}
		_, err = zadd(txn, 0, []byte("zb"), []byte("2.0"), []byte("b"), []byte("4.0"), []byte("d"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		m, err := zdiff(txn, 0, []byte("za"), []byte("zb"))
		if err != nil {
			return err
		}
		if len(m) != 2 {
			t.Fatalf("expected 2, got %d", len(m))
		}
		if _, ok := m["a"]; !ok {
			t.Fatal("expected a in diff")
		}
		if _, ok := m["c"]; !ok {
			t.Fatal("expected c in diff")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZInter(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zk1"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"))
		if err != nil {
			return err
		}
		_, err = zadd(txn, 0, []byte("zk2"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		m, err := zinter(txn, 0, "SUM", []byte("zk1"), []byte("zk2"))
		if err != nil {
			return err
		}
		if len(m) != 1 {
			t.Fatalf("expected 1, got %d", len(m))
		}
		if _, ok := m["b"]; !ok {
			t.Fatal("expected b in inter")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZUnion(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zu1"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"))
		if err != nil {
			return err
		}
		_, err = zadd(txn, 0, []byte("zu2"), []byte("3.0"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		m, err := zunion(txn, 0, "SUM", []byte("zu1"), []byte("zu2"))
		if err != nil {
			return err
		}
		if len(m) != 3 {
			t.Fatalf("expected 3, got %d", len(m))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZStoreResult(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zsrc"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		m, err := loadZSetMap(txn, []byte("zsrc"), 0)
		if err != nil {
			return err
		}
		_ = m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test storeZSetResult
	err = db.Update(func(txn *badger.Txn) error {
		members := []memberScore{
			{member: []byte("x"), score: 10.0},
			{member: []byte("y"), score: 20.0},
		}
		count, err := storeZSetResult(txn, 0, []byte("zdest"), members)
		if err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("expected 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		count, err := zcard(txn, 0, []byte("zdest"))
		if err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("expected 2, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZRangeNonExistingKey(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("nonexistent"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 0 {
			t.Fatalf("expected empty result, got %d", len(result))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddWithIncr(t *testing.T) {
	// Note: ZADD with INCR option is not yet implemented in our handler
	// But the internal function should handle basic cases
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		newScore, err := zincrby(txn, 0, []byte("zincr"), 5.0, []byte("cnt"))
		if err != nil {
			return err
		}
		if math.Abs(newScore-5.0) > 1e-10 {
			t.Fatalf("expected 5.0, got %f", newScore)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLexicalOrderingWithSameScore(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zlex"),
			[]byte("0"), []byte("delta"),
			[]byte("0"), []byte("alpha"),
			[]byte("0"), []byte("charlie"),
			[]byte("0"), []byte("bravo"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("zlex"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 4 {
			t.Fatalf("expected 4, got %d", len(result))
		}
		// Should be sorted lexicographically since all scores are equal
		expected := []string{"alpha", "bravo", "charlie", "delta"}
		for i, e := range expected {
			if string(result[i]) != e {
				t.Fatalf("expected %s at position %d, got %s", e, i, result[i])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZAddScoresNotForcingLexOrder(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zsc"),
			[]byte("0"), []byte("a"),
			[]byte("2"), []byte("b"),
			[]byte("1"), []byte("c"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		result, err := zrange(txn, 0, []byte("zsc"), 0, -1, false)
		if err != nil {
			return err
		}
		if len(result) != 3 {
			t.Fatalf("expected 3, got %d", len(result))
		}
		// Should be sorted by score: a(0), c(1), b(2)
		if string(result[0]) != "a" || string(result[1]) != "c" || string(result[2]) != "b" {
			t.Fatalf("expected [a c b] by score, got [%s %s %s]", result[0], result[1], result[2])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZDestroysKeyOnLastRem(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zlast"), []byte("1.0"), []byte("onlyme"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		removed, err := zrem(txn, 0, []byte("zlast"), []byte("onlyme"))
		if err != nil {
			return err
		}
		if removed != 1 {
			t.Fatalf("expected 1 removed, got %d", removed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		_, err := readZSetCount(txn, []byte("zlast"), 0)
		if err != badger.ErrKeyNotFound {
			t.Fatal("expected ErrKeyNotFound after last member removed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZScoreFormat(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	err := db.Update(func(txn *badger.Txn) error {
		_, err := zadd(txn, 0, []byte("zfmt"), []byte("3.14"), []byte("pi"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		score, found, err := zscore(txn, 0, []byte("zfmt"), []byte("pi"))
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("not found")
		}
		s := strconv.FormatFloat(score, 'f', -1, 64)
		if s != "3.14" {
			t.Fatalf("expected '3.14', got '%s'", s)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
