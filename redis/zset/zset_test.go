package zset

import (
	"math"
	"strconv"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
)

func mustAdd(t *testing.T, session *common.Session, db kv.KeyValueStore, key []byte, pairs ...[]byte) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, pairs...)
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func zcardOf(t *testing.T, session *common.Session, db kv.KeyValueStore, key []byte) int {
	t.Helper()
	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count.(int)
}

func TestZAddNewKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 3 {
		t.Errorf("expected 3 added, got %d", added)
	}
	if zcardOf(t, session, db, key) != 3 {
		t.Error("expected card 3")
	}
}

func TestZAddUpdatesExisting(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"))

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("5"), []byte("a"), []byte("7"), []byte("c"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Errorf("expected 1 added, got %d", added)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 5 {
		t.Errorf("expected score 5, got %v", score)
	}
	if zcardOf(t, session, db, key) != 3 {
		t.Error("expected card 3")
	}
}

func TestZAddWithNx(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"))

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("nx"), []byte("9"), []byte("a"), []byte("2"), []byte("b"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Errorf("expected 1 added, got %d", added)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 1 {
		t.Errorf("expected score 1 unchanged, got %v", score)
	}
}

func TestZAddWithXx(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"))

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("xx"), []byte("9"), []byte("a"), []byte("2"), []byte("b"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 0 {
		t.Errorf("expected 0 added, got %d", added)
	}
	if zcardOf(t, session, db, key) != 1 {
		t.Error("expected card 1")
	}
}

func TestZAddWithCh(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"))

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("ch"), []byte("9"), []byte("a"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Errorf("expected 1 changed with CH, got %d", added)
	}
}

func TestZAddWithGtLt(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("5"), []byte("a"))

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("gt"), []byte("3"), []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 5 {
		t.Errorf("expected GT to keep score 5, got %v", score)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, key, []byte("gt"), []byte("7"), []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	score, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 7 {
		t.Errorf("expected GT to raise score to 7, got %v", score)
	}
}

func TestZCardMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZCard(session, []byte("nonexistent"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 0 {
		t.Errorf("expected card 0, got %d", count.(int))
	}
}

func TestZScore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1.5"), []byte("a"))

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 1.5 {
		t.Errorf("expected score 1.5, got %v", score)
	}

	score, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("x"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score != nil {
		t.Errorf("expected nil for missing member, got %v", score)
	}

	score, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, []byte("nonexistent"), []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score != nil {
		t.Errorf("expected nil for missing key, got %v", score)
	}
}

func TestZRem(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRem(session, key, []byte("a"), []byte("x"))
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if zcardOf(t, session, db, key) != 2 {
		t.Error("expected card 2")
	}
}

func TestZRemAllMembers(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"))

	err := db.Update(func(tx kv.Tx) error {
		op := ZRem(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if zcardOf(t, session, db, key) != 0 {
		t.Error("expected card 0 after removing all")
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, 0, -1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty zset, got %d members", len(result.([][]byte)))
	}
}

func TestZRemMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRem(session, []byte("nonexistent"), []byte("a"))
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("expected 0 removed, got %d", removed)
	}
}

func TestZRange(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("3"), []byte("c"), []byte("1"), []byte("a"), []byte("2"), []byte("b"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, 0, -1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	if string(members[0]) != "a" || string(members[1]) != "b" || string(members[2]) != "c" {
		t.Errorf("expected a,b,c in score order, got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, 1, 1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members = result.([][]byte)
	if len(members) != 1 || string(members[0]) != "b" {
		t.Errorf("expected single member b, got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, -2, -1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members = result.([][]byte)
	if len(members) != 2 || string(members[0]) != "b" || string(members[1]) != "c" {
		t.Errorf("expected b,c for negative range, got %v", members)
	}
}

func TestZRangeWithScores(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, 0, -1, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(flat))
	}
	if string(flat[0]) != "a" || string(flat[1]) != "1" || string(flat[2]) != "b" || string(flat[3]) != "2" {
		t.Errorf("expected a,1,b,2 got %v", flat)
	}
}

func TestZRangeEmpty(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, []byte("nonexistent"), 0, -1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty result, got %d", len(result.([][]byte)))
	}
}

func TestZRevRange(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRange(session, key, 0, -1, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	if string(members[0]) != "c" || string(members[1]) != "b" || string(members[2]) != "a" {
		t.Errorf("expected c,b,a in reverse order, got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRange(session, key, 0, -1, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(flat))
	}
	if string(flat[0]) != "c" || string(flat[1]) != "3" || string(flat[4]) != "a" || string(flat[5]) != "1" {
		t.Errorf("expected c,3,...,a,1 got %v", flat)
	}
}

func TestZRank(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	rank, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRank(session, key, []byte("b"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if rank.(int) != 1 {
		t.Errorf("expected rank 1, got %d", rank.(int))
	}

	rank, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRank(session, key, []byte("x"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if rank != nil {
		t.Errorf("expected nil rank, got %v", rank)
	}
}

func TestZRevRank(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	rank, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRank(session, key, []byte("b"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if rank.(int) != 1 {
		t.Errorf("expected revrank 1, got %d", rank.(int))
	}

	rank, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRank(session, key, []byte("x"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if rank != nil {
		t.Errorf("expected nil revrank, got %v", rank)
	}
}

func TestZCount(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZCount(session, key, "1", "3")
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected count 3, got %d", count.(int))
	}

	count, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZCount(session, key, "(1", "3")
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 2 {
		t.Errorf("expected count 2 exclusive min, got %d", count.(int))
	}

	count, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZCount(session, key, "-inf", "+inf")
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected count 3 with infinities, got %d", count.(int))
	}
}

func TestZIncrBy(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")

	var score any
	err := db.Update(func(tx kv.Tx) error {
		op := ZIncrBy(session, key, 5, []byte("a"))
		var err error
		score, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 5 {
		t.Errorf("expected score 5, got %v", score)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := ZIncrBy(session, key, 2.5, []byte("a"))
		var err error
		score, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 7.5 {
		t.Errorf("expected score 7.5, got %v", score)
	}
	if zcardOf(t, session, db, key) != 1 {
		t.Error("expected card 1")
	}
}

func TestZRangeByScore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByScore(session, key, "2", "3", false, 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 || string(members[0]) != "b" || string(members[1]) != "c" {
		t.Errorf("expected b,c got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByScore(session, key, "(2", "+inf", true, 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(flat))
	}
	if string(flat[0]) != "c" || string(flat[1]) != "3" {
		t.Errorf("expected c first with score 3, got %v", flat)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByScore(session, key, "1", "4", false, 1, 2, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members = result.([][]byte)
	if len(members) != 2 || string(members[0]) != "b" || string(members[1]) != "c" {
		t.Errorf("expected b,c with limit 1 2, got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByScore(session, key, "10", "20", false, 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty result, got %d", len(result.([][]byte)))
	}
}

func TestZRevRangeByScore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRangeByScore(session, key, "+inf", "-inf", false, 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	if string(members[0]) != "c" || string(members[1]) != "b" || string(members[2]) != "a" {
		t.Errorf("expected c,b,a got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRangeByScore(session, key, "3", "2", true, 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(flat))
	}
	if string(flat[0]) != "c" || string(flat[1]) != "3" || string(flat[2]) != "b" || string(flat[3]) != "2" {
		t.Errorf("expected c,3,b,2 got %v", flat)
	}
}

func TestZRangeByLex(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"), []byte("0"), []byte("d"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByLex(session, key, "[b", "[c", 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 || string(members[0]) != "b" || string(members[1]) != "c" {
		t.Errorf("expected b,c got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByLex(session, key, "(a", "+", 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members = result.([][]byte)
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRangeByLex(session, key, "-", "+", 1, 2, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members = result.([][]byte)
	if len(members) != 2 || string(members[0]) != "b" || string(members[1]) != "c" {
		t.Errorf("expected b,c with limit, got %v", members)
	}
}

func TestZRevRangeByLex(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRevRangeByLex(session, key, "+", "-", 0, 0, false)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	if string(members[0]) != "c" || string(members[1]) != "b" || string(members[2]) != "a" {
		t.Errorf("expected c,b,a got %v", members)
	}
}

func TestZLexCount(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"))

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZLexCount(session, key, "-", "+")
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected count 3, got %d", count.(int))
	}

	count, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZLexCount(session, key, "[a", "(c")
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 2 {
		t.Errorf("expected count 2, got %d", count.(int))
	}
}

func TestZRemRangeByRank(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRemRangeByRank(session, key, 0, 1)
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("expected 2 removed, got %d", removed)
	}
	if zcardOf(t, session, db, key) != 1 {
		t.Error("expected card 1")
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("c"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 3 {
		t.Errorf("expected c to remain with score 3, got %v", score)
	}
}

func TestZRemRangeByScore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRemRangeByScore(session, key, "(1", "2")
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if zcardOf(t, session, db, key) != 2 {
		t.Error("expected card 2")
	}
}

func TestZRemRangeByLex(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"))

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRemRangeByLex(session, key, "[b", "+")
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("expected 2 removed, got %d", removed)
	}
	if zcardOf(t, session, db, key) != 1 {
		t.Error("expected card 1")
	}
}

func TestZPopMin(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var popped any
	err := db.Update(func(tx kv.Tx) error {
		op := ZPopMin(session, key, 1)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	items := popped.([]MemberScore)
	if len(items) != 1 || string(items[0].Member) != "a" || items[0].Score != 1 {
		t.Errorf("expected a with score 1, got %v", items)
	}
	if zcardOf(t, session, db, key) != 2 {
		t.Error("expected card 2")
	}
}

func TestZPopMinCount(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var popped any
	err := db.Update(func(tx kv.Tx) error {
		op := ZPopMin(session, key, 2)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	items := popped.([]MemberScore)
	if len(items) != 2 || string(items[0].Member) != "a" || string(items[1].Member) != "b" {
		t.Errorf("expected a,b got %v", items)
	}
}

func TestZPopMax(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var popped any
	err := db.Update(func(tx kv.Tx) error {
		op := ZPopMax(session, key, 1)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	items := popped.([]MemberScore)
	if len(items) != 1 || string(items[0].Member) != "c" || items[0].Score != 3 {
		t.Errorf("expected c with score 3, got %v", items)
	}
}

func TestZPopMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	var popped any
	err := db.Update(func(tx kv.Tx) error {
		op := ZPopMin(session, []byte("nonexistent"), 1)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(popped.([]MemberScore)) != 0 {
		t.Errorf("expected empty pop, got %v", popped)
	}
}

func TestZMScore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("3"), []byte("c"))

	var res, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZMScore(session, key, []byte("a"), []byte("x"), []byte("c"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	r := res.(zmscoreResult)
	if len(r.scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(r.scores))
	}
	if r.scores[0] != 1 || !r.found[0] {
		t.Errorf("expected a score 1 found, got %v %v", r.scores[0], r.found[0])
	}
	if r.found[1] {
		t.Errorf("expected x not found")
	}
	if r.scores[2] != 3 || !r.found[2] {
		t.Errorf("expected c score 3 found, got %v %v", r.scores[2], r.found[2])
	}
}

func TestZRandMember(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRandMember(session, key, 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items := result.([]MemberScore)
	if len(items) != 1 {
		t.Fatalf("expected 1 member, got %d", len(items))
	}
	if items[0].Score != 0 {
		t.Errorf("expected score zeroed without withscores, got %v", items[0].Score)
	}
	seen := map[string]bool{"a": true, "b": true, "c": true}
	if !seen[string(items[0].Member)] {
		t.Errorf("expected a known member, got %q", items[0].Member)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZRandMember(session, key, -2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	items = result.([]MemberScore)
	if len(items) != 2 {
		t.Fatalf("expected 2 members, got %d", len(items))
	}
	for _, e := range items {
		if e.Score == 0 {
			t.Errorf("expected nonzero score with negative count, got %v", e.Score)
		}
	}
}

func TestZRandMemberMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRandMember(session, []byte("nonexistent"), 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([]MemberScore)) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}

func TestZDiff(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("1"), []byte("b"), []byte("1"), []byte("d"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZDiff(session, false, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	seen := map[string]bool{}
	for _, m := range members {
		seen[string(m)] = true
	}
	if !seen["a"] || !seen["c"] {
		t.Errorf("expected a,c got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZDiff(session, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 4 {
		t.Fatalf("expected 4 elements withscores, got %d", len(flat))
	}
}

func TestZDiffStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"), []byte("2"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("1"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, dest, []byte("9"), []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := ZDiffStore(session, dest, z1, z2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected stored count 1, got %d", count)
	}
	if zcardOf(t, session, db, dest) != 1 {
		t.Error("expected dest card 1")
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, dest, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 1 {
		t.Errorf("expected a score 1 in dest, got %v", score)
	}
}

func TestZInter(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"), []byte("2"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("3"), []byte("b"), []byte("4"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZInter(session, "SUM", nil, false, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 1 || string(members[0]) != "b" {
		t.Fatalf("expected single member b, got %v", members)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZInter(session, "SUM", nil, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 2 || string(flat[0]) != "b" || flat[1] == nil {
		t.Fatalf("expected b with score, got %v", flat)
	}
	score, err := strconv.ParseFloat(string(flat[1]), 64)
	if err != nil {
		t.Fatal(err)
	}
	if score != 5 {
		t.Errorf("expected summed score 5, got %v", score)
	}
}

func TestZInterMinMaxAggregate(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("5"), []byte("a"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("7"), []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZInter(session, "MIN", nil, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if string(flat[1]) != "5" {
		t.Errorf("expected MIN score 5, got %v", flat[1])
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZInter(session, "MAX", nil, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat = result.([][]byte)
	if string(flat[1]) != "7" {
		t.Errorf("expected MAX score 7, got %v", flat[1])
	}
}

func TestZInterStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"), []byte("2"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("1"), []byte("b"), []byte("1"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, dest, []byte("9"), []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := ZInterStore(session, dest, "SUM", nil, z1, z2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected stored count 1, got %d", count)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, dest, []byte("b"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 3 {
		t.Errorf("expected b score 3 in dest, got %v", score)
	}
}

func TestZUnion(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"), []byte("2"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("3"), []byte("b"), []byte("4"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZUnion(session, "SUM", nil, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(flat))
	}
	for i := 0; i < len(flat); i += 2 {
		if string(flat[i]) == "b" {
			score, err := strconv.ParseFloat(string(flat[i+1]), 64)
			if err != nil {
				t.Fatal(err)
			}
			if score != 5 {
				t.Errorf("expected summed score 5 for b, got %v", score)
			}
		}
	}
}

func TestZUnionStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("2"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, dest, []byte("9"), []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := ZUnionStore(session, dest, "SUM", nil, z1, z2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected stored count 2, got %d", count)
	}
	if zcardOf(t, session, db, dest) != 2 {
		t.Error("expected dest card 2")
	}
}

func TestZInterWeights(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	z1 := []byte("z1")
	z2 := []byte("z2")

	err := db.Update(func(tx kv.Tx) error {
		op := ZAdd(session, z1, []byte("1"), []byte("a"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = ZAdd(session, z2, []byte("1"), []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZInter(session, "SUM", []float64{2}, true, z1, z2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 2 || string(flat[0]) != "a" {
		t.Fatalf("expected a with score, got %v", flat)
	}
	score, err := strconv.ParseFloat(string(flat[1]), 64)
	if err != nil {
		t.Fatal(err)
	}
	if score != 4 {
		t.Errorf("expected weighted score 4, got %v", score)
	}
}

func TestZRangeStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	src := []byte("src")
	dest := []byte("dest")

	mustAdd(t, session, db, src, []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"))

	var count any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRangeStore(session, dest, src, 0, 1)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected stored count 2, got %d", count)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, dest, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != 1 {
		t.Errorf("expected a score 1 in dest, got %v", score)
	}

	score, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, dest, []byte("c"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score != nil {
		t.Errorf("expected c absent from dest, got %v", score)
	}
}

func TestZRangeStoreEmptyRange(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	src := []byte("src")
	dest := []byte("dest")

	mustAdd(t, session, db, src, []byte("1"), []byte("a"))
	mustAdd(t, session, db, dest, []byte("9"), []byte("x"))

	var count any
	err := db.Update(func(tx kv.Tx) error {
		op := ZRangeStore(session, dest, src, 5, 9)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected stored count 0, got %d", count)
	}
	if zcardOf(t, session, db, dest) != 0 {
		t.Error("expected dest to be emptied")
	}
}

func TestNegativeAndInfScores(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("-1.5"), []byte("a"), []byte("0"), []byte("b"), []byte("2.25"), []byte("c"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZRange(session, key, 0, -1, true)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	flat := result.([][]byte)
	if len(flat) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(flat))
	}
	if string(flat[1]) != "-1.5" || string(flat[3]) != "0" || string(flat[5]) != "2.25" {
		t.Errorf("unexpected score ordering, got %v", flat)
	}

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if score.(float64) != -1.5 {
		t.Errorf("expected -1.5, got %v", score)
	}
}

func TestInfinityRoundTrip(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myzset")
	mustAdd(t, session, db, key, []byte("+inf"), []byte("a"), []byte("-inf"), []byte("b"))

	score, err := db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsInf(score.(float64), 1) {
		t.Errorf("expected +Inf, got %v", score)
	}

	score, err = db.Read(func(tx kv.Tx) (any, error) {
		op := ZScore(session, key, []byte("b"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsInf(score.(float64), -1) {
		t.Errorf("expected -Inf, got %v", score)
	}
}
