package zset

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
	"github.com/tidwall/redcon"
)

// mockConn is a minimal redcon.Conn that records all writes for inspection in tests.
type mockConn struct {
	mu     sync.Mutex
	writes []string
	ctx    interface{}
}

func newMockConn(session *common.Session) *mockConn {
	return &mockConn{ctx: session}
}

func (c *mockConn) writesStr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.writes, ";")
}

func (c *mockConn) RemoteAddr() string                   { return "127.0.0.1:0" }
func (c *mockConn) Close() error                         { return nil }
func (c *mockConn) WriteError(msg string)                { c.record("err", msg) }
func (c *mockConn) WriteString(str string)               { c.record("str", str) }
func (c *mockConn) WriteBulk(bulk []byte)                { c.record("bulk", string(bulk)) }
func (c *mockConn) WriteBulkString(bulk string)          { c.record("bulk", bulk) }
func (c *mockConn) WriteBulkFrom(num int64, r io.Reader) {}
func (c *mockConn) WriteInt(num int)                     { c.record("int", strconv.Itoa(num)) }
func (c *mockConn) WriteInt64(num int64)                 { c.record("int64", strconv.FormatInt(num, 10)) }
func (c *mockConn) WriteUint64(num uint64)               { c.record("uint64", strconv.FormatUint(num, 10)) }
func (c *mockConn) WriteArray(count int)                 { c.record("array", strconv.Itoa(count)) }
func (c *mockConn) WriteNull()                           { c.record("null", "") }
func (c *mockConn) WriteRaw(data []byte)                 { c.record("raw", string(data)) }
func (c *mockConn) WriteAny(v interface{})               {}
func (c *mockConn) Context() interface{}                 { return c.ctx }
func (c *mockConn) SetContext(v interface{})             { c.ctx = v }
func (c *mockConn) SetReadBuffer(bytes int)              {}
func (c *mockConn) Detach() redcon.DetachedConn          { return nil }
func (c *mockConn) ReadPipeline() []redcon.Command       { return nil }
func (c *mockConn) PeekPipeline() []redcon.Command       { return nil }
func (c *mockConn) NetConn() net.Conn                    { return nil }

func (c *mockConn) record(kind, payload string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, kind+":"+payload)
}

// zaddCount extracts the integer count from a zaddResult returned by ZAdd.DbOp.
func zaddCount(v any) int {
	return v.(zaddResult).count
}

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
	if zaddCount(added) != 3 {
		t.Errorf("expected 3 added, got %v", added)
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
	if zaddCount(added) != 1 {
		t.Errorf("expected 1 added, got %v", added)
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
	if zaddCount(added) != 1 {
		t.Errorf("expected 1 added, got %v", added)
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
	if zaddCount(added) != 0 {
		t.Errorf("expected 0 added, got %v", added)
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
	if zaddCount(added) != 1 {
		t.Errorf("expected 1 changed with CH, got %v", added)
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

// newRegistry creates a fresh WatchRegistry for tests that exercise the registry
// directly, isolated from the global one.
func newRegistry() *common.WatchRegistry {
	return common.NewWatchRegistry()
}

// TestBZPopMinServedByZAdd verifies that a single BZPOPMIN waiter is woken when a
// ZADD writes to the same key.
func TestBZPopMinServedByZAdd(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	registry := newRegistry()

	key := []byte("mykey")
	publicKey := string(session.PublicKey(key))

	// Start a waiter in a background goroutine.
	resultCh := make(chan common.PopResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, ok := registry.Block(ctx, []string{publicKey}, true)
		if ok {
			resultCh <- res
		} else {
			resultCh <- common.PopResult{} // timed out
		}
	}()

	// Give the goroutine time to register.
	time.Sleep(20 * time.Millisecond)

	// ZADD — must claim the waiter inside DbOp and wake in WireOp.
	err := db.Update(func(tx kv.Tx) error {
		claim := registry.TryClaim(publicKey)
		if claim == nil {
			t.Error("expected a waiter to claim")
			return nil
		}
		// Simulate the write.
		if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, 1.5, []byte("a")), nil)); err != nil {
			registry.ReleaseFront(claim)
			return err
		}
		if err := tx.Set(session.NewPrivateEntry(memberCompound(key, []byte("a")), scoreBytes(1.5))); err != nil {
			registry.ReleaseFront(claim)
			return err
		}
		if err := common.WriteUint32Sentinel(tx, session, key, 1, common.RedisSortedSet); err != nil {
			registry.ReleaseFront(claim)
			return err
		}
		claim.SetResult(common.PopResult{Key: string(key), Member: []byte("a"), Score: 1.5})
		// Wake only after we know Commit will succeed (simulated: we commit in Update).
		// In real code Wake is called from WireOp; here we call it post-commit via
		// a deferred-after-return pattern.
		go func() { claim.Wake() }()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-resultCh:
		if res.Key != string(key) || string(res.Member) != "a" || res.Score != 1.5 {
			t.Errorf("unexpected result: %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter to be served")
	}
}

// TestBZPopMinFIFO verifies that when two waiters are blocked on the same key, the
// first-registered waiter (longest-waiting) is always served first.
func TestBZPopMinFIFO(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	registry := newRegistry()

	key := []byte("fifokey")
	publicKey := string(session.PublicKey(key))

	order := make([]int, 0, 2)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 1; i <= 2; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			res, ok := registry.Block(ctx, []string{publicKey}, true)
			if ok {
				mu.Lock()
				order = append(order, i)
				_ = res
				mu.Unlock()
			}
		}()
		time.Sleep(15 * time.Millisecond) // ensure ordering
	}

	// Two ZADDs to serve the two waiters in order.
	for j := 0; j < 2; j++ {
		scoreVal := float64(j + 1)
		member := []byte{byte('x' + j)}
		err := db.Update(func(tx kv.Tx) error {
			if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, scoreVal, member), nil)); err != nil {
				return err
			}
			if err := tx.Set(session.NewPrivateEntry(memberCompound(key, member), scoreBytes(scoreVal))); err != nil {
				return err
			}
			count := uint32(1)
			if err := common.WriteUint32Sentinel(tx, session, key, count, common.RedisSortedSet); err != nil {
				return err
			}
			claim := registry.TryClaim(publicKey)
			if claim == nil {
				return nil
			}
			claim.SetResult(common.PopResult{Key: string(key), Member: member, Score: scoreVal})
			go func() { claim.Wake() }()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Errorf("expected FIFO order [1 2], got %v", order)
	}
}

// TestBZPopMinTimeout verifies that a waiter that receives no write within the timeout
// period gets a "not ok" return and no result.
func TestBZPopMinTimeout(t *testing.T) {
	registry := newRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, ok := registry.Block(ctx, []string{"nosuchkey"}, true)
	if ok {
		t.Error("expected timeout (ok=false), but got ok=true")
	}
}

// TestBZPopMinMultiKey verifies multi-key registration: the waiter can be found under
// any of its registered keys, and once claimed it is removed from all of them.
func TestBZPopMinMultiKey(t *testing.T) {
	registry := newRegistry()

	keys := []string{"k1", "k2", "k3"}

	resultCh := make(chan common.PopResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, ok := registry.Block(ctx, keys, true)
		if ok {
			resultCh <- res
		}
	}()

	time.Sleep(20 * time.Millisecond)

	// Claim via the second key.
	claim := registry.TryClaim("k2")
	if claim == nil {
		t.Fatal("expected claim on k2")
	}
	claim.SetResult(common.PopResult{Key: "k2", Member: []byte("m"), Score: 7})
	claim.Wake()

	select {
	case res := <-resultCh:
		if res.Key != "k2" || string(res.Member) != "m" || res.Score != 7 {
			t.Errorf("unexpected result: %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	// After the claim, the waiter must have been removed from all keys.
	for _, k := range keys {
		if c := registry.TryClaim(k); c != nil {
			t.Errorf("expected no waiter left on %q after claim, but got one", k)
		}
	}
}

// TestDispatchCompensation verifies that a claim made by an earlier DbOp is released
// back to the front of the queue when a later DbOp in the same batch fails.
//
// This is a unit test for the compensation path in DispatchPendingOps.  It runs the
// DbOps manually to simulate what DispatchPendingOps does.
func TestDispatchCompensation(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	registry := newRegistry()

	key := []byte("compkey")
	publicKey := string(session.PublicKey(key))

	// Register a waiter.
	resultCh := make(chan common.PopResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, ok := registry.Block(ctx, []string{publicKey}, true)
		if ok {
			resultCh <- res
		}
	}()
	time.Sleep(20 * time.Millisecond)

	// Simulate a batch where op0 claims the waiter successfully, but op1 fails.
	// DispatchPendingOps would release claims from op0 on the op1 error path.
	var claimedInOp0 *common.Claim

	err := db.Update(func(tx kv.Tx) error {
		// Op0: write a member and claim the waiter.
		if err := tx.Set(session.NewPrivateEntry(scoreCompound(key, 10, []byte("z")), nil)); err != nil {
			return err
		}
		if err := tx.Set(session.NewPrivateEntry(memberCompound(key, []byte("z")), scoreBytes(10))); err != nil {
			return err
		}
		if err := common.WriteUint32Sentinel(tx, session, key, 1, common.RedisSortedSet); err != nil {
			return err
		}
		claimedInOp0 = registry.TryClaim(publicKey)
		if claimedInOp0 != nil {
			claimedInOp0.SetResult(common.PopResult{Key: string(key), Member: []byte("z"), Score: 10})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if claimedInOp0 == nil {
		t.Fatal("expected op0 to claim the waiter")
	}

	// Simulate op1 failing: release op0's claims back to the front.
	registry.ReleaseFront(claimedInOp0)

	// The waiter must still be blockable — it should still be at the front.
	claim2 := registry.TryClaim(publicKey)
	if claim2 == nil {
		t.Fatal("expected waiter to be back at front after ReleaseFront")
	}
	claim2.SetResult(common.PopResult{Key: string(key), Member: []byte("z"), Score: 99})
	claim2.Wake()

	select {
	case res := <-resultCh:
		if res.Score != 99 {
			t.Errorf("expected score 99 from re-claimed result, got %v", res.Score)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-claimed waiter")
	}
}

// TestBZPopInMultiNonBlocking verifies that BZPOPMIN/BZPOPMAX inside a MULTI block
// does not block: when the key is empty it replies with null immediately, and when
// the key has data it pops and replies normally.
func TestBZPopInMultiNonBlocking(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	key := []byte("multikey")

	// Verify ShouldBlock is false inside MULTI.
	session.EnterMulti()
	if session.ShouldBlock() {
		t.Fatal("ShouldBlock() should return false inside MULTI")
	}

	// Empty key — must reply null immediately without blocking.
	conn := newMockConn(session)
	done := make(chan struct{})
	go func() {
		BZPopMin(session, conn, [][]byte{key}, 30) // 30s timeout — must NOT honour it
		close(done)
	}()

	select {
	case <-done:
		// good — returned immediately
	case <-time.After(500 * time.Millisecond):
		t.Fatal("BZPopMin inside MULTI blocked instead of returning immediately")
	}

	if !strings.Contains(conn.writesStr(), "null") {
		t.Errorf("expected null reply for empty key in MULTI, got %q", conn.writesStr())
	}

	// Pre-populate the key, then verify a successful non-blocking pop.
	session.ExitMulti(true)
	mustAdd(t, session, db, key, []byte("5"), []byte("m"))
	session.EnterMulti()

	conn2 := newMockConn(session)
	done2 := make(chan struct{})
	go func() {
		BZPopMin(session, conn2, [][]byte{key}, 30)
		close(done2)
	}()

	select {
	case <-done2:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("BZPopMin inside MULTI blocked on a non-empty key")
	}

	w := conn2.writesStr()
	if !strings.Contains(w, "array:3") || !strings.Contains(w, "bulk:m") {
		t.Errorf("expected 3-element pop reply in MULTI, got %q", w)
	}

	session.ExitMulti(true)
}

// TestBZPopInScriptNonBlocking verifies the identical degradation for the inScript
// flag, exercising the shared ShouldBlock() path.
func TestBZPopInScriptNonBlocking(t *testing.T) {
	session, _ := testutil.NewTestSession(t)
	key := []byte("scriptkey")

	session.EnterScript()
	if session.ShouldBlock() {
		t.Fatal("ShouldBlock() should return false inside a script")
	}

	conn := newMockConn(session)
	done := make(chan struct{})
	go func() {
		BZPopMin(session, conn, [][]byte{key}, 0) // indefinite timeout — must NOT honour it
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("BZPopMin inside script blocked instead of returning immediately")
	}

	if !strings.Contains(conn.writesStr(), "null") {
		t.Errorf("expected null reply for empty key in script, got %q", conn.writesStr())
	}

	session.ExitScript()

	// After ExitScript the flag is cleared and ShouldBlock is true again.
	if !session.ShouldBlock() {
		t.Fatal("ShouldBlock() should return true after ExitScript")
	}
}

// TestShouldBlockStates exhaustively checks the four combinations of inMulti/inScript.
func TestShouldBlockStates(t *testing.T) {
	session, _ := testutil.NewTestSession(t)

	if !session.ShouldBlock() {
		t.Error("fresh session: ShouldBlock() should be true")
	}

	session.EnterMulti()
	if session.ShouldBlock() {
		t.Error("inMulti only: ShouldBlock() should be false")
	}
	session.ExitMulti(true)

	session.EnterScript()
	if session.ShouldBlock() {
		t.Error("inScript only: ShouldBlock() should be false")
	}
	session.ExitScript()

	session.EnterMulti()
	session.EnterScript()
	if session.ShouldBlock() {
		t.Error("inMulti+inScript: ShouldBlock() should be false")
	}
	session.ExitScript()
	session.ExitMulti(true)

	if !session.ShouldBlock() {
		t.Error("after both cleared: ShouldBlock() should be true again")
	}
}
