package json

import (
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
	"github.com/tidwall/redcon"
)

type mockConn struct {
	mu     sync.Mutex
	writes []string
	ctx    interface{}
}

func newMockConn() *mockConn {
	return &mockConn{ctx: common.NewSession(nil)}
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

func setJSONDoc(t *testing.T, db kv.KeyValueStore, session *common.Session, key []byte, val []byte) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(session.NewPublicEntry(key, val).Metadata(byte(common.RedisJSON)))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func runOp(t *testing.T, db kv.KeyValueStore, op common.QueuedOp, conn *mockConn) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		val, err := op.DbOp(tx)
		if err != nil {
			op.WireOp(conn, val, err)
			return err
		}
		op.WireOp(conn, val, nil)
		return nil
	})
	_ = err
}

func TestWireSetGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	conn := newMockConn()

	val := map[string]any{"name": "Alice", "age": 30.0}
	runOp(t, db, Set(session, []byte("doc"), "$", val, false, false, FphaNone), conn)
	if got := conn.writesStr(); got != "str:OK" {
		t.Fatalf("set: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Get(session, []byte("doc"), nil), conn)
	if got := conn.writesStr(); !strings.HasPrefix(got, "bulk:") || !strings.Contains(got, `"name":"Alice"`) {
		t.Fatalf("get whole doc: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Get(session, []byte("doc"), []string{"$.name"}), conn)
	if got := conn.writesStr(); got != `bulk:"Alice"` {
		t.Fatalf("get path: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Get(session, []byte("missing"), []string{"$.name"}), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("get missing key: %q", got)
	}
}

func TestWireGetMultiPath(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"a":1,"b":2}`))
	conn := newMockConn()

	runOp(t, db, Get(session, []byte("doc"), []string{"$.a", "$.b"}), conn)
	writes := conn.writesStr()
	if !strings.HasPrefix(writes, "bulk:") {
		t.Fatalf("multi-path get: %q", writes)
	}
	if !strings.Contains(writes, `"$.a":1`) || !strings.Contains(writes, `"$.b":2`) {
		t.Fatalf("multi-path get payload: %q", writes)
	}
}

func TestWireDel(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"a":1,"b":2}`))

	conn := newMockConn()
	runOp(t, db, Del(session, []byte("doc"), []string{"$.a"}), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("del path: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Del(session, []byte("doc"), nil), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("del whole: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Del(session, []byte("missing"), nil), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("del missing whole returns 1: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Del(session, []byte("missing"), []string{"$.a"}), conn)
	if got := conn.writesStr(); got != "int:0" {
		t.Fatalf("del missing path returns 0: %q", got)
	}
}

func TestWireType(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"s":"x","n":1,"arr":[1],"o":{"k":1},"b":true,"nil":null}`))

	cases := []struct {
		path string
		want string
	}{
		{"$.s", "string"},
		{"$.n", "number"},
		{"$.arr", "array"},
		{"$.o", "object"},
		{"$.b", "boolean"},
		{"$.nil", "null"},
		{"$", "object"},
	}
	for _, c := range cases {
		conn := newMockConn()
		runOp(t, db, Type(session, []byte("doc"), c.path), conn)
		if got := conn.writesStr(); got != "bulk:"+c.want {
			t.Fatalf("type %s: %q (want %q)", c.path, got, c.want)
		}
	}

	conn := newMockConn()
	runOp(t, db, Type(session, []byte("missing"), "$"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("type missing key: %q", got)
	}
}

func TestWireArrAppendIndexLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"arr":[1,2,3]}`))

	conn := newMockConn()
	runOp(t, db, ArrAppend(session, []byte("doc"), "$.arr", []any{4.0, 5.0}), conn)
	if got := conn.writesStr(); got != "int:5" {
		t.Fatalf("arrAppend: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrIndex(session, []byte("doc"), "$.arr", 4.0), conn)
	if got := conn.writesStr(); got != "int:3" {
		t.Fatalf("arrIndex hit: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrIndex(session, []byte("doc"), "$.arr", 99.0), conn)
	if got := conn.writesStr(); got != "int:-1" {
		t.Fatalf("arrIndex miss: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrLen(session, []byte("doc"), "$.arr"), conn)
	if got := conn.writesStr(); got != "int:5" {
		t.Fatalf("arrLen: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrLen(session, []byte("doc"), "$.nope"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("arrLen missing path: %q", got)
	}
}

func TestWireNumIncrByMultBy(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"n":10}`))

	conn := newMockConn()
	runOp(t, db, NumIncrBy(session, []byte("doc"), "$.n", 5), conn)
	if got := conn.writesStr(); got != "bulk:15" {
		t.Fatalf("numIncrBy: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, NumMultBy(session, []byte("doc"), "$.n", 3), conn)
	if got := conn.writesStr(); got != "bulk:45" {
		t.Fatalf("numMultBy: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, NumIncrBy(session, []byte("missing"), "$.n", 1), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("numIncrBy missing key: %q", got)
	}
}

func TestWireObjKeysLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"a":1,"b":2}`))

	conn := newMockConn()
	runOp(t, db, ObjKeys(session, []byte("doc"), "$"), conn)
	writes := conn.writesStr()
	if !strings.HasPrefix(writes, "array:2;bulk:a;bulk:b") {
		t.Fatalf("objKeys: %q", writes)
	}

	conn = newMockConn()
	runOp(t, db, ObjLen(session, []byte("doc"), "$"), conn)
	if got := conn.writesStr(); got != "int:2" {
		t.Fatalf("objLen: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ObjKeys(session, []byte("missing"), "$"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("objKeys missing key: %q", got)
	}
}

func TestWireStrAppendLen(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"s":"hello"}`))

	conn := newMockConn()
	runOp(t, db, StrAppend(session, []byte("doc"), "$.s", " world"), conn)
	if got := conn.writesStr(); got != "int:11" {
		t.Fatalf("strAppend: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, StrLen(session, []byte("doc"), "$.s"), conn)
	if got := conn.writesStr(); got != "int:11" {
		t.Fatalf("strLen: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, StrLen(session, []byte("missing"), "$"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("strLen missing key: %q", got)
	}
}

func TestWireMGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("k1"), []byte(`{"v":1}`))
	setJSONDoc(t, db, session, []byte("k2"), []byte(`{"v":2}`))

	conn := newMockConn()
	runOp(t, db, MGet(session, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}, "$.v"), conn)
	writes := conn.writesStr()
	if !strings.HasPrefix(writes, "array:3;") {
		t.Fatalf("mget: %q", writes)
	}
	if !strings.Contains(writes, "bulk:1") || !strings.Contains(writes, "bulk:2") {
		t.Fatalf("mget values: %q", writes)
	}
	if !strings.Contains(writes, "null:") {
		t.Fatalf("mget missing key should be null: %q", writes)
	}
}

func TestWireResp(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"n":1,"s":"x","b":true,"arr":[1],"nil":null}`))

	conn := newMockConn()
	runOp(t, db, Resp(session, []byte("doc"), "$.n"), conn)
	if got := conn.writesStr(); got != "bulk:1" {
		t.Fatalf("resp number: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Resp(session, []byte("doc"), "$.s"), conn)
	if got := conn.writesStr(); got != "bulk:x" {
		t.Fatalf("resp string: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Resp(session, []byte("doc"), "$.b"), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("resp bool: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Resp(session, []byte("doc"), "$.nil"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("resp null: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Resp(session, []byte("doc"), "$.nope"), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("resp missing path: %q", got)
	}
}

func TestWireClear(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"a":{"x":1},"arr":[1,2]}`))

	conn := newMockConn()
	runOp(t, db, Clear(session, []byte("doc"), "$.a"), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("clear obj: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Clear(session, []byte("doc"), "$.arr"), conn)
	if got := conn.writesStr(); got != "int:1" {
		t.Fatalf("clear array: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Clear(session, []byte("missing"), "$"), conn)
	if got := conn.writesStr(); got != "int:0" {
		t.Fatalf("clear missing key: %q", got)
	}
}

func TestWireArrPop(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"arr":[10,20,30]}`))

	conn := newMockConn()
	runOp(t, db, ArrPop(session, []byte("doc"), "$.arr", 1), conn)
	if got := conn.writesStr(); got != "bulk:20" {
		t.Fatalf("arrPop index: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrPop(session, []byte("doc"), "$.arr", -1), conn)
	if got := conn.writesStr(); got != "bulk:30" {
		t.Fatalf("arrPop negative: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrPop(session, []byte("empty"), "$", -1), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("arrPop missing key: %q", got)
	}
}

func TestWireArrTrim(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"arr":[1,2,3,4,5]}`))

	conn := newMockConn()
	runOp(t, db, ArrTrim(session, []byte("doc"), "$.arr", 1, 3), conn)
	if got := conn.writesStr(); got != "int:3" {
		t.Fatalf("arrTrim: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrTrim(session, []byte("missing"), "$.arr", 0, -1), conn)
	_ = conn.writesStr() // missing key: null via errSkip->writeJSONErr
}

func TestWireArrInsert(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	setJSONDoc(t, db, session, []byte("doc"), []byte(`{"arr":[1,3]}`))

	conn := newMockConn()
	runOp(t, db, ArrInsert(session, []byte("doc"), "$.arr", 1, []any{2.0}), conn)
	if got := conn.writesStr(); got != "int:3" {
		t.Fatalf("arrInsert: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, ArrInsert(session, []byte("doc"), "$.arr", 99, []any{2.0}), conn)
	if got := conn.writesStr(); got != "err:ERR err index out of range" {
		t.Fatalf("arrInsert out of range: %q", got)
	}
}

func TestWireSetNX(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	runOp(t, db, Set(session, []byte("doc"), "$", "first", true, false, FphaNone), conn)
	if got := conn.writesStr(); got != "str:OK" {
		t.Fatalf("set nx new: %q", got)
	}

	conn = newMockConn()
	runOp(t, db, Set(session, []byte("doc"), "$", "second", true, false, FphaNone), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("set nx existing: %q", got)
	}
}

func TestWireSetXX(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	runOp(t, db, Set(session, []byte("doc"), "$", "second", false, true, FphaNone), conn)
	if got := conn.writesStr(); got != "null:" {
		t.Fatalf("set xx missing: %q", got)
	}

	setJSONDoc(t, db, session, []byte("doc"), []byte(`"first"`))
	conn = newMockConn()
	runOp(t, db, Set(session, []byte("doc"), "$", "second", false, true, FphaNone), conn)
	if got := conn.writesStr(); got != "str:OK" {
		t.Fatalf("set xx existing: %q", got)
	}
}

func TestWireWrongType(t *testing.T) {
	session, db := testutil.NewTestSession(t)
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(session.NewPublicEntry([]byte("doc"), []byte("not json")).Metadata(byte(common.RedisList)))
	})
	if err != nil {
		t.Fatal(err)
	}

	conn := newMockConn()
	runOp(t, db, Get(session, []byte("doc"), nil), conn)
	if got := conn.writesStr(); got != "err:WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("wrong type: %q", got)
	}
}
