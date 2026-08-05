package strings

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

func setString(t *testing.T, db kv.KeyValueStore, session *common.Session, key []byte, val []byte) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(session.NewPublicEntry(key, val).Metadata(byte(common.RedisString)))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func setWrongType(t *testing.T, db kv.KeyValueStore, session *common.Session, key []byte) {
	t.Helper()
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(session.NewPublicEntry(key, []byte("v")).Metadata(byte(common.RedisList)))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func writeOp(t *testing.T, db kv.KeyValueStore, op common.QueuedOp) any {
	t.Helper()
	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func writeOpErr(t *testing.T, db kv.KeyValueStore, op common.QueuedOp) any {
	t.Helper()
	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = op.DbOp(tx)
		return err
	})
	if err == nil {
		t.Fatal("expected error from write op")
	}
	return result
}

func readOp(t *testing.T, db kv.KeyValueStore, op common.QueuedOp) any {
	t.Helper()
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func readOpErr(t *testing.T, db kv.KeyValueStore, op common.QueuedOp) any {
	t.Helper()
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		return op.DbOp(tx)
	})
	if err == nil {
		t.Fatal("expected error from read op")
	}
	return result
}

func expiresAt(t *testing.T, db kv.KeyValueStore, session *common.Session, key []byte) uint64 {
	t.Helper()
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey(key))
		if err != nil {
			return nil, err
		}
		return item.ExpiresAt(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.(uint64)
}

func TestSetGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	result := writeOp(t, db, Set(session, []byte("k"), []byte("v1")))
	Set(session, []byte("k"), []byte("v1")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "str:OK" {
		t.Errorf("set wire: got %q, want str:OK", got)
	}

	conn = newMockConn()
	result = readOp(t, db, Get(session, []byte("k")))
	Get(session, []byte("k")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "bulk:v1" {
		t.Errorf("get wire: got %q, want bulk:v1", got)
	}

	conn = newMockConn()
	result = readOp(t, db, Get(session, []byte("missing")))
	Get(session, []byte("missing")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "null:" {
		t.Errorf("get missing wire: got %q, want null:", got)
	}

	setWrongType(t, db, session, []byte("wt"))
	conn = newMockConn()
	result = readOpErr(t, db, Get(session, []byte("wt")))
	Get(session, []byte("wt")).WireOp(conn, result, errWrongType)
	want := "err:WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := conn.writesStr(); got != want {
		t.Errorf("get wrongtype wire: got %q, want %q", got, want)
	}
}

func TestSetEx(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	writeOp(t, db, SetEx(session, []byte("k"), []byte("v"), 100))
	if got := expiresAt(t, db, session, []byte("k")); got == 0 {
		t.Error("setex: expected TTL to be set")
	}
}

func TestPSetEx(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	result := writeOp(t, db, PSetEx(session, []byte("k"), []byte("v"), 5000))
	PSetEx(session, []byte("k"), []byte("v"), 5000).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "str:OK" {
		t.Errorf("psetex wire: got %q, want str:OK", got)
	}
	if got := expiresAt(t, db, session, []byte("k")); got == 0 {
		t.Error("psetex: expected TTL to be set")
	}
}

func TestGetSet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("old"))

	conn := newMockConn()
	result := writeOp(t, db, GetSet(session, []byte("k"), []byte("new")))
	GetSet(session, []byte("k"), []byte("new")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "bulk:old" {
		t.Errorf("getset wire: got %q, want bulk:old", got)
	}

	if got := string(readOp(t, db, Get(session, []byte("k"))).([]byte)); got != "new" {
		t.Errorf("getset: stored %q, want new", got)
	}

	conn = newMockConn()
	result = writeOp(t, db, GetSet(session, []byte("missing"), []byte("v")))
	GetSet(session, []byte("missing"), []byte("v")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "null:" {
		t.Errorf("getset missing wire: got %q, want null:", got)
	}
}

func TestGetDel(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("v"))

	conn := newMockConn()
	result := writeOp(t, db, GetDel(session, []byte("k")))
	GetDel(session, []byte("k")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "bulk:v" {
		t.Errorf("getdel wire: got %q, want bulk:v", got)
	}

	exists, err := db.Read(func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey([]byte("k")))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		return 1, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Error("getdel: key still exists after delete")
	}

	conn = newMockConn()
	result = writeOp(t, db, GetDel(session, []byte("missing")))
	GetDel(session, []byte("missing")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "null:" {
		t.Errorf("getdel missing wire: got %q, want null:", got)
	}
}

func TestStrlen(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("hello"))

	if got := readOp(t, db, Strlen(session, []byte("k"))); got != 5 {
		t.Errorf("strlen: got %d, want 5", got)
	}
	if got := readOp(t, db, Strlen(session, []byte("missing"))); got != 0 {
		t.Errorf("strlen missing: got %d, want 0", got)
	}

	setWrongType(t, db, session, []byte("wt"))
	readOpErr(t, db, Strlen(session, []byte("wt")))
}

func TestSubstr(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("hello"))

	tests := []struct {
		start, end int
		want       string
	}{
		{0, -1, "hello"},
		{1, 3, "ell"},
		{-3, -1, "llo"},
		{0, 100, "hello"},
		{10, 20, ""},
		{3, 1, ""},
		{0, -100, ""},
	}

	for _, tt := range tests {
		if got := string(readOp(t, db, Substr(session, []byte("k"), tt.start, tt.end)).([]byte)); got != tt.want {
			t.Errorf("substr(%d,%d): got %q, want %q", tt.start, tt.end, got, tt.want)
		}
	}

	if got := readOp(t, db, Substr(session, []byte("missing"), 0, -1)); len(got.([]byte)) != 0 {
		t.Errorf("substr missing: got %q, want empty", got)
	}
}

func TestSetNX(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	if got := writeOp(t, db, SetNX(session, []byte("k"), []byte("v1"))); got != 1 {
		t.Errorf("setnx on missing: got %d, want 1", got)
	}
	if got := writeOp(t, db, SetNX(session, []byte("k"), []byte("v2"))); got != 0 {
		t.Errorf("setnx on existing: got %d, want 0", got)
	}
	if got := string(readOp(t, db, Get(session, []byte("k"))).([]byte)); got != "v1" {
		t.Errorf("setnx: stored %q, want v1", got)
	}
}

func TestAppend(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	if got := writeOp(t, db, Append(session, []byte("k"), []byte("Hello"))); got != 5 {
		t.Errorf("append missing: got %d, want 5", got)
	}
	if got := writeOp(t, db, Append(session, []byte("k"), []byte(" World"))); got != 11 {
		t.Errorf("append: got %d, want 11", got)
	}

	setWrongType(t, db, session, []byte("wt"))
	writeOpErr(t, db, Append(session, []byte("wt"), []byte("x")))
}

func TestGetEx(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("v"))

	conn := newMockConn()
	result := writeOp(t, db, GetEx(session, []byte("k"), []byte("ex"), []byte("100")))
	GetEx(session, []byte("k"), []byte("ex"), []byte("100")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "bulk:v" {
		t.Errorf("getex wire: got %q, want bulk:v", got)
	}
	if got := expiresAt(t, db, session, []byte("k")); got == 0 {
		t.Error("getex ex: expected TTL to be set")
	}

	writeOpErr(t, db, GetEx(session, []byte("k"), []byte("bogus")))
	writeOpErr(t, db, GetEx(session, []byte("k"), []byte("px"), []byte("notanum")))
}

func TestIncrByFloat(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	if got := writeOp(t, db, IncrByFloat(session, []byte("k"), 5.5)); got != "5.5" {
		t.Errorf("incrbyfloat missing: got %q, want 5.5", got)
	}
	if got := writeOp(t, db, IncrByFloat(session, []byte("k"), 1.25)); got != "6.75" {
		t.Errorf("incrbyfloat: got %q, want 6.75", got)
	}

	setString(t, db, session, []byte("bad"), []byte("x"))
	writeOpErr(t, db, IncrByFloat(session, []byte("bad"), 1.0))

	setWrongType(t, db, session, []byte("wt"))
	writeOpErr(t, db, IncrByFloat(session, []byte("wt"), 1.0))
}

func TestMSet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	result := writeOp(t, db, MSet(session, []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")))
	MSet(session, []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "str:OK" {
		t.Errorf("mset wire: got %q, want str:OK", got)
	}

	if got := string(readOp(t, db, Get(session, []byte("k2"))).([]byte)); got != "v2" {
		t.Errorf("mset: stored %q, want v2", got)
	}
}

func TestMSetNX(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("existing"), []byte("v"))

	if got := writeOp(t, db, MSetNX(session, []byte("existing"), []byte("x"), []byte("new"), []byte("y"))); got != 0 {
		t.Errorf("msetnx with existing key: got %d, want 0", got)
	}

	newExists, err := db.Read(func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey([]byte("new")))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		return 1, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if newExists != 0 {
		t.Error("msetnx: key 'new' should not have been written")
	}

	if got := writeOp(t, db, MSetNX(session, []byte("a"), []byte("1"), []byte("b"), []byte("2"))); got != 1 {
		t.Errorf("msetnx all missing: got %d, want 1", got)
	}
}

func TestSetRange(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("Hello World"))

	if got := writeOp(t, db, SetRange(session, []byte("k"), 6, []byte("Redis"))); got != 11 {
		t.Errorf("setrange: got %d, want 11", got)
	}
	if got := string(readOp(t, db, Get(session, []byte("k"))).([]byte)); got != "Hello Redis" {
		t.Errorf("setrange: got %q, want Hello Redis", got)
	}

	if got := writeOp(t, db, SetRange(session, []byte("new"), 3, []byte("xyz"))); got != 6 {
		t.Errorf("setrange missing: got %d, want 6", got)
	}

	setWrongType(t, db, session, []byte("wt"))
	writeOpErr(t, db, SetRange(session, []byte("wt"), 0, []byte("x")))
}

func TestIncrement(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	conn := newMockConn()
	result := writeOp(t, db, Increment(session, []byte("k"), 1))
	Increment(session, []byte("k"), 1).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "int64:1" {
		t.Errorf("incr wire: got %q, want int64:1", got)
	}

	if got := writeOp(t, db, Increment(session, []byte("k"), -1)); got != int64(0) {
		t.Errorf("decr: got %d, want 0", got)
	}

	setString(t, db, session, []byte("bad"), []byte("notanumber"))
	writeOpErr(t, db, Increment(session, []byte("bad"), 1))

	setWrongType(t, db, session, []byte("wt"))
	writeOpErr(t, db, Increment(session, []byte("wt"), 1))
}

func TestWireErr(t *testing.T) {
	conn := newMockConn()
	Increment(common.NewSession(nil), []byte("k"), 1).WireOp(conn, nil, errWrongType)
	if got := conn.writesStr(); got != "err:"+errWrongType.Error() {
		t.Errorf("wrongtype wire: got %q, want %q", got, "err:"+errWrongType.Error())
	}
}
