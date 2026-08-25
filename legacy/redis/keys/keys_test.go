package keys

import (
	"fmt"
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

func TestExists(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k1"), []byte("v1"))
	setString(t, db, session, []byte("k2"), []byte("v2"))

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		return Exists(session, []byte("missing")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 0 {
		t.Errorf("exists on missing key: got %d, want 0", result)
	}

	result, err = db.Read(func(tx kv.Tx) (any, error) {
		return Exists(session, []byte("k1"), []byte("missing"), []byte("k2")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 2 {
		t.Errorf("exists on mixed keys: got %d, want 2", result)
	}
}

func TestMGet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k1"), []byte("v1"))
	setString(t, db, session, []byte("k2"), []byte(""))

	conn := newMockConn()
	result, err := db.Read(func(tx kv.Tx) (any, error) {
		return MGet(session, []byte("k1"), []byte("missing"), []byte("k2")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}

	vals := result.([]any)
	if len(vals) != 3 {
		t.Fatalf("mget returned %d values, want 3", len(vals))
	}
	if string(vals[0].([]byte)) != "v1" {
		t.Errorf("vals[0]: got %q, want v1", vals[0])
	}
	if vals[1] != nil {
		t.Errorf("vals[1]: got %q, want null", vals[1])
	}
	if string(vals[2].([]byte)) != "" {
		t.Errorf("vals[2]: got %q, want empty string preserved", vals[2])
	}

	MGet(session, []byte("k1"), []byte("missing")).WireOp(conn, result, nil)
	want := "array:3;bulk:v1;null:;bulk:"
	if got := conn.writesStr(); got != want {
		t.Errorf("mget wire: got %q, want %q", got, want)
	}
}

func TestMove(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	targetDb := 5

	setString(t, db, session, []byte("k1"), []byte("v1"))

	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Move(session, []byte("k1"), targetDb).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 1 {
		t.Fatalf("move existing: got %d, want 1", result)
	}

	missing, err := db.Read(func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey([]byte("k1")))
		if err == kv.ErrKeyNotFound {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !missing.(bool) {
		t.Error("source key still exists after move")
	}

	targetSession := common.NewSession(db)
	targetSession.SwitchDB(targetDb)

	val, err := db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(targetSession.PublicKey([]byte("k1")))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("target key missing after move: %v", err)
	}
	if string(val.([]byte)) != "v1" {
		t.Errorf("moved value: got %q, want v1", val)
	}

	err = db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Move(session, []byte("missing"), targetDb).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 0 {
		t.Errorf("move missing key: got %d, want 0", result)
	}
}

func TestRename(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("old"), []byte("value"))

	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Rename(session, []byte("old"), []byte("new")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "OK" {
		t.Fatalf("rename result: got %v, want OK", result)
	}

	val, err := db.Read(func(tx kv.Tx) (any, error) {
		item, err := tx.Get(session.PublicKey([]byte("new")))
		if err != nil {
			return nil, err
		}
		return item.Value()
	})
	if err != nil {
		t.Fatalf("renamed key missing: %v", err)
	}
	if string(val.([]byte)) != "value" {
		t.Errorf("renamed value: got %q, want value", val)
	}

	conn := newMockConn()
	err = db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Rename(session, []byte("missing"), []byte("other")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	Rename(session, []byte("missing"), []byte("other")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "err:ERR no such key" {
		t.Errorf("rename missing wire: got %q, want ERR no such key", got)
	}
}

func TestRenameNX(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("old"), []byte("value"))
	setString(t, db, session, []byte("new"), []byte("existing"))

	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = RenameNX(session, []byte("old"), []byte("new")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 0 {
		t.Errorf("renamenx with existing target: got %d, want 0", result)
	}

	err = db.Update(func(tx kv.Tx) error {
		var err error
		result, err = RenameNX(session, []byte("old"), []byte("target")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 1 {
		t.Errorf("renamenx success: got %d, want 1", result)
	}

	conn := newMockConn()
	err = db.Update(func(tx kv.Tx) error {
		var err error
		result, err = RenameNX(session, []byte("missing"), []byte("fresh-target")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	RenameNX(session, []byte("missing"), []byte("fresh-target")).WireOp(conn, result, nil)
	if got := conn.writesStr(); got != "err:no such key" {
		t.Errorf("renamenx missing old wire: got %q, want bare no such key", got)
	}
}

func TestExpireAndTTL(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k"), []byte("v"))
	setString(t, db, session, []byte("k2"), []byte("v"))

	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Expire(session, []byte("k"), 100).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 1 {
		t.Fatalf("expire: got %d, want 1", result)
	}

	ttl, err := db.Read(func(tx kv.Tx) (any, error) {
		return TTL(session, []byte("k")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ttl.(int) <= 0 || ttl.(int) > 100 {
		t.Errorf("ttl: got %d, want in (0, 100]", ttl)
	}

	pttl, err := db.Read(func(tx kv.Tx) (any, error) {
		return PTTL(session, []byte("k")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if pttl.(int64) <= 0 || pttl.(int64) > 100000 {
		t.Errorf("pttl: got %d, want in (0, 100000]", pttl)
	}

	noTTL, err := db.Read(func(tx kv.Tx) (any, error) {
		return TTL(session, []byte("k2")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if noTTL != -1 {
		t.Errorf("ttl on key without expiry: got %d, want -1", noTTL)
	}

	missingTTL, err := db.Read(func(tx kv.Tx) (any, error) {
		return TTL(session, []byte("missing")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if missingTTL != -2 {
		t.Errorf("ttl on missing key: got %d, want -2", missingTTL)
	}

	missingPTTL, err := db.Read(func(tx kv.Tx) (any, error) {
		return PTTL(session, []byte("missing")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if missingPTTL != int64(-2) {
		t.Errorf("pttl on missing key: got %d, want -2", missingPTTL)
	}

	var expired any
	err = db.Update(func(tx kv.Tx) error {
		var err error
		expired, err = Expire(session, []byte("missing"), 100).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if expired != 0 {
		t.Errorf("expire on missing key: got %d, want 0", expired)
	}
}

func TestType(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	meta := map[common.RedisValueType]string{
		common.RedisString:          "string",
		common.RedisList:            "list",
		common.RedisSet:             "set",
		common.RedisSortedSet:       "zset",
		common.RedisHash:            "hash",
		common.RedisStream:          "stream",
		common.RedisVectorSet:       "vectorset",
		common.RedisBloom:           "bloom",
		common.RedisJSON:            "json",
		common.RedisValueType(0xFF): "unknown",
	}

	i := 0
	for typ, want := range meta {
		key := []byte(fmt.Sprintf("key%d", i))
		i++
		m := byte(typ)
		err := db.Update(func(tx kv.Tx) error {
			return tx.Set(session.NewPublicEntry(key, []byte("v")).Metadata(m))
		})
		if err != nil {
			t.Fatal(err)
		}

		result, err := db.Read(func(tx kv.Tx) (any, error) {
			return Type(session, key).DbOp(tx)
		})
		if err != nil {
			t.Fatal(err)
		}
		if result != want {
			t.Errorf("type for meta %#x: got %q, want %q", m, result, want)
		}
	}

	none, err := db.Read(func(tx kv.Tx) (any, error) {
		return Type(session, []byte("missing")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if none != "none" {
		t.Errorf("type on missing key: got %q, want none", none)
	}
}

func TestDel(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	setString(t, db, session, []byte("k1"), []byte("v1"))
	setString(t, db, session, []byte("k2"), []byte("v2"))

	var result any
	err := db.Update(func(tx kv.Tx) error {
		var err error
		result, err = Del(session, []byte("k1"), []byte("missing"), []byte("k2")).DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 2 {
		t.Errorf("del: got %d, want 2", result)
	}

	remaining, err := db.Read(func(tx kv.Tx) (any, error) {
		return Exists(session, []byte("k1"), []byte("k2")).DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Errorf("keys still exist after del: got %d, want 0", remaining)
	}
}

func TestWireErr(t *testing.T) {
	conn := newMockConn()
	Expire(common.NewSession(nil), []byte("k"), 10).WireOp(conn, nil, fmt.Errorf("boom"))
	if got := conn.writesStr(); got != "err:ERR boom" {
		t.Errorf("error wire: got %q, want err:ERR boom", got)
	}
}
