package server

import (
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hardpointlabs/invar/redis/common"
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

func cmd(args ...string) redcon.Command {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return redcon.Command{Args: raw}
}

func TestInfo(t *testing.T) {
	c := newMockConn()
	Info(c.ctx.(*common.Session), c)
	out := c.writesStr()
	for _, want := range []string{
		"# Server",
		"redis_version:6.2.0",
		"invar_version:dev",
		"redis_mode:standalone",
		"maxmemory_policy:noeviction",
		"loading:0",
		"role:master",
		"# Keyspace",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("INFO output missing %q", want)
		}
	}
	if !strings.HasPrefix(out, "bulk:# Server") {
		t.Errorf("INFO should be written as a bulk string, got %q", out)
	}
}

func TestHelloNoArgs(t *testing.T) {
	c := newMockConn()
	Hello(c.ctx.(*common.Session), c, cmd("HELLO"))
	out := c.writesStr()
	if !strings.Contains(out, "array:14") {
		t.Errorf("expected RESP2 map reply of 14 elements, got %q", out)
	}
	for _, want := range []string{"bulk:server", "bulk:redis", "bulk:proto", "int:2", "bulk:modules", "array:0"} {
		if !strings.Contains(out, want) {
			t.Errorf("HELLO reply missing %q", want)
		}
	}
}

func TestHelloRejectsResp3(t *testing.T) {
	c := newMockConn()
	Hello(c.ctx.(*common.Session), c, cmd("HELLO", "3"))
	out := c.writesStr()
	if !strings.Contains(out, "err:NOPROTO unsupported protocol version") {
		t.Errorf("expected NOPROTO error for RESP3, got %q", out)
	}
}

func TestHelloSetsNameAndIgnoresAuth(t *testing.T) {
	s := common.NewSession(nil)
	c := newMockConn()
	Hello(s, c, cmd("HELLO", "2", "AUTH", "default", "pw", "SETNAME", "bull:conn"))
	if s.ClientName != "bull:conn" {
		t.Errorf("SETNAME did not stick: %q", s.ClientName)
	}
	if !strings.Contains(c.writesStr(), "int:2") {
		t.Errorf("expected proto 2 reply, got %q", c.writesStr())
	}
}

func TestClientSetNameGetName(t *testing.T) {
	s := common.NewSession(nil)
	c := newMockConn()
	Client(s, c, cmd("CLIENT", "SETNAME", "worker-1"))
	if got := c.writesStr(); got != "str:OK" {
		t.Errorf("SETNAME should reply OK, got %q", got)
	}
	c = newMockConn()
	Client(s, c, cmd("CLIENT", "GETNAME"))
	if got := c.writesStr(); got != "bulk:worker-1" {
		t.Errorf("GETNAME should reply the name, got %q", got)
	}
}

func TestClientGetNameUnset(t *testing.T) {
	c := newMockConn()
	Client(c.ctx.(*common.Session), c, cmd("CLIENT", "GETNAME"))
	if got := c.writesStr(); got != "null:" {
		t.Errorf("GETNAME on unnamed connection should be nil, got %q", got)
	}
}

func TestClientSetInfo(t *testing.T) {
	s := common.NewSession(nil)
	c := newMockConn()
	Client(s, c, cmd("CLIENT", "SETINFO", "LIB-NAME", "ioredis"))
	Client(s, c, cmd("CLIENT", "SETINFO", "LIB-VER", "6.0.0"))
	if s.LibName != "ioredis" || s.LibVer != "6.0.0" {
		t.Errorf("SETINFO did not stick: %q %q", s.LibName, s.LibVer)
	}
	c = newMockConn()
	Client(s, c, cmd("CLIENT", "GETINFO", "LIB-VER"))
	if got := c.writesStr(); got != "bulk:6.0.0" {
		t.Errorf("GETINFO LIB-VER should reply version, got %q", got)
	}
}

func TestClientId(t *testing.T) {
	s := common.NewSession(nil)
	c := newMockConn()
	Client(s, c, cmd("CLIENT", "ID"))
	want := "uint64:" + strconv.FormatUint(s.Id, 10)
	if got := c.writesStr(); got != want {
		t.Errorf("CLIENT ID should reply %s, got %q", want, got)
	}
}

func TestClientInfo(t *testing.T) {
	s := common.NewSession(nil)
	s.ClientName = "conn-1"
	s.LibName = "ioredis"
	s.LibVer = "6.0.0"
	c := newMockConn()
	Client(s, c, cmd("CLIENT", "INFO"))
	out := c.writesStr()
	for _, want := range []string{"bulk:id=", "name=conn-1", "lib-name=ioredis", "lib-ver=6.0.0", "db=0"} {
		if !strings.Contains(out, want) {
			t.Errorf("CLIENT INFO missing %q in %q", want, out)
		}
	}
}

func TestClientSetNameRejectsSpaces(t *testing.T) {
	c := newMockConn()
	Client(c.ctx.(*common.Session), c, cmd("CLIENT", "SETNAME", "bad name"))
	if got := c.writesStr(); !strings.Contains(got, "err:") {
		t.Errorf("SETNAME with spaces should error, got %q", got)
	}
}
