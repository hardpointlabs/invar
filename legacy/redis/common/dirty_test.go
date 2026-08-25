package common

import (
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/tidwall/redcon"
)

type fakeKvs struct{ kv.KeyValueStore }

type fakeConn struct {
	redcon.Conn
	replies []string
}

func (f *fakeConn) WriteString(s string)             { f.replies = append(f.replies, "str:"+s) }
func (f *fakeConn) WriteError(s string)              { f.replies = append(f.replies, "err:"+s) }
func (f *fakeConn) WriteBulkString(s string)         { f.replies = append(f.replies, "bulkstr:"+s) }

func TestDirtyExec(t *testing.T) {
	s := NewSession(fakeKvs{})
	conn := &fakeConn{}
	wrapped := s.TrackedConn(conn)

	s.EnterMulti()
	wrapped.WriteError("ERR unknown command 'NOTACOMMAND'")
	if !s.dirtyExec {
		t.Fatalf("expected dirtyExec true after queued error")
	}
	if err := s.ExitMulti(false); err != nil {
		t.Fatalf("ExitMulti: %v", err)
	}
	s.DispatchPendingOps(wrapped, true)
	want := []string{
		"err:ERR unknown command 'NOTACOMMAND'",
		"err:EXECABORT Transaction discarded because of previous errors.",
	}
	if len(conn.replies) != len(want) {
		t.Fatalf("got replies %v", conn.replies)
	}
	for i := range want {
		if conn.replies[i] != want[i] {
			t.Fatalf("reply %d: got %q want %q (all: %v)", i, conn.replies[i], want[i], conn.replies)
		}
	}
}
