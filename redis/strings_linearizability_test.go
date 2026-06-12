package redis

// Linearizability checks for the basic string get/set operations.
//
// We use Porcupine (https://github.com/anishathalye/porcupine) to verify that
// the observed histories produced by concurrent callers of setKey / getKey are
// consistent with a sequentially-correct key-value register model.
//
// The guarantees modelled here follow directly from BadgerDB's serializable
// read-write transactions (db.Update) and read-only snapshots (db.View):
//   - Every setKey completes an atomic committed write; no torn writes.
//   - Every getKey reads from a consistent snapshot taken at the start of the
//     transaction; it cannot observe a partially-written value.
//   - Because BadgerDB serialises concurrent writes with MVCC, the full
//     execution is serialisable, which implies linearizability for
//     single-object operations.
//
// Model (per key, Porcupine partition):
//   state  = string  (current value; "" means the key does not exist / is nil)
//   input  = strInput{op, key, value}
//   output = strOutput{value}
//
// Step semantics:
//   set(key, value)  → always legal; new state = value
//   get(key)         → legal iff output.value == state; state unchanged

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

// ---------------------------------------------------------------------------
// Mock redcon.Conn
// ---------------------------------------------------------------------------

// mockConn is a minimal in-memory implementation of redcon.Conn.
// It records the most recent response written so the caller can inspect it.
type mockConn struct {
	mu      sync.Mutex
	last    string // last non-null response written
	wasNull bool   // true if the last write was WriteNull
	wasErr  bool   // true if the last write was WriteError
	ctx     interface{}
}

func newMockConn() *mockConn {
	c := &mockConn{}
	c.ctx = &ClientInfo{Id: uniqueID()}
	return c
}

var connIDCounter uint64

func uniqueID() uint64 {
	return atomic.AddUint64(&connIDCounter, 1)
}

func (c *mockConn) result() (value string, isNull bool, isErr bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.wasNull, c.wasErr
}

func (c *mockConn) RemoteAddr() string          { return "127.0.0.1:0" }
func (c *mockConn) Close() error                { return nil }
func (c *mockConn) WriteError(msg string)       { c.mu.Lock(); c.last = msg; c.wasNull = false; c.wasErr = true; c.mu.Unlock() }
func (c *mockConn) WriteString(str string)      { c.mu.Lock(); c.last = str; c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteBulk(bulk []byte)       { c.mu.Lock(); c.last = string(bulk); c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteBulkString(bulk string) { c.mu.Lock(); c.last = bulk; c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteInt(num int)            { c.mu.Lock(); c.last = fmt.Sprintf("%d", num); c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteInt64(num int64)        { c.mu.Lock(); c.last = fmt.Sprintf("%d", num); c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteUint64(num uint64)      { c.mu.Lock(); c.last = fmt.Sprintf("%d", num); c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteArray(count int)        {}
func (c *mockConn) WriteNull()                  { c.mu.Lock(); c.last = ""; c.wasNull = true; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteRaw(data []byte)        { c.mu.Lock(); c.last = string(data); c.wasNull = false; c.wasErr = false; c.mu.Unlock() }
func (c *mockConn) WriteAny(any interface{})    {}
func (c *mockConn) Context() interface{}        { return c.ctx }
func (c *mockConn) SetContext(v interface{})    { c.ctx = v }
func (c *mockConn) SetReadBuffer(bytes int)     {}
func (c *mockConn) Detach() redcon.DetachedConn { return nil }
func (c *mockConn) ReadPipeline() []redcon.Command { return nil }
func (c *mockConn) PeekPipeline() []redcon.Command { return nil }
func (c *mockConn) NetConn() net.Conn           { return nil }

// ---------------------------------------------------------------------------
// Porcupine model for a single string register (GET / SET)
// ---------------------------------------------------------------------------

type strOp int

const (
	opGet strOp = iota
	opSet
)

// strInput describes one operation on the KV store.
// key   – the Redis key being operated on (always present; used for partitioning)
// value – the value to write (only meaningful for opSet)
type strInput struct {
	op    strOp
	key   string
	value string
}

// strOutput is the observed result of a get.
// value – "" means the key was absent (nil response from Redis).
type strOutput struct {
	value string
}

// kvStringModel is a partitioned Porcupine model.
// Each partition corresponds to one Redis key; its state is the current string
// value ("" = absent / never written).
var kvStringModel = porcupine.Model{
	// Partition by key so Porcupine can exploit P-compositionality.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(strInput).key
			m[key] = append(m[key], op)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, m[k])
		}
		return out
	},

	Init: func() interface{} {
		return "" // absent
	},

	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(strInput)
		out := output.(strOutput)
		st := state.(string)
		switch inp.op {
		case opSet:
			// A set is always legal; the new state is the written value.
			return true, inp.value
		case opGet:
			// A get is legal iff it returns exactly the current state.
			return out.value == st, state
		}
		return false, state
	},

	DescribeOperation: func(input, output interface{}) string {
		inp := input.(strInput)
		out := output.(strOutput)
		switch inp.op {
		case opSet:
			return fmt.Sprintf("set(%q, %q)", inp.key, inp.value)
		case opGet:
			if out.value == "" {
				return fmt.Sprintf("get(%q) -> nil", inp.key)
			}
			return fmt.Sprintf("get(%q) -> %q", inp.key, out.value)
		}
		return "<invalid>"
	},
}

// ---------------------------------------------------------------------------
// Helpers for recording operations against a real BadgerDB instance
// ---------------------------------------------------------------------------

// opRecord carries the timing and result of a single get/set call.
type opRecord struct {
	input    strInput
	output   strOutput
	callNs   int64
	returnNs int64
	clientID int
}

func doSet(db *badger.DB, key, value string, clientID int) opRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()
	setKey(conn, db, []byte(key), []byte(value))
	returnNs := time.Now().UnixNano()
	return opRecord{
		input:    strInput{op: opSet, key: key, value: value},
		output:   strOutput{}, // set response is "OK", output not used by model
		callNs:   callNs,
		returnNs: returnNs,
		clientID: clientID,
	}
}

func doGet(db *badger.DB, key string, clientID int) opRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()
	getKey(conn, db, []byte(key))
	returnNs := time.Now().UnixNano()

	got, isNull, _ := conn.result()
	var outVal string
	if !isNull {
		outVal = got
	}
	return opRecord{
		input:    strInput{op: opGet, key: key},
		output:   strOutput{value: outVal},
		callNs:   callNs,
		returnNs: returnNs,
		clientID: clientID,
	}
}

// toOperations converts recorded opRecords to the porcupine.Operation slice
// expected by CheckOperations.
func toOperations(records []opRecord) []porcupine.Operation {
	ops := make([]porcupine.Operation, len(records))
	for i, r := range records {
		ops[i] = porcupine.Operation{
			ClientId: r.clientID,
			Input:    r.input,
			Output:   r.output,
			Call:     r.callNs,
			Return:   r.returnNs,
		}
	}
	return ops
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestLinearizabilitySetGetSerial verifies a simple sequential history:
// set("foo", "bar") then get("foo") -> "bar". Trivially linearizable.
func TestLinearizabilitySetGetSerial(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	var records []opRecord
	records = append(records, doSet(db, "foo", "bar", 0))
	records = append(records, doGet(db, "foo", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected serial set/get history to be linearizable")
	}
}

// TestLinearizabilityGetOnAbsent verifies that getting a key that was never
// set returns "" (nil), consistent with the initial absent state.
func TestLinearizabilityGetOnAbsent(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	var records []opRecord
	records = append(records, doGet(db, "nonexistent", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected get-on-absent history to be linearizable")
	}
}

// TestLinearizabilityOverwrite verifies a set-overwrite-get chain:
// set("k","v1"), set("k","v2"), get("k") -> "v2".
func TestLinearizabilityOverwrite(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	var records []opRecord
	records = append(records, doSet(db, "k", "v1", 0))
	records = append(records, doSet(db, "k", "v2", 0))
	records = append(records, doGet(db, "k", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected overwrite history to be linearizable")
	}
}

// TestLinearizabilityConcurrent spawns several goroutines that interleave
// sets and gets against a shared key. BadgerDB's serialisable MVCC guarantees
// that the resulting history is linearizable; Porcupine confirms this.
func TestLinearizabilityConcurrent(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	const (
		numClients   = 6
		opsPerClient = 10
	)

	var (
		mu      sync.Mutex
		records []opRecord
		wg      sync.WaitGroup
	)

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				var r opRecord
				if i%2 == 0 {
					r = doSet(db, "shared", fmt.Sprintf("c%d-v%d", clientID, i), clientID)
				} else {
					r = doGet(db, "shared", clientID)
				}
				mu.Lock()
				records = append(records, r)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected concurrent set/get history to be linearizable")
	}
}

// TestLinearizabilityConcurrentDisjointKeys exercises multiple independent
// keys concurrently. Porcupine partitions by key, so each key's history is
// checked independently, which is both correct and efficient.
func TestLinearizabilityConcurrentDisjointKeys(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	keys := []string{"alpha", "beta", "gamma", "delta"}

	var (
		mu      sync.Mutex
		records []opRecord
		wg      sync.WaitGroup
	)

	for c, key := range keys {
		wg.Add(1)
		go func(clientID int, k string) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				var r opRecord
				if i%3 != 0 {
					r = doSet(db, k, fmt.Sprintf("v%d", i), clientID)
				} else {
					r = doGet(db, k, clientID)
				}
				mu.Lock()
				records = append(records, r)
				mu.Unlock()
			}
		}(c, key)
	}
	wg.Wait()

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected concurrent disjoint-key history to be linearizable")
	}
}

// TestLinearizabilityDetectsViolation confirms that Porcupine correctly
// identifies a *manually crafted* non-linearizable history. This sanity-check
// verifies that the model and checker are wired up correctly; it does not
// exercise BadgerDB.
//
// History (single key "x", three clients):
//
//	C0: |-------- set("x","A") --------|   (call=0, return=100)
//	C1:    |- get("x") -> "A" -|           (call=20, return=80; overlaps C0)
//	C2:                          |- get("x") -> "" -|  (call=110, return=200)
//
// C2's get returns "" after C0's set has already returned, which is illegal:
// once a write has returned the value must be visible to subsequent reads.
func TestLinearizabilityDetectsViolation(t *testing.T) {
	ops := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    strInput{op: opSet, key: "x", value: "A"},
			Output:   strOutput{},
			Call:     0,
			Return:   100,
		},
		{
			ClientId: 1,
			Input:    strInput{op: opGet, key: "x"},
			Output:   strOutput{value: "A"},
			Call:     20,
			Return:   80,
		},
		{
			ClientId: 2,
			Input:    strInput{op: opGet, key: "x"},
			Output:   strOutput{value: ""},
			Call:     110,
			Return:   200,
		},
	}

	if porcupine.CheckOperations(kvStringModel, ops) {
		t.Fatal("expected crafted non-linearizable history to be flagged as illegal")
	}
}
