package strings

// Linearizability checks for the basic string get/set operations.
//
// We use Porcupine (https://github.com/anishathalye/porcupine) to verify that
// the observed histories produced by concurrent callers of the strings
// package's Set / Get operations are consistent with a sequentially-correct
// key-value register model.
//
// The guarantees modelled here follow directly from the kv.KeyValueStore's
// serializable read-write transactions (Update) and read-only snapshots
// (Read):
//   - Every Set completes an atomic committed write; no torn writes.
//   - Every Get reads from a consistent snapshot taken at the start of the
//     transaction; it cannot observe a partially-written value.
//   - Because the underlying store serialises concurrent writes with MVCC, the
//     full execution is serialisable, which implies linearizability for
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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
)

// result returns the most recent response recorded by the mockConn, in a
// shape suitable for the Porcupine model (value, isNull, isErr).
func (c *mockConn) result() (value string, isNull bool, isErr bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return "", false, false
	}
	last := c.writes[len(c.writes)-1]
	switch {
	case strings.HasPrefix(last, "null:"):
		return "", true, false
	case strings.HasPrefix(last, "err:"):
		return strings.TrimPrefix(last, "err:"), false, true
	default:
		parts := strings.SplitN(last, ":", 2)
		if len(parts) == 2 {
			return parts[1], false, false
		}
		return "", false, false
	}
}

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
// Helpers for recording operations against a real kv.KeyValueStore instance
// ---------------------------------------------------------------------------

// opRecord carries the timing and result of a single get/set call.
type opRecord struct {
	input    strInput
	output   strOutput
	callNs   int64
	returnNs int64
	clientID int
}

func doSet(session *common.Session, kvs kv.KeyValueStore, key, value string, clientID int) opRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()
	var result any
	err := kvs.Update(func(tx kv.Tx) error {
		var err error
		result, err = Set(session, []byte(key), []byte(value)).DbOp(tx)
		return err
	})
	if err != nil {
		panic(err)
	}
	Set(session, []byte(key), []byte(value)).WireOp(conn, result, nil)
	returnNs := time.Now().UnixNano()
	return opRecord{
		input:    strInput{op: opSet, key: key, value: value},
		output:   strOutput{}, // set response is "OK", output not used by model
		callNs:   callNs,
		returnNs: returnNs,
		clientID: clientID,
	}
}

func doGet(session *common.Session, kvs kv.KeyValueStore, key string, clientID int) opRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()
	result, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Get(session, []byte(key)).DbOp(tx)
	})
	if err != nil {
		panic(err)
	}
	Get(session, []byte(key)).WireOp(conn, result, nil)
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
	session, kvs := testutil.NewTestSession(t)

	var records []opRecord
	records = append(records, doSet(session, kvs, "foo", "bar", 0))
	records = append(records, doGet(session, kvs, "foo", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected serial set/get history to be linearizable")
	}
}

// TestLinearizabilityGetOnAbsent verifies that getting a key that was never
// set returns "" (nil), consistent with the initial absent state.
func TestLinearizabilityGetOnAbsent(t *testing.T) {
	session, kvs := testutil.NewTestSession(t)

	var records []opRecord
	records = append(records, doGet(session, kvs, "nonexistent", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected get-on-absent history to be linearizable")
	}
}

// TestLinearizabilityOverwrite verifies a set-overwrite-get chain:
// set("k","v1"), set("k","v2"), get("k") -> "v2".
func TestLinearizabilityOverwrite(t *testing.T) {
	session, kvs := testutil.NewTestSession(t)

	var records []opRecord
	records = append(records, doSet(session, kvs, "k", "v1", 0))
	records = append(records, doSet(session, kvs, "k", "v2", 0))
	records = append(records, doGet(session, kvs, "k", 0))

	if !porcupine.CheckOperations(kvStringModel, toOperations(records)) {
		t.Fatal("expected overwrite history to be linearizable")
	}
}

// TestLinearizabilityConcurrent spawns several goroutines that interleave
// sets and gets against a shared key. The store's serialisable MVCC
// guarantees that the resulting history is linearizable; Porcupine confirms
// this.
func TestLinearizabilityConcurrent(t *testing.T) {
	session, kvs := testutil.NewTestSession(t)

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
					r = doSet(session, kvs, "shared", fmt.Sprintf("c%d-v%d", clientID, i), clientID)
				} else {
					r = doGet(session, kvs, "shared", clientID)
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
	session, kvs := testutil.NewTestSession(t)

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
					r = doSet(session, kvs, k, fmt.Sprintf("v%d", i), clientID)
				} else {
					r = doGet(session, kvs, k, clientID)
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
// exercise the KV store.
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
