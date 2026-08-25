package json

// Linearizability checks for the JSON package's read-modify-write operations.
//
// We use Porcupine (https://github.com/anishathalye/porcupine) to verify that
// concurrent NumIncrBy / Get operations against a single JSON document's
// "counter" path behave as if executed serially. This is the same guarantee
// the old badger-direct concurrency tests exercised via conflict-retry loops;
// in the current architecture each op is one kv.Tx, so the check focuses on
// the serialisability of the store rather than retry bookkeeping.
//
// Model (single document, counter path $.n):
//   state  = float64 (current numeric value of the path)
//   input  = jsonInput{op, delta}
//   output = jsonOutput{value, isNull}
//
// Step semantics:
//   incrBy(delta) → legal iff the returned value equals the new state
//   get()         → legal iff the returned value equals the current state

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/hardpointlabs/invar/redis/testutil"
)

func (c *mockConn) result() (value string, isNull bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return "", false
	}
	last := c.writes[len(c.writes)-1]
	switch {
	case last == "null:":
		return "", true
	default:
		parts := strings.SplitN(last, ":", 2)
		if len(parts) == 2 {
			return parts[1], false
		}
		return "", false
	}
}

type jsonOp int

const (
	opIncrBy jsonOp = iota
	opGet
)

type jsonInput struct {
	op    jsonOp
	delta float64
}

type jsonOutput struct {
	value  float64
	isNull bool
}

var kvJSONCounterModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		return [][]porcupine.Operation{history}
	},

	Init: func() interface{} {
		return 0.0
	},

	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(jsonInput)
		out := output.(jsonOutput)
		st := state.(float64)
		switch inp.op {
		case opIncrBy:
			newState := st + inp.delta
			return !out.isNull && out.value == newState, newState
		case opGet:
			return !out.isNull && out.value == st, state
		}
		return false, state
	},

	DescribeOperation: func(input, output interface{}) string {
		inp := input.(jsonInput)
		out := output.(jsonOutput)
		switch inp.op {
		case opIncrBy:
			return fmt.Sprintf("incrBy(%v) -> %v", inp.delta, out.value)
		case opGet:
			return fmt.Sprintf("get() -> %v", out.value)
		}
		return "<invalid>"
	},
}

type jsonOpRecord struct {
	input    jsonInput
	output   jsonOutput
	callNs   int64
	returnNs int64
	clientID int
}

func jsonToOperations(records []jsonOpRecord) []porcupine.Operation {
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

func doIncrBy(session *common.Session, kvs kv.KeyValueStore, delta float64, clientID int) jsonOpRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()

	op := NumIncrBy(session, []byte("counter"), "$.n", delta)
	var err error
	for {
		err = kvs.Update(func(tx kv.Tx) error {
			val, err := op.DbOp(tx)
			if err != nil {
				op.WireOp(conn, val, err)
				return err
			}
			op.WireOp(conn, val, nil)
			return nil
		})
		if err == nil || err != badger.ErrConflict {
			break
		}
	}
	if err != nil {
		panic(err)
	}
	returnNs := time.Now().UnixNano()
	got, isNull := conn.result()
	var v float64
	if !isNull {
		fmt.Sscanf(got, "%f", &v)
	}
	return jsonOpRecord{
		input:    jsonInput{op: opIncrBy, delta: delta},
		output:   jsonOutput{value: v, isNull: isNull},
		callNs:   callNs,
		returnNs: returnNs,
		clientID: clientID,
	}
}

func doGetCounter(session *common.Session, kvs kv.KeyValueStore, clientID int) jsonOpRecord {
	conn := newMockConn()
	callNs := time.Now().UnixNano()
	result, err := kvs.Read(func(tx kv.Tx) (any, error) {
		return Get(session, []byte("counter"), nil).DbOp(tx)
	})
	if err != nil {
		panic(err)
	}
	Get(session, []byte("counter"), nil).WireOp(conn, result, nil)
	returnNs := time.Now().UnixNano()
	got, isNull := conn.result()
	var v float64
	if !isNull {
		// The doc is {"n":<value>}; extract the number.
		fmt.Sscanf(got, `{"n":%f`, &v)
	}
	return jsonOpRecord{
		input:    jsonInput{op: opGet},
		output:   jsonOutput{value: v, isNull: isNull},
		callNs:   callNs,
		returnNs: returnNs,
		clientID: clientID,
	}
}

func TestLinearizabilityCounterSerial(t *testing.T) {
	session, kvs := testutil.NewTestSession(t)
	if err := kvs.Update(func(tx kv.Tx) error {
		_, err := Set(session, []byte("counter"), "$", map[string]any{"n": 0.0}, false, false, FphaNone).DbOp(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var records []jsonOpRecord
	records = append(records, doIncrBy(session, kvs, 5, 0))
	records = append(records, doIncrBy(session, kvs, 3, 0))
	records = append(records, doGetCounter(session, kvs, 0))

	if !porcupine.CheckOperations(kvJSONCounterModel, jsonToOperations(records)) {
		t.Fatal("expected serial counter history to be linearizable")
	}
}

// TestLinearizabilityCounterConcurrent exercises many concurrent NumIncrBy
// writers plus readers on the same path. Under the store's serialisable MVCC,
// the observed increments must be exactly order-preserved: the final value is
// the sum of all increments, and every read observes a prefix sum. This is
// the guarantee the old same-path concurrency test chased with retries.
func TestLinearizabilityCounterConcurrent(t *testing.T) {
	session, kvs := testutil.NewTestSession(t)
	if err := kvs.Update(func(tx kv.Tx) error {
		_, err := Set(session, []byte("counter"), "$", map[string]any{"n": 0.0}, false, false, FphaNone).DbOp(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	const (
		numClients   = 6
		opsPerClient = 10
	)

	var (
		mu      sync.Mutex
		records []jsonOpRecord
		wg      sync.WaitGroup
	)

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				var r jsonOpRecord
				if i%2 == 0 {
					r = doIncrBy(session, kvs, 1, clientID)
				} else {
					r = doGetCounter(session, kvs, clientID)
				}
				mu.Lock()
				records = append(records, r)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if !porcupine.CheckOperations(kvJSONCounterModel, jsonToOperations(records)) {
		t.Fatal("expected concurrent counter history to be linearizable")
	}
}

// TestLinearizabilityDetectsViolation confirms the model flags a crafted
// non-linearizable history (a get returning a value never written).
func TestLinearizabilityDetectsViolation(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: jsonInput{op: opIncrBy, delta: 1}, Output: jsonOutput{value: 1}, Call: 0, Return: 100},
		{ClientId: 1, Input: jsonInput{op: opGet}, Output: jsonOutput{value: 42}, Call: 110, Return: 200},
	}

	if porcupine.CheckOperations(kvJSONCounterModel, ops) {
		t.Fatal("expected crafted non-linearizable history to be flagged as illegal")
	}
}
