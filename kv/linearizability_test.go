package kv

// Linearizability / snapshot-isolation checks for the kv.KeyValueStore
// abstraction, exercised through the Tx interface against the BadgerDB
// implementation (the only backend that exists today).
//
// We use Porcupine (https://github.com/anishathalye/porcupine), the same
// checker used by the other linearizability tests in this repo
// (redis/strings, redis/json).
//
// PART 1 (general document mutations): each Tx is modeled as a single
// Porcupine operation whose Input is an ordered list of {Get,key} /
// {Set,key,value} / {Delete,key} sub-ops and whose Output is the ordered
// list of Get results plus a final status (Committed / Conflict / Error).
// The sequential model checks, for every Committed tx, that its reads
// match the state at the point it is linearized, then applies its writes.
// Aborted / conflicted / errored txs are treated as no-ops: a correct
// OCC-based store may abort any transaction, so that branch is never
// asserted against.
//
// The guarantee under test is SNAPSHOT ISOLATION, not full
// serializability: write skew is an accepted, documented limitation of
// this store. TestKvWriteSkewPinnedDown empirically pins down whether the
// current Badger-backed implementation permits write skew or not (Badger
// performs per-key read-write conflict detection, which is closer to SSI
// than plain SI), and asserts whichever outcome actually occurs rather
// than assuming one.
//
// PART 2 (KeyValueIterator): snapshot scope, ordering, and phantom
// detection of Tx.NewIterator(prefix) are checked behaviorally with
// hand-crafted concurrent scenarios.
//
// Explicitly OUT OF SCOPE for this suite (do not read anything into a pass
// or fail of these tests regarding them):
//   - KeyValueStore.Merge / MergeOption / WriteHandle (not implemented;
//     MergeOption is a stub).
//   - Any SlateDB implementation (only Badger exists).
//   - Any domain-specific modeling (job queues, BullMQ semantics, etc.).
//
// Also note: Entry metadata / TTL / expiry semantics are NOT modeled here —
// only plain byte values. If/when commands come to depend on metadata or
// TTL for correctness (e.g. expiry semantics), that needs its own dedicated
// suite; this one would not catch a metadata/TTL bug.

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	badger "github.com/dgraph-io/badger/v4"
)

// ---------------------------------------------------------------------------
// Flags. Everything that affects workload shape is configurable so the same
// binary can run a fast CI pass (defaults) or a longer soak (larger values).
// ---------------------------------------------------------------------------

var (
	flagLinRounds      = flag.Int("kv-lin-rounds", 1, "rounds of the concurrent workload; the store is reset between rounds")
	flagLinWorkers     = flag.Int("kv-lin-workers", 8, "number of concurrent Tx goroutines in the linearizability workload")
	flagLinIterations  = flag.Int("kv-lin-iterations", 25, "transactions executed per worker goroutine")
	flagLinKeys        = flag.Int("kv-lin-keys", 8, "size of the shared key universe (contention driver)")
	flagLinMutateProb  = flag.Float64("kv-lin-mutate-prob", 0.7, "probability that a generated Tx is mutating (else read-only)")
	flagLinMaxSubOps   = flag.Int("kv-lin-max-subops", 4, "maximum sub-ops per generated Tx")
	flagLinTimeout     = flag.Duration("kv-lin-timeout", 60*time.Second, "porcupine linearizability check timeout")
	flagWriteSkewRuns  = flag.Int("kv-write-skew-runs", 5, "rounds for the write-skew pinning test")
	flagPhantomRuns    = flag.Int("kv-phantom-runs", 3, "rounds for the iterator phantom-check test")
)

// ---------------------------------------------------------------------------
// Operation model: one Tx = one operation with an ordered list of sub-ops.
// ---------------------------------------------------------------------------

type kvSubOpKind int

const (
	subGet kvSubOpKind = iota
	subSet
	subDelete
)

// kvSubOp is one primitive within a Tx. For subGet, val is unused.
type kvSubOp struct {
	kind kvSubOpKind
	key  []byte
	val  []byte
}

// kvGetResult is the observed outcome of one Get sub-op.
type kvGetResult struct {
	found bool
	value []byte // meaningful only when found == true
}

type kvTxStatus int

const (
	txCommitted kvTxStatus = iota
	txConflict
	txError
)

// kvTxInput is the full Input of a Tx: the ordered sub-ops to execute.
type kvTxInput struct {
	subOps   []kvSubOp
	readOnly bool
}

// kvTxOutput is the full Output of a Tx: Get results in sub-op order, plus
// the terminal status.
type kvTxOutput struct {
	gets   []kvGetResult
	status kvTxStatus
}

// kvOpRecord is the recorded (timing + input + output) of one Tx.
type kvOpRecord struct {
	input    kvTxInput
	output   kvTxOutput
	callNs   int64
	returnNs int64
	clientID int
}

// kvState is the sequential model's state: a map from key to value.
type kvState map[string][]byte

// initialKvState returns the state every model check starts from. It must
// exactly match the values the workload seeds into the store. The value "v0"
// means "a plain stored value"; absence of a key means ErrKeyNotFound.
func initialKvState() kvState {
	st := make(kvState, *flagLinKeys)
	for i := 0; i < *flagLinKeys; i++ {
		st[string(keyName(i))] = []byte("v0")
	}
	return st
}

func keyName(i int) []byte {
	return []byte(fmt.Sprintf("k%02d", i))
}

func cloneState(st kvState) kvState {
	out := make(kvState, len(st))
	for k, v := range st {
		out[k] = append([]byte{}, v...)
	}
	return out
}

func kvStateEqual(a, b interface{}) bool {
	sa := a.(kvState)
	sb := b.(kvState)
	if len(sa) != len(sb) {
		return false
	}
	for k, va := range sa {
		vb, ok := sb[k]
		if !ok || !bytes.Equal(va, vb) {
			return false
		}
	}
	return true
}

func kvStateHash(state interface{}) uint64 {
	st := state.(kvState)
	keys := make([]string, 0, len(st))
	for k := range st {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var h uint64 = 14695981039346656037 // FNV-1a offset basis
	fold := func(b []byte) {
		for _, c := range b {
			h ^= uint64(c)
			h *= 1099511628211
		}
	}
	for _, k := range keys {
		fold([]byte(k))
		fold([]byte{0})
		fold(st[k])
		fold([]byte{0})
	}
	return h
}

// kvTxStep is the sequential specification of one Tx operation.
//
// Non-committed txs (Conflict / Error) are always legal no-ops: a correct
// OCC-based store may abort any transaction, so no value assertions are made
// on that branch (it is a liveness concern, not a safety one).
//
// A Committed tx is legal iff its recorded Get results match a replay of its
// sub-ops against a scratch copy of the state (value, or not-found iff the
// scratch copy has no entry for the key). If the replay matches, the
// Set/Delete sub-ops are applied to the scratch copy and it becomes the new
// state.
func kvTxStep(state, input, output interface{}) (bool, interface{}) {
	st := state.(kvState)
	in := input.(kvTxInput)
	out := output.(kvTxOutput)

	if out.status != txCommitted {
		return true, state
	}

	scratch := cloneState(st)
	getIdx := 0
	for _, sub := range in.subOps {
		switch sub.kind {
		case subGet:
			if getIdx >= len(out.gets) {
				return false, state
			}
			got := out.gets[getIdx]
			getIdx++
			want, exists := scratch[string(sub.key)]
			if got.found != exists || (exists && !bytes.Equal(got.value, want)) {
				return false, state
			}
		case subSet:
			scratch[string(sub.key)] = append([]byte{}, sub.val...)
		case subDelete:
			delete(scratch, string(sub.key))
		}
	}
	if getIdx != len(out.gets) {
		return false, state
	}
	return true, scratch
}

func describeTxOperation(input, output interface{}) string {
	in := input.(kvTxInput)
	out := output.(kvTxOutput)
	var sb strings.Builder
	sb.WriteString("tx{")
	getIdx := 0
	for i, sub := range in.subOps {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch sub.kind {
		case subGet:
			sb.WriteString("get(")
			sb.Write(sub.key)
			sb.WriteString(")=")
			if getIdx < len(out.gets) {
				if out.gets[getIdx].found {
					sb.Write(out.gets[getIdx].value)
				} else {
					sb.WriteString("<absent>")
				}
			} else {
				sb.WriteString("?")
			}
			getIdx++
		case subSet:
			sb.WriteString("set(")
			sb.Write(sub.key)
			sb.WriteString(",")
			sb.Write(sub.val)
			sb.WriteString(")")
		case subDelete:
			sb.WriteString("del(")
			sb.Write(sub.key)
			sb.WriteString(")")
		}
	}
	sb.WriteString("} -> ")
	switch out.status {
	case txCommitted:
		sb.WriteString("committed")
	case txConflict:
		sb.WriteString("conflict")
	default:
		sb.WriteString("error")
	}
	return sb.String()
}

func describeKvState(state interface{}) string {
	st := state.(kvState)
	keys := make([]string, 0, len(st))
	for k := range st {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, st[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// kvTxModel is the Porcupine model. No Partition: each operation touches
// multiple keys (a Tx spans the whole key universe), so the history must be
// checked as a single partition.
var kvTxModel = porcupine.Model{
	Init: func() interface{} { return initialKvState() },
	Step: kvTxStep,
	// State is map[string][]byte, which is not comparable with ==, so a
	// proper Equal + Hash are required.
	Equal:             kvStateEqual,
	Hash:              kvStateHash,
	DescribeOperation: describeTxOperation,
	DescribeState:     describeKvState,
}

// ---------------------------------------------------------------------------
// Recording helpers
// ---------------------------------------------------------------------------

func setOp(key, val []byte) kvSubOp    { return kvSubOp{kind: subSet, key: key, val: val} }
func getOp(key []byte) kvSubOp         { return kvSubOp{kind: subGet, key: key} }
func deleteOp(key []byte) kvSubOp      { return kvSubOp{kind: subDelete, key: key} }

// executeTx runs one Tx against the store and records the full
// input/output/timing for Porcupine.
//
// Call/Return timestamps are real wall-clock time captured at the moment
// Begin is invoked and the moment Commit()/Discard() returns, respectively —
// no synthetic sequence numbers.
//
// Read-only txs are Discarded (not Committed) but recorded with status
// Committed so their snapshot reads are still validated: SI guarantees a
// read-only tx reads a consistent snapshot, and that is exactly the property
// under test.
func executeTx(kvs KeyValueStore, clientID int, subOps []kvSubOp, mutating bool) kvOpRecord {
	rec := kvOpRecord{
		clientID: clientID,
		input:    kvTxInput{subOps: subOps, readOnly: !mutating},
	}
	rec.callNs = time.Now().UnixNano()
	tx := kvs.Begin(mutating)

	ok := true
	for _, sub := range subOps {
		if !ok {
			break
		}
		switch sub.kind {
		case subGet:
			item, err := tx.Get(sub.key)
			switch {
			case err == ErrKeyNotFound:
				rec.output.gets = append(rec.output.gets, kvGetResult{found: false})
			case err != nil:
				ok = false
			default:
				val, verr := item.Value()
				if verr != nil {
					ok = false
				} else {
					rec.output.gets = append(rec.output.gets, kvGetResult{found: true, value: val})
				}
			}
		case subSet:
			if err := tx.Set(kvs.NewEntry(sub.key, sub.val)); err != nil {
				ok = false
			}
		case subDelete:
			if err := tx.Delete(sub.key); err != nil {
				ok = false
			}
		}
	}

	if ok {
		if mutating {
			switch err := tx.Commit(); {
			case err == nil:
				rec.output.status = txCommitted
			case errors.Is(err, badger.ErrConflict):
				// badgerTx.Commit returns Badger's own ErrConflict unwrapped
				// ("Transaction Conflict. Please retry"); recognize it here.
				rec.output.status = txConflict
			default:
				rec.output.status = txError
			}
		} else {
			tx.Discard()
			rec.output.status = txCommitted
		}
	} else {
		tx.Discard()
		rec.output.status = txError
	}

	rec.returnNs = time.Now().UnixNano()
	if rec.returnNs < rec.callNs {
		rec.returnNs = rec.callNs
	}
	return rec
}

// generateSubOps builds a random ordered list of sub-ops. Read-only txs are
// restricted to Get sub-ops only (they must not call Set/Delete).
func generateSubOps(rng *rand.Rand, mutating bool) []kvSubOp {
	n := 1 + rng.Intn(*flagLinMaxSubOps)
	ops := make([]kvSubOp, 0, n)
	for i := 0; i < n; i++ {
		kind := subGet
		if mutating {
			kind = kvSubOpKind(rng.Intn(3))
		}
		sub := kvSubOp{kind: kind, key: keyName(rng.Intn(*flagLinKeys))}
		if kind == subSet {
			sub.val = []byte(fmt.Sprintf("v%d", rng.Intn(100)))
		}
		ops = append(ops, sub)
	}
	return ops
}

func toKvOperations(records []kvOpRecord) []porcupine.Operation {
	ops := make([]porcupine.Operation, len(records))
	for i, r := range records {
		ops[i] = porcupine.Operation{
			ClientId: r.clientID,
			Input:    r.input,
			Call:     r.callNs,
			Output:   r.output,
			Return:   r.returnNs,
		}
	}
	return ops
}

// seedStore writes initialKvState() into the store so the live state matches
// the model's Init.
func seedStore(t *testing.T, kvs KeyValueStore) {
	t.Helper()
	err := kvs.Update(func(tx Tx) error {
		for k, v := range initialKvState() {
			if err := tx.Set(kvs.NewEntry([]byte(k), v)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seeding initial state failed: %v", err)
	}
}

// checkLinearizable runs the verbose Porcupine check. On violation or
// timeout it prints the full failing history, not just pass/fail.
func checkLinearizable(t *testing.T, history []porcupine.Operation) {
	t.Helper()
	result, _ := porcupine.CheckOperationsVerbose(kvTxModel, history, *flagLinTimeout)
	switch result {
	case porcupine.Ok:
		return
	case porcupine.Illegal:
		t.Fatalf("history of %d Tx operations is NOT linearizable:\n%s", len(history), describeHistory(history))
	case porcupine.Unknown:
		t.Fatalf("linearizability check timed out after %v on %d operations (raise -kv-lin-timeout, or lower -kv-lin-iterations / -kv-lin-workers / -kv-lin-rounds). history:\n%s",
			*flagLinTimeout, len(history), describeHistory(history))
	}
}

func describeHistory(history []porcupine.Operation) string {
	ops := append([]porcupine.Operation{}, history...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	var sb strings.Builder
	for i, op := range ops {
		fmt.Fprintf(&sb, "  [%d] client=%d call=%d return=%d  %s\n",
			i, op.ClientId, op.Call, op.Return, kvTxModel.DescribeOperation(op.Input, op.Output))
	}
	return sb.String()
}

func countStatus(records []kvOpRecord, status kvTxStatus) int {
	n := 0
	for _, r := range records {
		if r.output.status == status {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// PART 1: General document mutations
// ---------------------------------------------------------------------------

// TestKvLinearizabilitySerial runs a small deterministic history through the
// real store (set, get, get, delete, get) to validate the recording/checking
// plumbing end to end.
func TestKvLinearizabilitySerial(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()
	seedStore(t, kvs)

	k := []byte("k00")
	var records []kvOpRecord
	records = append(records, executeTx(kvs, 0, []kvSubOp{setOp(k, []byte("hello"))}, true))
	records = append(records, executeTx(kvs, 0, []kvSubOp{getOp(k)}, false))
	records = append(records, executeTx(kvs, 0, []kvSubOp{getOp(k)}, false))
	records = append(records, executeTx(kvs, 0, []kvSubOp{deleteOp(k)}, true))
	records = append(records, executeTx(kvs, 0, []kvSubOp{getOp(k)}, false))

	checkLinearizable(t, toKvOperations(records))
}

// TestKvLinearizabilityConcurrentTxs runs the randomized workload: N
// goroutines each repeatedly open a Tx (a mix of mutating Begin(true) and
// read-only Begin(false)), issue a random mix of Get/Set/Delete against a
// small shared key universe to force contention, and commit or discard. The
// recorded history must be linearizable against the model.
//
// The workload is split into ROUNDS. Between rounds the store is reset to
// initialKvState() (which the model uses as Init), so every round is a
// self-contained history checked from a known seed state. This exists because
// Porcupine's single-partition check cost explodes with the number of
// concurrent operations whose intervals overlap, NOT with total op count:
// 4000 ops at 8 workers check in ~2s, while 500 ops at 50 workers time out
// after a minute. Rounds bound the concurrency-per-check while still allowing
// an arbitrarily long total soak:
//
//	go test ./kv -run TestKvLinearizabilityConcurrentTxs \
//	   -kv-lin-rounds=50 -kv-lin-workers=8 -kv-lin-iterations=200
func TestKvLinearizabilityConcurrentTxs(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	var totalCommits, totalConflicts, totalErrors int
	for round := 0; round < *flagLinRounds; round++ {
		seedStore(t, kvs)

		var (
			mu      sync.Mutex
			records []kvOpRecord
			wg      sync.WaitGroup
		)

		for c := 0; c < *flagLinWorkers; c++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()
				seed := int64(clientID+1)*2654435761 + int64(round)*15485863
				rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ seed))
				for i := 0; i < *flagLinIterations; i++ {
					mutating := rng.Float64() < *flagLinMutateProb
					rec := executeTx(kvs, clientID, generateSubOps(rng, mutating), mutating)
					mu.Lock()
					records = append(records, rec)
					mu.Unlock()
				}
			}(c)
		}
		wg.Wait()

		totalCommits += countStatus(records, txCommitted)
		totalConflicts += countStatus(records, txConflict)
		totalErrors += countStatus(records, txError)

		t.Logf("round %d/%d: recorded %d txs (%d workers x %d iterations) over %d keys: %d committed, %d conflict, %d error",
			round+1, *flagLinRounds, len(records), *flagLinWorkers, *flagLinIterations, *flagLinKeys,
			countStatus(records, txCommitted), countStatus(records, txConflict), countStatus(records, txError))

		checkLinearizable(t, toKvOperations(records))
	}
	t.Logf("total across %d rounds: %d committed, %d conflict, %d error",
		*flagLinRounds, totalCommits, totalConflicts, totalErrors)
}

// TestKvModelDetectsViolation sanity-checks that the model is not vacuous: a
// crafted history where a committed read happens after a committed write yet
// reports the pre-write value must be flagged Illegal. This does not touch
// the store.
func TestKvModelDetectsViolation(t *testing.T) {
	history := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    kvTxInput{subOps: []kvSubOp{setOp([]byte("x"), []byte("A"))}},
			Call:     0,
			Output:   kvTxOutput{status: txCommitted},
			Return:   100,
		},
		{
			ClientId: 1,
			Input:    kvTxInput{subOps: []kvSubOp{getOp([]byte("x"))}},
			Call:     20,
			Output:   kvTxOutput{gets: []kvGetResult{{found: true, value: []byte("A")}}, status: txCommitted},
			Return:   80,
		},
		{
			ClientId: 2,
			Input:    kvTxInput{subOps: []kvSubOp{getOp([]byte("x"))}},
			Call:     110,
			Output:   kvTxOutput{gets: []kvGetResult{{found: false}}, status: txCommitted},
			Return:   200,
		},
	}
	if porcupine.CheckOperations(kvTxModel, history) {
		t.Fatalf("expected crafted non-linearizable history to be flagged Illegal:\n%s", describeHistory(history))
	}
}

// TestKvModelAcceptsLinearizable sanity-checks the positive direction of the
// model: a history where reads observe a prior committed write is accepted.
func TestKvModelAcceptsLinearizable(t *testing.T) {
	history := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    kvTxInput{subOps: []kvSubOp{setOp([]byte("k00"), []byte("v5"))}},
			Call:     0,
			Output:   kvTxOutput{status: txCommitted},
			Return:   100,
		},
		{
			ClientId: 1,
			Input:    kvTxInput{subOps: []kvSubOp{getOp([]byte("k00"))}},
			Call:     20,
			Output:   kvTxOutput{gets: []kvGetResult{{found: true, value: []byte("v5")}}, status: txCommitted},
			Return:   80,
		},
		{
			ClientId: 2,
			Input:    kvTxInput{subOps: []kvSubOp{getOp([]byte("k00"))}},
			Call:     110,
			Output:   kvTxOutput{gets: []kvGetResult{{found: true, value: []byte("v5")}}, status: txCommitted},
			Return:   200,
		},
	}
	if !porcupine.CheckOperations(kvTxModel, history) {
		t.Fatalf("expected crafted linearizable history to be accepted:\n%s", describeHistory(history))
	}
}

// TestKvWriteSkewPinnedDown empirically pins down whether the current
// Badger-backed implementation permits write skew.
//
// Two keys balance:A and balance:B are both initialized to 60. Two
// concurrent txs each read both keys and, because decrementing one key alone
// keeps the invariant A+B >= 100 true, write only their own key and commit.
// If both commit, the invariant is broken (49+49 < 100) => write skew.
//
// Badger tracks read sets and performs per-key read-write conflict detection
// at commit time, so we expect one of the two txs to get ErrConflict. But we
// do NOT assume this up front: the test asserts whichever outcome actually
// occurs, consistently across repeated runs. It fails only if the outcome is
// inconsistent across runs (a genuine harness race or a nondeterministic
// conflict-detection bug), not based on which single outcome it lands on.
func TestKvWriteSkewPinnedDown(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	const (
		balanceA  = "balance:A"
		balanceB  = "balance:B"
		initial   = 60
		threshold = 100
		decrement = 11
	)

	resetBalances := func() {
		t.Helper()
		err := kvs.Update(func(tx Tx) error {
			if err := tx.Set(kvs.NewEntry([]byte(balanceA), []byte(strconv.Itoa(initial)))); err != nil {
				return err
			}
			return tx.Set(kvs.NewEntry([]byte(balanceB), []byte(strconv.Itoa(initial))))
		})
		if err != nil {
			t.Fatalf("reset balances failed: %v", err)
		}
	}

	runs := *flagWriteSkewRuns
	if runs < 1 {
		runs = 1
	}
	outcomes := make(map[string]int)
	for r := 0; r < runs; r++ {
		resetBalances()
		outcome, commitErrs := runWriteSkewRound(kvs, threshold, decrement)
		for _, e := range commitErrs {
			if e != nil && !errors.Is(e, badger.ErrConflict) {
				t.Fatalf("run %d: unexpected commit error (not a conflict): %v", r, e)
			}
		}
		t.Logf("run %d: outcome=%s commitErrs=(%v, %v)", r, outcome, commitErrs[0], commitErrs[1])
		outcomes[outcome]++
	}

	if len(outcomes) > 1 {
		t.Fatalf("write-skew outcome INCONSISTENT across %d runs: %v", runs, outcomes)
	}
	for o, n := range outcomes {
		t.Logf("consistent outcome across %d runs: %s", n, o)
	}
}

// runWriteSkewRound runs a single write-skew round and reports which outcome
// occurred, judged by the ground truth: whether the invariant A+B >= threshold
// holds in the final state. commitErrs carries each tx's Commit() error.
func runWriteSkewRound(kvs KeyValueStore, threshold, decrement int) (outcome string, commitErrs [2]error) {
	const (
		balanceA = "balance:A"
		balanceB = "balance:B"
	)

	type result struct {
		idx int
		err error
	}
	barrier := make(chan struct{})
	goSignal := make(chan struct{})
	results := make(chan result, 2)

	worker := func(idx int) {
		tx := kvs.Begin(true)
		defer tx.Discard()

		getInt := func(key string) (int, bool) {
			item, err := tx.Get([]byte(key))
			if err != nil {
				return 0, false
			}
			v, verr := item.Value()
			if verr != nil {
				return 0, false
			}
			n, aerr := strconv.Atoi(string(v))
			return n, aerr == nil
		}
		a, okA := getInt(balanceA)
		b, okB := getInt(balanceB)
		if !okA || !okB {
			results <- result{idx, fmt.Errorf("failed to read both balances")}
			return
		}

		// Write only our own key, and only if decrementing it alone keeps
		// the invariant true.
		if idx == 0 {
			if a-decrement+b >= threshold {
				if err := tx.Set(kvs.NewEntry([]byte(balanceA), []byte(strconv.Itoa(a-decrement)))); err != nil {
					results <- result{idx, err}
					return
				}
			}
		} else {
			if a+b-decrement >= threshold {
				if err := tx.Set(kvs.NewEntry([]byte(balanceB), []byte(strconv.Itoa(b-decrement)))); err != nil {
					results <- result{idx, err}
					return
				}
			}
		}

		// Wait until both txs have read both keys before either commits,
		// guaranteeing overlapping read/write sets.
		barrier <- struct{}{}
		<-goSignal
		results <- result{idx, tx.Commit()}
	}

	go worker(0)
	go worker(1)
	<-barrier
	<-barrier
	close(goSignal)
	r0 := <-results
	r1 := <-results
	commitErrs[r0.idx] = r0.err
	commitErrs[r1.idx] = r1.err

	var finalA, finalB int
	rtx := kvs.Begin(false)
	defer rtx.Discard()
	if item, err := rtx.Get([]byte(balanceA)); err == nil {
		if v, verr := item.Value(); verr == nil {
			finalA, _ = strconv.Atoi(string(v))
		}
	}
	if item, err := rtx.Get([]byte(balanceB)); err == nil {
		if v, verr := item.Value(); verr == nil {
			finalB, _ = strconv.Atoi(string(v))
		}
	}

	if finalA+finalB < threshold {
		return "write-skew-observed (invariant violated)", commitErrs
	}
	return "no-write-skew (conflict-prevented)", commitErrs
}

// ---------------------------------------------------------------------------
// PART 2: Iterator properties (Tx.NewIterator)
// ---------------------------------------------------------------------------

// TestKvIteratorOrdering verifies that Tx.NewIterator returns keys under a
// shared prefix in lexicographic byte order, regardless of insertion order.
func TestKvIteratorOrdering(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	prefix := []byte("ord:")
	insertOrder := [][]byte{
		[]byte("ord:b"), []byte("ord:a"), []byte("ord:c"), []byte("ord:aa"),
	}
	err := kvs.Update(func(tx Tx) error {
		for _, k := range insertOrder {
			if err := tx.Set(kvs.NewEntry(k, []byte("v"))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seeding failed: %v", err)
	}

	tx := kvs.Begin(false)
	defer tx.Discard()
	it := tx.NewIterator(prefix)
	defer it.Close()

	var got [][]byte
	for it.Next() {
		if err := it.Err(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		got = append(got, append([]byte{}, it.Item().Key()...))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error after scan: %v", err)
	}

	want := [][]byte{
		[]byte("ord:a"), []byte("ord:aa"), []byte("ord:b"), []byte("ord:c"),
	}
	if len(got) != len(want) {
		t.Fatalf("iterator returned %d keys, want %d (got %v)", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("position %d: got %q, want %q", i, got[i], want[i])
		}
		if i > 0 && bytes.Compare(got[i-1], got[i]) >= 0 {
			t.Errorf("keys not strictly increasing at position %d: %q >= %q", i, got[i-1], got[i])
		}
	}
}

// TestKvIteratorSnapshotScope verifies that a Tx's iterator view is fixed at
// its own snapshot: a concurrent committed insert under the same prefix is
// never observed, whether the iterator finishes before or after that commit.
func TestKvIteratorSnapshotScope(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	t.Run("insert-commits-mid-iteration", func(t *testing.T) {
		prefix := []byte("snap:")
		seedKeys := [][]byte{[]byte("snap:a"), []byte("snap:b"), []byte("snap:c")}
		err := kvs.Update(func(tx Tx) error {
			for _, k := range seedKeys {
				if err := tx.Set(kvs.NewEntry(k, []byte("v"))); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("seeding failed: %v", err)
		}

		txA := kvs.Begin(false)
		defer txA.Discard()
		itA := txA.NewIterator(prefix)
		defer itA.Close()

		aStarted := make(chan struct{})
		bCommitted := make(chan struct{})
		go func() {
			<-aStarted
			txB := kvs.Begin(true)
			defer txB.Discard()
			if err := txB.Set(kvs.NewEntry([]byte("snap:zz"), []byte("v"))); err != nil {
				t.Error(err)
			}
			if err := txB.Commit(); err != nil {
				t.Error(err)
			}
			close(bCommitted)
		}()

		// A's snapshot is fixed at Begin, before B's insert. Begin iterating
		// (partially), then let B commit, then finish iterating.
		if !itA.Next() {
			t.Fatal("expected at least one key under prefix")
		}
		close(aStarted)
		<-bCommitted

		var got [][]byte
		got = append(got, append([]byte{}, itA.Item().Key()...))
		for itA.Next() {
			got = append(got, append([]byte{}, itA.Item().Key()...))
		}
		if err := itA.Err(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}

		assertIteratorKeys(t, got, seedKeys)
	})

	t.Run("commit-after-full-iteration", func(t *testing.T) {
		// Note: this subtest uses its own prefix so its expected key set is
		// independent of the first subtest's committed insert.
		prefix := []byte("snap2:")
		seedKeys := [][]byte{[]byte("snap2:a"), []byte("snap2:b"), []byte("snap2:c")}
		err := kvs.Update(func(tx Tx) error {
			for _, k := range seedKeys {
				if err := tx.Set(kvs.NewEntry(k, []byte("v"))); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("seeding failed: %v", err)
		}

		txA := kvs.Begin(false)
		defer txA.Discard()
		itA := txA.NewIterator(prefix)
		defer itA.Close()

		var got [][]byte
		for itA.Next() {
			got = append(got, append([]byte{}, itA.Item().Key()...))
		}

		// B's insert commits after A has already finished iterating; A's
		// snapshot still predates it, so it must not appear.
		err = kvs.Update(func(tx Tx) error {
			return tx.Set(kvs.NewEntry([]byte("snap2:zz"), []byte("v")))
		})
		if err != nil {
			t.Fatalf("B insert failed: %v", err)
		}

		assertIteratorKeys(t, got, seedKeys)
	})
}

func assertIteratorKeys(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("iterator observed %d keys, want %d (got %v)", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("position %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestKvIteratorPhantomCheck probes whether a full-range read (via iterator
// only, never a Get on a specific key) followed by a write that encodes the
// observed range contents is protected against a concurrent insert into that
// range. This test exists to determine empirically whether Badger's range
// reads close the phantom hole — do not assume the answer going in.
//
//   - Tx A iterates prefix "phan:" fully, counts the keys, then writes that
//     count to an unrelated key (never calling Get on any iterated key).
//   - Concurrently, Tx B inserts a new key under "phan:" and commits before A
//     commits.
//
// Accepted outcomes, both logged:
//   (i) A's snapshot was taken strictly before B's insert and simply never
//       observed it (Badger conflict detection is per-key, and k3 is not in
//       A's read set, so A commits). Acceptable SI snapshot behavior.
//   (ii) A's Commit() returns ErrConflict, because A's write encoded a
//       precondition on the prefix range's contents that B's insert violated.
//
// The test FAILS if A commits silently while having missed an insert that
// committed before A's snapshot (i.e. an insert A should have seen), with A's
// write still encoding the stale observation as valid — a genuine phantom.
func TestKvIteratorPhantomCheck(t *testing.T) {
	kvs := InMemoryBadger(t)
	defer kvs.Close()

	prefix := []byte("phan:")
	seedKeys := [][]byte{[]byte("phan:k1"), []byte("phan:k2")}
	err := kvs.Update(func(tx Tx) error {
		for _, k := range seedKeys {
			if err := tx.Set(kvs.NewEntry(k, []byte("v"))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seeding failed: %v", err)
	}

	runs := *flagPhantomRuns
	if runs < 1 {
		runs = 1
	}
	for r := 0; r < runs; r++ {
		t.Logf("round %d: %s", r, runPhantomRound(t, kvs, prefix))
	}
}

func runPhantomRound(t *testing.T, kvs KeyValueStore, prefix []byte) string {
	t.Helper()

	countKey := []byte("count-observed")
	aStarted := make(chan struct{})
	bCommitted := make(chan struct{})
	var bCommitErr error

	txA := kvs.Begin(true)
	defer txA.Discard()
	aBeginTs := time.Now().UnixNano()

	go func() {
		txB := kvs.Begin(true)
		defer txB.Discard()
		<-aStarted
		if err := txB.Set(kvs.NewEntry([]byte("phan:k3"), []byte("v"))); err != nil {
			bCommitErr = err
			close(bCommitted)
			return
		}
		bCommitErr = txB.Commit()
		close(bCommitted)
	}()

	it := txA.NewIterator(prefix)
	count := 0
	for it.Next() {
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	close(aStarted)
	<-bCommitted
	if bCommitErr != nil {
		t.Fatalf("Tx B commit failed: %v", bCommitErr)
	}
	bCommitTs := time.Now().UnixNano()

	if err := txA.Set(kvs.NewEntry(countKey, []byte(strconv.Itoa(count)))); err != nil {
		t.Fatalf("Tx A set failed: %v", err)
	}
	commitErr := txA.Commit()

	switch {
	case errors.Is(commitErr, ErrConflict):
		// Outcome (ii): A's write encoded a precondition on the range that B's
		// insert violated; the store aborted A.
		return "A commit conflicted with B's insert (range-read tracking active) — acceptable"
	case commitErr == nil:
		if aBeginTs > bCommitTs {
			// A's snapshot was taken after B's insert committed, yet A missed it
			// and committed a stale observation as valid. Genuine phantom.
			t.Fatalf("phantom: A committed having observed %d keys (missed B's %q), even though B's insert committed before A's snapshot; A's write encodes a stale observation as valid", count, "phan:k3")
		}
		// Outcome (i): A's snapshot predates B's insert, so missing it is
		// legitimate snapshot-isolation behavior.
		return fmt.Sprintf("A committed without observing B's insert (snapshot t=%d predates B's commit t=%d) — acceptable", aBeginTs, bCommitTs)
	default:
		t.Fatalf("unexpected commit error from A: %v", commitErr)
		return ""
	}
}
