// Package kv abstracts a transactional LSM-tree key/value store —
// currently BadgerDB, with a SlateDB implementation in progress — behind
// one interface so that calling code code never talks to a
// specific storage engine directly.
//
// # Guarantee model
//
// The abstraction makes different promises for different kinds of
// operations. Don't assume one blanket isolation level applies to
// everything below — which promise applies depends on which entry point
// you use.
//
//	Entry point                 Guarantee
//	Tx (Get/Set/Delete/Commit)  Snapshot Isolation, write-write conflict
//	                             detection (ErrConflict)
//	Tx.NewIterator              Same snapshot as the enclosing Tx;
//	                             lexicographic key order
//	KeyValueStore.Merge         Not yet implemented. See "Commutative
//	                             operations" below for the target contract.
//
// # Tx: Snapshot Isolation, not full serializability
//
// All Get/Set/Delete calls made through a Tx observe a consistent
// snapshot fixed at the transaction's start, and a Tx's buffered writes
// apply as a single indivisible unit on successful commit, or not at all
// on error. Two concurrent mutating transactions whose write sets
// intersect cannot both commit. The loser's commit returns ErrConflict.
//
// This is Snapshot Isolation, which is weaker than full serializability:
// write skew (two transactions each individually preserve an invariant
// they read, but committing both together violates it) may or may not be
// possible depending on the backend's default conflict-detection
// behavior. Do not assume write skew is prevented unless you've checked
// TestKvWriteSkewPinnedDown in linearizability_test.go for the backend
// you're using. That test exists specifically to pin this down
// empirically per backend rather than let it be assumed, since it can
// differ between BadgerDB (read-write conflict detection by
// default, closer to SSI) and SlateDB (isolation level is an explicit,
// separately-configured choice).
//
// If your use case cannot tolerate write skew (the canonical example:
// atomically claiming a queued job so two workers can't both claim it),
// don't rely on this package's default — verify the relevant test's
// current result for your backend, and if it shows write skew is
// possible, either configure that backend's stronger isolation mode
// explicitly or serialize the relevant transactions at the application
// level.
//
// # Iterators
//
// Tx.NewIterator(prefix) shares its enclosing Tx's snapshot: a key
// inserted by another transaction, even one that commits during the
// iteration, must never appear (TestKvIteratorSnapshotScope). Iteration
// order is lexicographic by key (TestKvIteratorOrdering).
//
// Range reads are not automatically phantom-safe. A transaction that
// bases a write on "what the iterator observed" (e.g. a count, or "the
// range was empty") rather than an explicit Get of a specific key may not
// be protected against a concurrent insert into that range, depending on
// whether the backend tracks range reads for conflict detection the same
// way it tracks point reads. TestKvIteratorPhantomCheck exists to
// determine this empirically per backend — check its current result
// before relying on range-read-based invariants for anything where a
// missed concurrent insert would be a real bug, not just a stale read.
//
// # Commutative operations (planned, not yet implemented)
//
// High-contention counter-style operations (e.g. Redis INCR/HINCRBY and
// similar) are not implemented as Tx.Get + Tx.Set + retry-on-conflict —
// that pattern is exactly the worst case for OCC under contention. The
// planned Merge method will append a blind, associative delta via each
// backend's native merge-operator support, resolved lazily at read time.
// A Merge call will NOT participate in a Tx's atomicity, snapshot, or
// conflict tracking, and a batch of Merge calls across several keys will
// NOT be atomic as a group. See MergeOption / WriteHandle for the
// in-progress shape of this API, and do not assume Merge exists or has
// any particular behavior until its own doc comment says otherwise.
//
// # Durability
//
// Commit() returning nil means the write is applied and visible; it does
// NOT yet mean the write is guaranteed durable against an unclean process
// restart; that depends on backend-level configuration (e.g. Badger's
// SyncWrites, SlateDB's AwaitDurable) which is not currently exposed as a
// per-call choice on this interface. If a caller needs "acknowledged
// implies durable" (e.g. job-queue state that must survive a crash), that
// guarantee is not yet available through this package and needs to be
// verified/added before relying on it. Durability under crash is
// explicitly a different property from linearizability and is not
// covered by anything in linearizability_test.go. We do plan to write
// separate fault-injection tests to verify this, however.
//
// # Enforcement
//
// linearizability_test.go is the executable specification for everything
// above: it runs both a randomized concurrent workload and several
// hand-crafted adversarial scenarios (write skew, iterator snapshot
// scope, phantom reads) through Porcupine against every backend
// implementation. If you change what this package guarantees, update the
// corresponding test (and this comment) in the same change. A guarantee
// documented here that isn't checked there will drift silently.
package kv
