// This contains code for in-memory tracking of clients 'watching' various Redis data
// structures for changes via blocking Redis command variants.
package common

import (
	"container/list"
	"context"
	"sync"
)

// PopResult is the value delivered to a blocked waiter once a matching write lands.
type PopResult struct {
	Key    string
	Member []byte
	Score  float64
}

// waiter is an in-flight blocked client waiting for a pop result on one or more keys.
type waiter struct {
	ch      chan PopResult            // buffered cap-1; writer sends exactly once
	elems   map[string]*list.Element // position in each key's FIFO queue for O(1) removal
	wantMin bool                     // true=BZPOPMIN, false=BZPOPMAX
}

// Claim records a single decision made inside a DbOp: "this waiter will receive this
// result".  The Claimer interface lets DispatchPendingOps release claims generically
// on failure without knowing about zsets.
type Claim struct {
	w       *waiter
	result  PopResult
	keys    []string // keys from which the waiter was removed when claimed
	WantMin bool     // true=BZPOPMIN, false=BZPOPMAX; informs the DbOp which element to pop
}

// Claimer is implemented by any DbOp result that carried waiter claims.  If a batch
// fails after the DbOp ran, DispatchPendingOps calls ReleaseClaims so the waiters are
// pushed back to the front of their respective queues.
type Claimer interface {
	Claims() []*Claim
}

// WatchRegistry is a process-wide, in-memory registry that maps a public Redis key to
// the FIFO queue of clients currently blocked waiting for a pop on that key.
//
// Invariants:
//   - The longest-waiting client on any key is always list.Front().
//   - A single waiter may appear in multiple key queues (BZPOPMIN key1 key2 …).
//   - All mutations are guarded by mu.
type WatchRegistry struct {
	mu      sync.Mutex
	waiters map[string]*list.List // key → FIFO list of *waiter
}

// NewWatchRegistry allocates a ready-to-use registry.
func NewWatchRegistry() *WatchRegistry {
	return &WatchRegistry{waiters: make(map[string]*list.List)}
}

// register adds w to the back of every key's queue.  Caller must hold r.mu.
func (r *WatchRegistry) registerLocked(w *waiter, keys []string) {
	for _, k := range keys {
		l, ok := r.waiters[k]
		if !ok {
			l = list.New()
			r.waiters[k] = l
		}
		elem := l.PushBack(w)
		w.elems[k] = elem
	}
}

// removeAllLocked removes w from every key queue it appears in and returns true if it
// was still present everywhere (i.e. no claim raced us).  Caller must hold r.mu.
func (r *WatchRegistry) removeAllLocked(w *waiter) bool {
	if len(w.elems) == 0 {
		return false
	}
	for k, elem := range w.elems {
		l := r.waiters[k]
		if l == nil {
			return false // shouldn't happen
		}
		l.Remove(elem)
		if l.Len() == 0 {
			delete(r.waiters, k)
		}
	}
	w.elems = nil
	return true
}

// TryClaim inspects the front of the queue for publicKey and, if a waiter is present,
// removes it from ALL of its registered key queues and returns a *Claim that the caller
// must later either Wake (on commit success) or ReleaseFront (on any failure).
//
// Returns nil when there are no waiters for the key.
//
// This must be called from inside a DbOp — i.e. while the transaction is still in
// progress — so that the element consumed on behalf of the waiter is removed atomically
// with the persisted write.
func (r *WatchRegistry) TryClaim(publicKey string) *Claim {
	r.mu.Lock()
	defer r.mu.Unlock()

	l, ok := r.waiters[publicKey]
	if !ok || l.Len() == 0 {
		return nil
	}

	front := l.Front()
	w := front.Value.(*waiter)

	// Remove this waiter from every key it registered under.
	keys := make([]string, 0, len(w.elems))
	for k, elem := range w.elems {
		keys = append(keys, k)
		ql := r.waiters[k]
		ql.Remove(elem)
		if ql.Len() == 0 {
			delete(r.waiters, k)
		}
	}
	w.elems = nil // mark as claimed

	return &Claim{w: w, keys: keys, WantMin: w.wantMin}
}

// SetResult attaches the popped result to the claim.  Call this before Wake.
func (c *Claim) SetResult(res PopResult) {
	c.result = res
}

// Wake delivers the claimed result to the waiting goroutine.  Call from a WireOp,
// i.e. only after Commit() has succeeded.
func (c *Claim) Wake() {
	c.w.ch <- c.result
}

// ReleaseFront pushes the waiter back to the FRONT of each key's queue so it remains
// the longest-waiting client.  Call when a transaction (or a later op in the same
// batch) fails after TryClaim already ran.
func (r *WatchRegistry) ReleaseFront(c *Claim) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-initialise elems so registerLocked can repopulate it.
	c.w.elems = make(map[string]*list.Element)

	for _, k := range c.keys {
		l, ok := r.waiters[k]
		if !ok {
			l = list.New()
			r.waiters[k] = l
		}
		elem := l.PushFront(c.w)
		c.w.elems[k] = elem
	}
}

// Block registers the caller as a waiter on all listed keys, then blocks until one of
// the listed keys delivers a pop result or ctx is cancelled.
//
// wantMin should be true for BZPOPMIN (pop the lowest score) and false for BZPOPMAX.
//
// Returns (result, true) when a result was received, or (PopResult{}, false) when the
// context expired and no write had already claimed this waiter.
//
// Each connection's goroutine (provided by redcon) calls this directly, so blocking
// here does not stall other connections.
func (r *WatchRegistry) Block(ctx context.Context, keys []string, wantMin bool) (PopResult, bool) {
	w := &waiter{
		ch:      make(chan PopResult, 1),
		elems:   make(map[string]*list.Element),
		wantMin: wantMin,
	}

	r.mu.Lock()
	r.registerLocked(w, keys)
	r.mu.Unlock()

	select {
	case res := <-w.ch:
		return res, true
	case <-ctx.Done():
		r.mu.Lock()
		removed := r.removeAllLocked(w)
		r.mu.Unlock()

		if removed {
			// We cleanly removed ourselves — genuinely timed out.
			return PopResult{}, false
		}
		// Lost the race: a writer already claimed us and is about to send
		// (or has already sent) into w.ch.  Drain it rather than discarding
		// a real element that was already removed from the sorted set.
		return <-w.ch, true
	}
}
