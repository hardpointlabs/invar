package kv

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

var (
	ErrKeyNotFound = errors.New("Key not found")
	ErrConflict    = errors.New("Conflict")
	ErrUndefined   = errors.New("Undefined")
)

// Abstraction over a transactional LSM Tree implementation.
type KeyValueStore interface {
	NewEntry(key []byte, value []byte) Entry
	// Create a new manually managed transaction. It's critical to call Discard() (normally in a `defer` statement after initialization)
	// To ensure any resources are cleaned up after use
	Begin(mutating bool) Tx
	Update(fn func(tx Tx) error) error
	Read(fn func(tx Tx) (any, error)) (any, error)
	// It's critical to call this to clean up all DB resources and
	// to ensure all data is persisted to durable storage
	Close() error
	// DEPRECATED! This accessor is purely for smoothing transition
	// to the KeyValueStore interface
	Badger() *badger.DB
	// Merge appends a commutative delta to key using the store's globally
	// registered MergeFunc (see Options.MergeFunc). This is a blind write:
	// it never conflicts with concurrent Merge calls on the same key, and
	// does NOT participate in any Tx's atomicity, snapshot isolation, or
	// conflict tracking. A series of Merge calls across multiple keys is
	// NOT atomic as a group — a failure partway through leaves earlier
	// keys already updated. Reading a merge-written key back via Tx.Get()
	// is [transparent / not supported, returns ErrRequiresMergeHandle —
	// confirm and delete one] on both backends.
	Merge(key []byte, operand []byte, opts ...MergeOption) (WriteHandle, error)
}

// Generic transaction object
type Tx interface {
	Get(key []byte) (Item, error)
	Set(entry Entry) error
	Delete(key []byte) error
	NewIterator(prefix []byte) *KeyValueIterator
	Commit() error
	Discard()
}

// TODO make this work 💩
type MergeOption struct{}

// func WithMergeTTL(d time.Duration) MergeOption // maps to SlateDB's MergeOptions.TTL
// func WithAwaitDurable(await bool) MergeOption  // maps to SlateDB's WriteOptions.AwaitDurable

type WriteHandle interface {
	Wait() error // blocks until applied, and durable if WithAwaitDurable was set
}

// Key-value pair, with optional TTL and Metadata, to write to the store
type Entry interface {
	Key() []byte
	Value() []byte
	Metadata(data byte) Entry
	MetadataByte() byte
	TTL(duration time.Duration) Entry
	// TTLValue returns the TTL set via TTL(), or 0 if none was set.
	TTLValue() time.Duration
}

// Immutable key-value pair read from the store
type Item interface {
	Key() []byte
	TTL() time.Duration
	// Metadata returns the metadata byte set when the entry was written.
	Metadata() byte
	// ExpiresAt returns the absolute expiry time as Unix seconds, or 0 if the
	// entry never expires.
	ExpiresAt() uint64
	// Returns the value of this entry as a freshly copied byte slice
	Value() ([]byte, error)
}

// Generic ordered iterator of keys in the store (not complete)
type KeyValueIterator interface {
	// Next advances the iterator and reports whether an item is available.
	// Must be called before the first Item(). Returns false when the
	// prefix range is exhausted OR on error — call Err() to distinguish
	// the two.
	Next() bool
	// Item returns the current entry. Valid only after Next() returns true.
	Item() Item
	// Err returns any error encountered during iteration. Always check
	// this after a Next() that returns false.
	Err() error
	Close() error
}
