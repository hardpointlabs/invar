package kv

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
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

// Key-value pair, with optional TTL and Metadata, to write to the store
type Entry interface {
	Key() []byte
	Metadata(data byte) Entry
	TTL(duration time.Duration) Entry
}

// Immutable key-value pair read from the store
type Item interface {
	Key() []byte
	TTL() time.Duration
	// Returns the value of this entry as a freshly copied byte slice
	Value() ([]byte, error)
}

// Generic ordered iterator of keys in the store (not complete)
type KeyValueIterator interface {
	Close() error
}

// A QueuedOp separates operations on the KeyValueStore from subsequent wire operations
// They're declared lazily so they can be either executed straight away, or enqueued
// and run in a batch if required by the caller. The lifecycle of the Tx instance is
// managed outside of the QueuedOp.
type QueuedOp struct {
	// The DB side: runs inside the transaction, returns an opaque result or error
	DbOp func(tx Tx) (any, error)
	// The wire side: runs after commit, consumes the result
	WireOp func(conn redcon.Conn, result any, err error)
	// Flags whether this op needs to run in a write transaction
	IsMutating bool
}
