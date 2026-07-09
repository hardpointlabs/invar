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
	NewEntry() Entry
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

type Tx interface {
	Get(key []byte) (Item, error)
	Set(entry Entry) error
	Delete(key []byte) error
	NewIterator(prefix []byte) *KeyValueIterator
	Commit() error
	Discard()
}

type Entry interface {
	Key() []byte
	Metadata(data byte) Entry
	TTL(duration time.Duration) Entry
}

type Item interface {
	Key() []byte
	TTL() time.Duration
	// Returns the value of this entry as a freshly copied byte slice
	Value() ([]byte, error)
}

type KeyValueIterator interface {
	Close() error
}

type QueuedOp struct {
	// The DB side: runs inside the transaction, returns an opaque result or error
	DbOp func(tx Tx) (any, error)
	// The wire side: runs after commit, consumes the result
	WireOp func(conn redcon.Conn, result any, err error)
	// Flags whether this op needs to run in a write transaction
	IsMutating bool
}
