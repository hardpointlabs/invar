package kv

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type BadgerOpts struct {
	DataDir string // The directory where data is persisted. Must be writable by this process
	Logger  zerolog.Logger
}

// --- Badger KeyValueStore implementation ---
type badgerKvImpl struct {
	db *badger.DB
}

func (b badgerKvImpl) NewEntry(key []byte, value []byte) Entry {
	return &badgerEntry{e: badger.NewEntry(key, value), key: key, val: value, meta: 0}
}

func (b badgerKvImpl) Begin(mutating bool) Tx {
	return &badgerTx{tx: b.db.NewTransaction(mutating)}
}

func (b badgerKvImpl) Update(fn func(tx Tx) error) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return fn(&badgerTx{tx: txn})
	})
}

func (b badgerKvImpl) Read(fn func(tx Tx) (any, error)) (any, error) {
	var result any
	err := b.db.View(func(txn *badger.Txn) error {
		badgerTx := &badgerTx{tx: txn}
		val, err := fn(badgerTx)
		result = val
		return err
	})
	return result, err
}

func (b badgerKvImpl) Merge(key []byte, operand []byte, opts ...MergeOption) (WriteHandle, error) {
	return nil, nil
}

func (b badgerKvImpl) Sync() error {
	// TODO map Badger error to a public type
	return b.db.Sync()
}

func (b badgerKvImpl) Destroy() error {
	// TODO map Badger error to a public type
	return b.db.DropAll()
}

func (b badgerKvImpl) DropPrefix(prefix []byte) error {
	// TODO map Badger error to a public type
	return b.db.DropPrefix(prefix)
}

func (b badgerKvImpl) Close() error {
	return b.db.Close()
}

type badgerIterator struct {
	iter    *badger.Iterator
	prefix  []byte
	started bool
}

// Badger
func (it *badgerIterator) Next() bool {
	if !it.started {
		it.iter.Rewind()
		it.started = true
	} else {
		it.iter.Next()
	}
	return it.iter.ValidForPrefix(it.prefix)
}
func (it *badgerIterator) Item() Item { return &badgerItem{item: it.iter.Item()} }
func (it *badgerIterator) Err() error { return nil } // Badger has no mid-scan error state
func (it *badgerIterator) Close() error {
	it.iter.Close()
	return nil
}

// --- Logging adapter ---
// BadgerZerologAdapter routes Badger logs through Zerolog.
type badgerZerologAdapter struct {
	Logger zerolog.Logger
}

// Errorf handles Badger Error logs
func (b *badgerZerologAdapter) Errorf(format string, v ...interface{}) {
	b.Logger.Error().Msg(fmt.Sprintf(format, v...))
}

// Warningf handles Badger Warning logs
func (b *badgerZerologAdapter) Warningf(format string, v ...interface{}) {
	b.Logger.Warn().Msg(fmt.Sprintf(format, v...))
}

// Infof handles Badger Info logs
func (b *badgerZerologAdapter) Infof(format string, v ...interface{}) {
	b.Logger.Info().Msg(fmt.Sprintf(format, v...))
}

// Debugf handles Badger Debug logs (requires Debug level enabled)
func (b *badgerZerologAdapter) Debugf(format string, v ...interface{}) {
	b.Logger.Debug().Msg(fmt.Sprintf(format, v...))
}

// --- Badger Settable Entry implementation ---
type badgerEntry struct {
	e    *badger.Entry
	key  []byte
	val  []byte
	meta byte
	ttl  time.Duration
}

func (e *badgerEntry) Key() []byte {
	return e.key
}

func (e *badgerEntry) Value() []byte {
	return e.val
}

func (e *badgerEntry) MetadataByte() byte {
	return e.meta
}

func (e *badgerEntry) Metadata(data byte) Entry {
	e.e = e.e.WithMeta(data)
	e.meta = data
	return e
}

func (e *badgerEntry) TTL(duration time.Duration) Entry {
	e.e = e.e.WithTTL(duration)
	e.ttl = duration
	return e
}

func (e *badgerEntry) TTLValue() time.Duration {
	return e.ttl
}

type badgerItem struct {
	item *badger.Item
}

func (i badgerItem) Key() []byte {
	return i.item.Key()
}

func (i badgerItem) TTL() time.Duration {
	return time.Duration(i.item.ExpiresAt())
}

func (i badgerItem) Metadata() byte {
	return i.item.UserMeta()
}

func (i badgerItem) ExpiresAt() uint64 {
	return i.item.ExpiresAt()
}

func (i badgerItem) Value() ([]byte, error) {
	return copyItemValue(i.item)
}

// this is such a common idiom that we keep a utility function for it
func copyItemValue(item *badger.Item) ([]byte, error) {
	var out []byte
	err := item.Value(func(val []byte) error {
		out = append([]byte{}, val...)
		return nil
	})
	return out, err
}

// --- Badger Transaction implementation ---
type badgerTx struct {
	tx *badger.Txn
}

func (t badgerTx) Get(key []byte) (Item, error) {
	item, err := t.tx.Get(key)
	if err != nil {
		return nil, mapError(err)
	}

	return &badgerItem{item: item}, nil
}

func mapError(err error) error {
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	return ErrUndefined
}

func (t badgerTx) Set(entry Entry) error {
	be := badger.NewEntry(entry.Key(), entry.Value()).WithMeta(entry.MetadataByte())
	if ttl := entry.TTLValue(); ttl > 0 {
		be = be.WithTTL(ttl)
	}
	return t.tx.SetEntry(be)
}

func (t badgerTx) Delete(key []byte) error {
	return t.tx.Delete(key)
}

func (t badgerTx) NewIterator(prefix []byte) KeyValueIterator {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	badgerIt := t.tx.NewIterator(opts)
	var kvIt KeyValueIterator = &badgerIterator{iter: badgerIt, prefix: prefix}
	return kvIt
}

func (t badgerTx) Commit() error {
	return t.tx.Commit()
}

func (t badgerTx) Discard() {
	t.tx.Discard()
}

// --- Factories ---

func NewBadger(opts BadgerOpts) KeyValueStore {
	// ---- BadgerDB ----
	badgerOpts := badger.DefaultOptions(opts.DataDir)
	adapter := &badgerZerologAdapter{Logger: opts.Logger}
	badgerOpts.Logger = adapter
	db, err := badger.Open(badgerOpts)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open badger database")
	}
	return badgerKvImpl{db: db}
}

func InMemoryBadger(t *testing.T) KeyValueStore {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return badgerKvImpl{db: db}
}

func WrapBadger(db *badger.DB) KeyValueStore {
	return badgerKvImpl{db: db}
}
