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

func (b badgerKvImpl) NewEntry() Entry {
	return nil
}

func (b badgerKvImpl) Begin(mutating bool) Tx {
	return nil
}

func (b badgerKvImpl) Update(fn func(tx Tx) error) error {
	b.db.Update(func(txn *badger.Txn) error {
		return fn(&badgerTx{tx: txn})
	})
	return nil
}

func (b badgerKvImpl) Read(fn func(tx Tx) (any, error)) (any, error) {
	err := b.db.View(func(txn *badger.Txn) error {
		badgerTx := &badgerTx{tx: txn}
		_, err := fn(badgerTx)
		return err
	})
	return nil, err
}

func (b badgerKvImpl) Badger() *badger.DB {
	return b.db
}

func (b badgerKvImpl) Close() error {
	return b.db.Close()
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
	e   *badger.Entry
	key []byte
	val []byte
}

func (e badgerEntry) Key() []byte {
	return e.key
}

func (e badgerEntry) Metadata(data byte) Entry {
	return nil
}

func (e badgerEntry) TTL(duration time.Duration) Entry {
	return nil
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

func (i badgerItem) Value() ([]byte, error) {
	return copyItemValue(i.item)
}

// this is such a common idiom that we expose a utility function for it
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
	return t.tx.SetEntry(entry.(*badgerEntry).e)
}

func (t badgerTx) Delete(key []byte) error {
	return t.tx.Delete(key)
}

func (t badgerTx) NewIterator(prefix []byte) *KeyValueIterator {
	return nil
}

func (t badgerTx) Commit() error {
	return nil
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
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return badgerKvImpl{db: db}
}
