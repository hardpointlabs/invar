//go:build slatedb

package kv

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

type SlateDBOpts struct {
	Path           string
	ObjectStoreURL string
	Settings       *slatedb.Settings // optional; applied to the DbBuilder when non-nil
}

func NewSlateDB(opts SlateDBOpts) (KeyValueStore, error) {
	store, err := slatedb.ObjectStoreResolve(opts.ObjectStoreURL)
	if err != nil {
		return nil, err
	}
	builder := slatedb.NewDbBuilder(opts.Path, store)
	defer builder.Destroy()
	if opts.Settings != nil {
		if err := builder.WithSettings(opts.Settings); err != nil {
			store.Destroy()
			return nil, err
		}
	}
	db, err := builder.Build()
	if err != nil {
		store.Destroy()
		return nil, err
	}
	return &slateKvImpl{db: db, store: store}, nil
}

func InMemorySlateDB(t *testing.T) KeyValueStore {
	t.Helper()
	kvs, err := NewSlateDB(SlateDBOpts{Path: "test-db", ObjectStoreURL: "memory:///"})
	if err != nil {
		t.Fatal(err)
	}
	return kvs
}

type slateKvImpl struct {
	db    *slatedb.Db
	store *slatedb.ObjectStore
}

func (s *slateKvImpl) NewEntry(key []byte, value []byte) Entry {
	return &slateEntry{key: key, val: value}
}

func (s *slateKvImpl) Begin(mutating bool) Tx {
	tx, err := s.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		return &slateTx{err: err}
	}
	return &slateTx{tx: tx}
}

func (s *slateKvImpl) Update(fn func(tx Tx) error) error {
	tx := s.Begin(true)
	defer tx.Discard()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *slateKvImpl) Read(fn func(tx Tx) (any, error)) (any, error) {
	tx := s.Begin(false)
	defer tx.Discard()
	return fn(tx)
}

func (s *slateKvImpl) Badger() *badger.DB {
	return nil
}

func (s *slateKvImpl) Merge(key []byte, operand []byte, opts ...MergeOption) (WriteHandle, error) {
	return nil, nil
}

func (s *slateKvImpl) Sync() error {
	return s.db.Flush()
}

func (s *slateKvImpl) Destroy() error {
	return s.deleteScanned(func() (*slatedb.DbIterator, error) {
		return s.db.Scan(slatedb.KeyRange{})
	})
}

func (s *slateKvImpl) DropPrefix(prefix []byte) error {
	return s.deleteScanned(func() (*slatedb.DbIterator, error) {
		return s.db.ScanPrefix(prefix, slatedb.KeyRange{})
	})
}

func (s *slateKvImpl) Close() error {
	var err error
	if e := s.db.Shutdown(); e != nil {
		err = e
	}
	s.db.Destroy()
	s.store.Destroy()
	return err
}

func (s *slateKvImpl) deleteScanned(scan func() (*slatedb.DbIterator, error)) error {
	iter, err := scan()
	if err != nil {
		return err
	}
	defer iter.Destroy()

	var keys [][]byte
	for {
		kv, err := iter.Next()
		if err != nil {
			return err
		}
		if kv == nil {
			break
		}
		keys = append(keys, append([]byte{}, kv.Key...))
	}
	if len(keys) == 0 {
		return nil
	}

	tx, err := s.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		return err
	}
	defer tx.Destroy()
	for _, k := range keys {
		if err := tx.Delete(k); err != nil {
			return err
		}
	}
	_, err = tx.Commit()
	return mapSlateError(err)
}

type slateEntry struct {
	key  []byte
	val  []byte
	meta byte
	ttl  time.Duration
}

func (e *slateEntry) Key() []byte {
	return e.key
}

func (e *slateEntry) Value() []byte {
	return e.val
}

func (e *slateEntry) Metadata(data byte) Entry {
	e.meta = data
	return e
}

func (e *slateEntry) MetadataByte() byte {
	return e.meta
}

func (e *slateEntry) TTL(duration time.Duration) Entry {
	e.ttl = duration
	return e
}

func (e *slateEntry) TTLValue() time.Duration {
	return e.ttl
}

type slateItem struct {
	key      []byte
	value    []byte
	meta     byte
	expireTs *int64
}

func (i *slateItem) Key() []byte {
	return i.key
}

func (i *slateItem) TTL() time.Duration {
	if i.expireTs == nil {
		return 0
	}
	remaining := time.Duration(*i.expireTs-time.Now().UnixMilli()) * time.Millisecond
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (i *slateItem) Metadata() byte {
	return i.meta
}

func (i *slateItem) ExpiresAt() uint64 {
	if i.expireTs == nil {
		return 0
	}
	return uint64(*i.expireTs / 1000)
}

func (i *slateItem) Value() ([]byte, error) {
	return append([]byte{}, i.value...), nil
}

type slateTx struct {
	tx   *slatedb.DbTransaction
	err  error
	done bool
}

func (t *slateTx) Get(key []byte) (Item, error) {
	if t.err != nil {
		return nil, t.err
	}
	kv, err := t.tx.GetKeyValue(key)
	if err != nil {
		return nil, mapSlateError(err)
	}
	if kv == nil {
		return nil, ErrKeyNotFound
	}
	val, meta := decodeValue(kv.Value)
	return &slateItem{
		key:      append([]byte{}, key...),
		value:    append([]byte{}, val...),
		meta:     meta,
		expireTs: kv.ExpireTs,
	}, nil
}

func (t *slateTx) Set(entry Entry) error {
	if t.err != nil {
		return t.err
	}
	stored := encodeValue(entry.MetadataByte(), entry.Value())
	if ttl := entry.TTLValue(); ttl > 0 {
		return t.tx.PutWithOptions(entry.Key(), stored, slatedb.PutOptions{
			Ttl: slatedb.TtlExpireAfterTicks{Field0: uint64(ttl.Milliseconds())},
		})
	}
	return t.tx.Put(entry.Key(), stored)
}

func (t *slateTx) Delete(key []byte) error {
	if t.err != nil {
		return t.err
	}
	return t.tx.Delete(key)
}

func (t *slateTx) NewIterator(prefix []byte) KeyValueIterator {
	if t.err != nil {
		return &slateIterator{err: t.err}
	}
	iter, err := t.tx.ScanPrefix(prefix, slatedb.KeyRange{})
	if err != nil {
		return &slateIterator{err: err}
	}
	return &slateIterator{iter: iter}
}

func (t *slateTx) Commit() error {
	if t.err != nil {
		return t.err
	}
	if t.done {
		return nil
	}
	t.done = true
	_, err := t.tx.Commit()
	return mapSlateError(err)
}

func (t *slateTx) Discard() {
	if t.err != nil || t.done {
		return
	}
	t.done = true
	_ = t.tx.Rollback()
}

type slateIterator struct {
	iter *slatedb.DbIterator
	err  error
	cur  Item
}

func (it *slateIterator) Next() bool {
	if it.err != nil {
		return false
	}
	kv, err := it.iter.Next()
	if err != nil {
		it.err = err
		return false
	}
	if kv == nil {
		return false
	}
	val, meta := decodeValue(kv.Value)
	it.cur = &slateItem{
		key:      append([]byte{}, kv.Key...),
		value:    append([]byte{}, val...),
		meta:     meta,
		expireTs: kv.ExpireTs,
	}
	return true
}

func (it *slateIterator) Item() Item {
	return it.cur
}

func (it *slateIterator) Err() error {
	return it.err
}

func (it *slateIterator) Close() error {
	if it.iter != nil {
		it.iter.Destroy()
		it.iter = nil
	}
	return nil
}

func encodeValue(meta byte, val []byte) []byte {
	out := make([]byte, 1, len(val)+1)
	out[0] = meta
	return append(out, val...)
}

func decodeValue(stored []byte) (val []byte, meta byte) {
	if len(stored) == 0 {
		return nil, 0
	}
	return stored[1:], stored[0]
}

func mapSlateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, slatedb.ErrErrorTransaction) {
		return ErrConflict
	}
	return ErrUndefined
}
