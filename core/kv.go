package core

import "github.com/dgraph-io/badger/v4"

type KV interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
}

type Listener interface {
	Serve(db *badger.DB)
}
