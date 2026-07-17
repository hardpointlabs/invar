package redis

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func inMemDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
