package main

import (
	"log"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/kv/redis"
)

func main() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	redis.Serve(db)
}
