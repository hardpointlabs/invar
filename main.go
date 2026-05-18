package main

import (
	"os"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/kv/redis"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	defer db.Close()
	redis.Serve(db)
}
