package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/redis"
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

	go func() {
		log.Info().Msg("starting pprof server on localhost:6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Fatal().Err(err).Msg("pprof server failed")
		}
	}()

	redis.Serve(db)
}
