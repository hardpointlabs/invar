package mongo

import (
	"context"
	"net"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

// MongoListener implements the main.Listener interface for the MongoDB wire protocol.
// This is a placeholder; the protocol implementation is not yet written.
type MongoListener struct {
	Ln net.Listener
}

func (l *MongoListener) Serve(ctx context.Context, db *badger.DB) error {
	log.Info().Msgf("started mongo listener at %s", l.Ln.Addr())
	go func() {
		<-ctx.Done()
		l.Ln.Close()
	}()
	for {
		conn, err := l.Ln.Accept()
		if err != nil {
			// Listener was closed; treat as clean shutdown.
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go serveConn(conn)
	}
}
