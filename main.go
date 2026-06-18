package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	arg "github.com/alexflint/go-arg"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/config"
	"github.com/hardpointlabs/invar/metrics"
	mongolistener "github.com/hardpointlabs/invar/mongo"
	"github.com/hardpointlabs/invar/redis"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// Listener is implemented by each protocol backend.
type Listener interface {
	Serve(ctx context.Context, db *badger.DB) error
}

// listenAddr is a parsed network address that understands "tcp:6379" and
// "unix:/var/run/invar.sock" syntax.
type listenAddr struct {
	network string
	address string
}

// UnmarshalText satisfies encoding.TextUnmarshaler, which go-arg uses for
// custom flag types.
func (a *listenAddr) UnmarshalText(b []byte) error {
	s := string(b)
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("listen address %q must be in the form tcp:<addr> or unix:<path>", s)
	}
	switch parts[0] {
	case "tcp", "unix":
	default:
		return fmt.Errorf("unsupported network %q: must be tcp or unix", parts[0])
	}
	a.network = parts[0]
	a.address = parts[1]
	return nil
}

func (a listenAddr) String() string {
	return a.network + ":" + a.address
}

func (a listenAddr) Listen() (net.Listener, error) {
	return net.Listen(a.network, a.address)
}

// ---- subcommand argument structs ----

type versionCmd struct{}

type redisCmd struct {
	ListenAddr listenAddr `arg:"--listen-addr" default:"tcp::6379" help:"listen address (tcp:<addr> or unix:<path>)"`
}

type mongoCmd struct {
	ListenAddr listenAddr `arg:"--listen-addr" default:"tcp::27017" help:"listen address (tcp:<addr> or unix:<path>)"`
}

// ---- top-level args ----

type args struct {
	DataDir string `arg:"--data-dir" default:"/tmp/badger" help:"path to BadgerDB storage directory"`
	Pprof   bool   `arg:"--pprof" help:"start pprof HTTP server on localhost:6060"`
	Metrics bool   `arg:"--metrics" help:"start Prometheus metrics server on localhost:2112"`

	Version *versionCmd `arg:"subcommand:version" help:"print version information and exit"`
	Redis   *redisCmd   `arg:"subcommand:redis"   help:"start with RESP-protocol compatibility"`
	Mongo   *mongoCmd   `arg:"subcommand:mongo"   help:"start the MongoDB Wire Protocol compatibility"`
}

func (args) Description() string { return "Invar - a lightweight, durable document store" }

// ---- Prometheus gauges ----

func main() {
	var a args
	p := arg.MustParse(&a)

	// Handle version subcommand before touching the DB.
	if a.Version != nil {
		fmt.Printf("invar version %s (commit %s)\n", config.Version, config.Commit)
		return
	}

	// Ensure a listener subcommand was provided.
	if a.Redis == nil && a.Mongo == nil {
		p.Fail("a subcommand is required: version, redis, or mongo")
	}

	// ---- logging ----
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	adapter := &BadgerZerologAdapter{Logger: logger}

	// ---- BadgerDB ----
	opts := badger.DefaultOptions(a.DataDir)
	opts.Logger = adapter
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open badger database")
	}
	defer db.Close()

	// ---- context / signal handling ----
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	g, groupCtx := errgroup.WithContext(ctx)

	// ---- optional: pprof ----
	if a.Pprof {
		g.Go(func() error {
			log.Info().Msg("starting pprof server on localhost:6060")
			srv := &http.Server{Addr: "localhost:6060"}
			go func() {
				<-groupCtx.Done()
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				srv.Shutdown(shutdownCtx)
			}()
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				return err
			}
			return nil
		})
	}

	// ---- optional: Prometheus metrics ----
	if a.Metrics {
		g.Go(func() error {
			m := metrics.CreateMetrics(db)
			return m.Start(groupCtx)
		})
	}

	// ---- protocol listener ----
	var l Listener
	var ln net.Listener

	switch {
	case a.Redis != nil:
		ln, err = a.Redis.ListenAddr.Listen()
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open redis listen socket")
		}
		l = &redis.RedisListener{Ln: ln}

	case a.Mongo != nil:
		ln, err = a.Mongo.ListenAddr.Listen()
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open mongo listen socket")
		}
		l = &mongolistener.MongoListener{Ln: ln}
	}

	g.Go(func() error {
		return l.Serve(groupCtx, db)
	})

	// ---- signal → cancel ----
	go func() {
		select {
		case sig := <-sigChan:
			log.Info().Str("signal", sig.String()).Msg("received signal, initiating shutdown")
			cancel()
		case <-groupCtx.Done():
		}
	}()

	if err := g.Wait(); err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("process stopped with error")
	}
	log.Info().Msg("all processes shut down gracefully")
}
