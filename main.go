package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hardpointlabs/invar/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

var (
	lsmGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "badger_lsm_size_bytes",
		Help: "BadgerDB LSM tree size in bytes",
	})
	vlogGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "badger_vlog_size_bytes",
		Help: "BadgerDB value log size in bytes",
	})
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	adapter := &BadgerZerologAdapter{Logger: logger}

	opts := badger.DefaultOptions("/tmp/badger")
	opts.Logger = adapter
	db, err := badger.Open(opts)

	if err != nil {
		log.Fatal().Err(err).Msg("failed to open badger database")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	g, groupCtx := errgroup.WithContext(ctx)

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

	g.Go(func() error {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: "localhost:2112", Handler: mux}
		go func() {
			<-groupCtx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			srv.Shutdown(shutdownCtx)
		}()
		log.Info().Msg("starting prometheus metrics server on localhost:2112")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	go func() {
		for {
			lsm, vlog := db.Size()
			lsmGauge.Set(float64(lsm))
			vlogGauge.Set(float64(vlog))
			time.Sleep(60 * time.Second)
		}
	}()

	g.Go(func() error {
		return redis.Serve(groupCtx, db)
	})

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
