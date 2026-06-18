package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
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

type Metrics struct {
	db *badger.DB
}

func (m *Metrics) Start(ctx context.Context) error {
	go func() {
		for {
			lsm, vlog := m.db.Size()
			lsmGauge.Set(float64(lsm))
			vlogGauge.Set(float64(vlog))
			time.Sleep(60 * time.Second)
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: "localhost:2112", Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		srv.Shutdown(shutdownCtx)
	}()
	log.Info().Msg("starting prometheus metrics server on localhost:2112")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func CreateMetrics(db *badger.DB) *Metrics {
	return &Metrics{db: db}
}
