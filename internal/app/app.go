// Package app wires hookrelay's components together and runs them until the
// context is cancelled.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/marcus-girolneto/hookrelay/internal/api"
	"github.com/marcus-girolneto/hookrelay/internal/config"
	"github.com/marcus-girolneto/hookrelay/internal/delivery"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/postgres"
)

// connectTimeout bounds how long startup waits for the database, which
// commonly starts after the service under docker compose or Kubernetes.
const connectTimeout = 60 * time.Second

// Run starts the API server and the delivery engine and blocks until ctx is
// cancelled or a component fails. Shutdown order: readiness flips to failing,
// the HTTP server drains, and the engine finishes in-flight deliveries.
//
// If ready is non-nil, the bound listener address is sent on it once the
// server accepts connections; tests use this with port 0.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger, version string, ready chan<- net.Addr) error {
	pool, err := connect(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.Migrate {
		if err := postgres.Migrate(ctx, pool, logger); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := metrics.New(reg)
	store := postgres.New(pool)
	engine := delivery.New(store, delivery.NewHTTPClient(), cfg.Delivery, m, logger.With("component", "delivery"))
	apiServer := api.New(store, m, logger.With("component", "api"))

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.Handle("/", apiServer.Handler())
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		logger.LogAttrs(ctx, slog.LevelInfo, "hookrelay started",
			slog.String("version", version),
			slog.String("addr", ln.Addr().String()),
			slog.Int("concurrency", cfg.Delivery.Concurrency))
		if ready != nil {
			ready <- ln.Addr()
		}
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		return engine.Run(gctx)
	})
	g.Go(func() error {
		<-gctx.Done()
		logger.LogAttrs(ctx, slog.LevelInfo, "shutting down")
		apiServer.SetDraining()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})
	err = g.Wait()
	logger.LogAttrs(context.WithoutCancel(ctx), slog.LevelInfo, "hookrelay stopped")
	return err
}

func connect(ctx context.Context, cfg config.Config, logger *slog.Logger) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pcfg.MaxConns = int32(cfg.MaxDBConns) //nolint:gosec // bounded by config.Validate.
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	delay := 250 * time.Millisecond
	for {
		err := pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		logger.LogAttrs(ctx, slog.LevelWarn, "waiting for database", slog.Any("error", err))
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("connect to database: %w", err)
		case <-time.After(delay):
			delay = min(delay*2, 5*time.Second)
		}
	}
}
