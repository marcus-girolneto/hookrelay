// Command receiver is an example webhook consumer that verifies hookrelay
// signatures and deduplicates deliveries. It exists for the docker compose
// demo and as a reference for integrators.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/receiver"
	"github.com/marcus-girolneto/hookrelay/signature"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "receiver:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("receiver", flag.ContinueOnError)
	addr := fs.String("addr", envOr("RECEIVER_ADDR", ":8081"), "listen address")
	secret := fs.String("secret", os.Getenv("RECEIVER_SECRET"), "endpoint signing secret (whsec_...)")
	failRate := fs.Float64("fail-rate", 0, "fraction of first-time deliveries answered with 503, to demo retries")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if v := os.Getenv("RECEIVER_FAIL_RATE"); v != "" && *failRate == 0 {
		if err := fs.Set("fail-rate", v); err != nil {
			return fmt.Errorf("RECEIVER_FAIL_RATE: %w", err)
		}
	}
	verifier, err := signature.NewVerifier(*secret)
	if err != nil {
		return fmt.Errorf("secret: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()
	mux.Handle("POST /webhooks", receiver.NewHandler(verifier, logger, 10_000, receiver.WithFailureRate(*failRate, nil)))
	// /gone lets the demo show endpoint auto-disable on 410.
	mux.HandleFunc("POST /gone", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusGone) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() {
		logger.Info("receiver listening", slog.String("addr", *addr), slog.Float64("fail_rate", *failRate))
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
