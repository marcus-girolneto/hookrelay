// Command hookrelay runs the webhook delivery service: an HTTP API for
// registering endpoints and publishing events, and a worker pool that
// delivers signed webhooks with retries.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/marcus-girolneto/hookrelay/internal/app"
	"github.com/marcus-girolneto/hookrelay/internal/config"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cfg, err := config.Load(os.Args[1:], os.Getenv, os.Stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return
	case err != nil:
		fmt.Fprintln(os.Stderr, "hookrelay:", err)
		os.Exit(2)
	case cfg.ShowVersion:
		fmt.Println("hookrelay", version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg, logger, version, nil); err != nil {
		logger.Error("fatal", slog.Any("error", err))
		stop()
		os.Exit(1)
	}
}
