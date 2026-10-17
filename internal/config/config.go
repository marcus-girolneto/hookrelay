// Package config parses hookrelay's configuration from command-line flags,
// with every flag also settable through a HOOKRELAY_* environment variable.
// Flags take precedence over the environment.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/delivery"
)

// EnvPrefix is prepended to the upper-cased, underscored flag name to form
// the environment variable name, e.g. -database-url -> HOOKRELAY_DATABASE_URL.
const EnvPrefix = "HOOKRELAY_"

const maxDBConns = 10000

// Config is the complete service configuration.
type Config struct {
	Addr            string
	DatabaseURL     string
	MaxDBConns      int
	Migrate         bool
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	Delivery        delivery.Config
	ShowVersion     bool
}

// Load parses args (without the program name) using getenv for defaults.
// It returns flag.ErrHelp when -h or -help is requested.
func Load(args []string, getenv func(string) string, usage io.Writer) (Config, error) {
	cfg := Config{Delivery: delivery.DefaultConfig()}
	d := &cfg.Delivery

	fs := flag.NewFlagSet("hookrelay", flag.ContinueOnError)
	fs.SetOutput(usage)
	fs.StringVar(&cfg.Addr, "addr", ":8080", "HTTP listen address for the API and /metrics")
	fs.StringVar(&cfg.DatabaseURL, "database-url", "", "PostgreSQL connection string (required)")
	fs.IntVar(&cfg.MaxDBConns, "max-db-conns", 0, "maximum database connections (0 = concurrency + 8)")
	fs.BoolVar(&cfg.Migrate, "migrate", true, "apply embedded schema migrations on startup")
	fs.TextVar(&cfg.LogLevel, "log-level", slog.LevelInfo, "log level: debug, info, warn or error")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 30*time.Second, "time allowed for in-flight HTTP requests to finish on shutdown")
	fs.IntVar(&d.Concurrency, "concurrency", d.Concurrency, "maximum concurrent delivery attempts")
	fs.DurationVar(&d.PollInterval, "poll-interval", d.PollInterval, "idle wait between queue polls")
	fs.DurationVar(&d.RequestTimeout, "request-timeout", d.RequestTimeout, "timeout for each delivery attempt")
	fs.DurationVar(&d.Lease, "lease", d.Lease, "how long a claimed delivery is hidden from other workers")
	fs.IntVar(&d.MaxAttempts, "max-attempts", d.MaxAttempts, "attempts before a delivery is dead-lettered")
	fs.DurationVar(&d.Backoff.Base, "backoff-base", d.Backoff.Base, "backoff ceiling for the first retry")
	fs.DurationVar(&d.Backoff.Max, "backoff-max", d.Backoff.Max, "maximum backoff ceiling")
	fs.DurationVar(&d.MaxRetryAfter, "max-retry-after", d.MaxRetryAfter, "cap on receiver-supplied Retry-After")
	fs.BoolVar(&cfg.ShowVersion, "version", false, "print version and exit")

	var envErrs []error
	fs.VisitAll(func(f *flag.Flag) {
		name := EnvName(f.Name)
		if v := getenv(name); v != "" {
			if err := fs.Set(f.Name, v); err != nil {
				envErrs = append(envErrs, fmt.Errorf("%s: %w", name, err))
			}
		}
	})
	if err := errors.Join(envErrs...); err != nil {
		return Config{}, err
	}
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if cfg.ShowVersion {
		return cfg, nil
	}
	if cfg.MaxDBConns == 0 {
		cfg.MaxDBConns = d.Concurrency + 8
	}
	return cfg, cfg.Validate()
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, fmt.Errorf("database URL is required (-database-url or %sDATABASE_URL)", EnvPrefix))
	}
	if c.MaxDBConns < 1 || c.MaxDBConns > maxDBConns {
		errs = append(errs, fmt.Errorf("max-db-conns must be between 1 and %d", maxDBConns))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown-timeout must be positive"))
	}
	if err := c.Delivery.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// EnvName returns the environment variable that sets the named flag.
func EnvName(flagName string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}
