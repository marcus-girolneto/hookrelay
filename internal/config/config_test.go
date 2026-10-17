package config

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load(nil, env(map[string]string{"HOOKRELAY_DATABASE_URL": "postgres://db/hookrelay"}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || !cfg.Migrate || cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.MaxDBConns != cfg.Delivery.Concurrency+8 {
		t.Fatalf("MaxDBConns = %d, want concurrency+8", cfg.MaxDBConns)
	}
}

func TestLoadPrecedence(t *testing.T) {
	t.Parallel()
	e := env(map[string]string{
		"HOOKRELAY_DATABASE_URL":    "postgres://env/db",
		"HOOKRELAY_CONCURRENCY":     "4",
		"HOOKRELAY_REQUEST_TIMEOUT": "5s",
		"HOOKRELAY_LOG_LEVEL":       "debug",
		"HOOKRELAY_MIGRATE":         "false",
	})
	cfg, err := Load([]string{"-concurrency", "16", "-addr", "127.0.0.1:9000"}, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Delivery.Concurrency != 16 {
		t.Errorf("flag should override env: concurrency = %d", cfg.Delivery.Concurrency)
	}
	if cfg.Delivery.RequestTimeout != 5*time.Second || cfg.LogLevel != slog.LevelDebug || cfg.Migrate {
		t.Errorf("env not applied: %+v", cfg)
	}
	if cfg.Addr != "127.0.0.1:9000" || cfg.DatabaseURL != "postgres://env/db" {
		t.Errorf("unexpected %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()
	dbEnv := map[string]string{"HOOKRELAY_DATABASE_URL": "postgres://db"}
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
	}{
		{name: "missing database", wantErr: "database URL is required"},
		{name: "bad env value", env: map[string]string{"HOOKRELAY_CONCURRENCY": "lots"}, wantErr: "HOOKRELAY_CONCURRENCY"},
		{name: "bad flag", args: []string{"-concurrency", "x"}, env: dbEnv, wantErr: "invalid value"},
		{name: "positional args", args: []string{"serve"}, env: dbEnv, wantErr: "unexpected arguments"},
		{name: "invalid delivery config", args: []string{"-lease", "1s"}, env: dbEnv, wantErr: "lease"},
		{name: "too many conns", args: []string{"-max-db-conns", "20000"}, env: dbEnv, wantErr: "max-db-conns"},
		{name: "bad shutdown timeout", args: []string{"-shutdown-timeout", "0s"}, env: dbEnv, wantErr: "shutdown-timeout"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Load(tc.args, env(tc.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadHelpAndVersion(t *testing.T) {
	t.Parallel()
	var usage strings.Builder
	if _, err := Load([]string{"-h"}, env(nil), &usage); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	if !strings.Contains(usage.String(), "-database-url") {
		t.Fatal("usage does not list flags")
	}
	cfg, err := Load([]string{"-version"}, env(nil), io.Discard)
	if err != nil || !cfg.ShowVersion {
		t.Fatalf("-version without database url: %+v, %v", cfg, err)
	}
}

func TestEnvName(t *testing.T) {
	t.Parallel()
	if got := EnvName("max-retry-after"); got != "HOOKRELAY_MAX_RETRY_AFTER" {
		t.Fatalf("EnvName = %q", got)
	}
}
