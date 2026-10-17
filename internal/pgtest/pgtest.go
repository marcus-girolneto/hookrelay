//go:build integration

// Package pgtest provides throwaway PostgreSQL databases for integration
// tests. One container is started per test binary; every call to NewPool
// clones a fresh database from a migrated template, which takes milliseconds
// and keeps tests isolated from each other.
package pgtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/marcus-girolneto/hookrelay/internal/postgres"
)

const templateDB = "hookrelay_template"

var (
	once     sync.Once
	adminDSN string
	startErr error
	counter  atomic.Int64
)

func start() (string, error) {
	ctx := context.Background()
	// The container is reaped by testcontainers' Ryuk sidecar when the test
	// binary exits, so there is deliberately no Terminate call.
	c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("hookrelay"),
		tcpostgres.WithUsername("hookrelay"),
		tcpostgres.WithPassword("hookrelay"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", fmt.Errorf("connection string: %w", err)
	}
	if err := exec(ctx, dsn, "CREATE DATABASE "+templateDB); err != nil {
		return "", err
	}
	tpl, err := pgxpool.New(ctx, WithDatabase(dsn, templateDB))
	if err != nil {
		return "", fmt.Errorf("connect template: %w", err)
	}
	defer tpl.Close()
	if err := postgres.Migrate(ctx, tpl, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		return "", fmt.Errorf("migrate template: %w", err)
	}
	return dsn, nil
}

func exec(ctx context.Context, dsn, sql string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}

// WithDatabase returns dsn with its database name replaced.
func WithDatabase(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + db
	return u.String()
}

// NewDatabase creates a fresh, migrated database and returns its DSN.
func NewDatabase(t testing.TB) string {
	t.Helper()
	once.Do(func() { adminDSN, startErr = start() })
	if startErr != nil {
		t.Fatalf("pgtest: %v", startErr)
	}
	name := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), counter.Add(1))
	if err := exec(t.Context(), adminDSN, "CREATE DATABASE "+name+" TEMPLATE "+templateDB); err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	return WithDatabase(adminDSN, name)
}

// NewPool returns a pool connected to a fresh, migrated database. The pool
// is closed when the test ends.
func NewPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), NewDatabase(t))
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
