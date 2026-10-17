package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockID is an arbitrary constant for pg_advisory_lock so that
// instances starting concurrently apply migrations one at a time.
const migrationLockID = 0x686f6f6b72656c61 // "hookrela"

// Migration is a single forward-only schema change.
type Migration struct {
	Version string
	SQL     string
}

// Migrations returns the embedded migrations in application order.
func Migrations() ([]Migration, error) {
	return loadMigrations(migrationsFS, "migrations")
}

func loadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		b, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		out = append(out, Migration{Version: strings.TrimSuffix(e.Name(), ".sql"), SQL: string(b)})
	}
	slices.SortFunc(out, func(a, b Migration) int { return strings.Compare(a.Version, b.Version) })
	return out, nil
}

// Migrate applies all pending embedded migrations. Each migration runs in its
// own transaction together with its bookkeeping row, so a failure leaves the
// schema at the last fully applied version.
func Migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	ms, err := Migrations()
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Use a fresh context: the caller's may already be cancelled, and a
		// leaked session lock would block every other instance.
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text        PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("list applied migrations: %w", err)
	}
	applied, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("list applied migrations: %w", err)
	}

	for _, m := range ms {
		if slices.Contains(applied, m.Version) {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", m.Version)
			return err
		})
		if err != nil {
			return fmt.Errorf("apply migration %s: %w", m.Version, err)
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "applied migration", slog.String("version", m.Version))
	}
	return nil
}
