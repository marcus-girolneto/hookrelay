//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/postgres"
	"github.com/marcus-girolneto/hookrelay/internal/storetest"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

// startPostgres starts one container per test binary; each test gets its own
// database cloned from a migrated template, which is far faster than a
// container per test and keeps tests isolated.
var (
	containerOnce sync.Once
	adminDSN      string
	containerErr  error
	dbCounter     atomic.Int64
)

func startContainer(t *testing.T) string {
	t.Helper()
	containerOnce.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("hookrelay"),
			tcpostgres.WithUsername("hookrelay"),
			tcpostgres.WithPassword("hookrelay"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			containerErr = err
			return
		}
		// The container is reaped by testcontainers' Ryuk sidecar when the
		// test binary exits.
		adminDSN, containerErr = c.ConnectionString(ctx, "sslmode=disable")
		if containerErr != nil {
			return
		}
		pool, err := pgxpool.New(ctx, adminDSN)
		if err != nil {
			containerErr = err
			return
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, "CREATE DATABASE template_hookrelay"); err != nil {
			containerErr = err
			return
		}
		tplPool, err := pgxpool.New(ctx, replaceDB(adminDSN, "template_hookrelay"))
		if err != nil {
			containerErr = err
			return
		}
		defer tplPool.Close()
		containerErr = postgres.Migrate(ctx, tplPool, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	if containerErr != nil {
		t.Fatalf("start postgres: %v", containerErr)
	}
	return adminDSN
}

// replaceDB returns dsn with its database name replaced by db.
func replaceDB(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + db
	return u.String()
}

// newPool returns a pool connected to a fresh, migrated database.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	admin := startContainer(t)
	ctx := t.Context()
	name := "test_" + time.Now().Format("150405") + "_" + strconv.FormatInt(dbCounter.Add(1), 10)

	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE template_hookrelay"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	pool, err := pgxpool.New(ctx, replaceDB(admin, name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Store { return postgres.New(newPool(t)) })
}

func TestMigrateIsIdempotent(t *testing.T) {
	pool := newPool(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() { errs <- postgres.Migrate(t.Context(), pool, logger) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Migrate: %v", err)
		}
	}
	ms, err := postgres.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(ms) {
		t.Fatalf("schema_migrations has %d rows, want %d", n, len(ms))
	}
}

func seed(t *testing.T, s *postgres.Store, events int, at time.Time) {
	t.Helper()
	ep := webhook.Endpoint{ID: id.New("ep"), URL: "http://localhost/hook", Secret: signature.NewSecret(), CreatedAt: at}
	if err := s.CreateEndpoint(t.Context(), ep); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		ev := webhook.Event{
			ID:             id.New("evt"),
			Type:           "t",
			Payload:        json.RawMessage(`{"i":` + strconv.Itoa(i) + `}`),
			IdempotencyKey: id.New("k"),
			RequestHash:    []byte{1},
			CreatedAt:      at,
		}
		if _, _, _, err := s.PublishEvent(t.Context(), ev); err != nil {
			t.Fatal(err)
		}
	}
}

// TestClaimDueConcurrentClaimantsNeverOverlap is the core safety property of
// the queue: with many claimants racing, every due delivery is handed out
// exactly once per lease.
func TestClaimDueConcurrentClaimantsNeverOverlap(t *testing.T) {
	const deliveries, claimants, batch = 300, 8, 7
	pool := newPool(t)
	s := postgres.New(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	seed(t, s, deliveries, now)

	var (
		mu     sync.Mutex
		counts = make(map[string]int)
		wg     sync.WaitGroup
	)
	for range claimants {
		wg.Go(func() {
			for {
				jobs, err := s.ClaimDue(t.Context(), now, batch, time.Hour)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					counts[j.Delivery.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(counts) != deliveries {
		t.Fatalf("claimed %d distinct deliveries, want %d", len(counts), deliveries)
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("delivery %s claimed %d times", id, n)
		}
	}
}

func TestPublishConcurrentSameIdempotencyKey(t *testing.T) {
	pool := newPool(t)
	s := postgres.New(pool)
	now := time.Now().UTC()
	seed(t, s, 0, now)

	const racers = 10
	var (
		wg      sync.WaitGroup
		created atomic.Int64
		ids     sync.Map
	)
	for range racers {
		wg.Go(func() {
			ev := webhook.Event{
				ID: id.New("evt"), Type: "t", Payload: json.RawMessage(`{}`),
				IdempotencyKey: "same", RequestHash: []byte{9}, CreatedAt: now,
			}
			got, ds, ok, err := s.PublishEvent(t.Context(), ev)
			if err != nil {
				t.Error(err)
				return
			}
			if len(ds) != 1 {
				t.Errorf("got %d deliveries, want 1", len(ds))
			}
			if ok {
				created.Add(1)
			}
			ids.Store(got.ID, struct{}{})
		})
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("%d publishes reported created, want exactly 1", created.Load())
	}
	n := 0
	ids.Range(func(any, any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("racers observed %d distinct event ids, want 1", n)
	}
}
