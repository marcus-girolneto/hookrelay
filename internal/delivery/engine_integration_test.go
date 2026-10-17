//go:build integration

package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/marcus-girolneto/hookrelay/internal/backoff"
	"github.com/marcus-girolneto/hookrelay/internal/delivery"
	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/pgtest"
	"github.com/marcus-girolneto/hookrelay/internal/postgres"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

// TestTwoEnginesNeverDoubleDeliver runs two independent engines, each with
// its own connection pool as separate processes would, against one database
// and checks that every event reaches the receiver exactly once.
func TestTwoEnginesNeverDoubleDeliver(t *testing.T) {
	const events = 300
	secret := signature.NewSecret()
	verifier, err := signature.NewVerifier(secret)
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu       sync.Mutex
		received = make(map[string]int)
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := verifier.Verify(r.Header, body); err != nil {
			t.Errorf("signature: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		mu.Lock()
		received[r.Header.Get(signature.HeaderID)]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)

	dsn := pgtest.NewDatabase(t)
	seedStore := postgres.New(newPool(t, dsn))
	ctx := t.Context()
	ep := webhook.Endpoint{ID: id.New("ep"), URL: receiver.URL, Secret: secret, CreatedAt: time.Now()}
	if err := seedStore.CreateEndpoint(ctx, ep); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		ev := webhook.Event{
			ID: id.New("evt"), Type: "order.created", Payload: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`),
			IdempotencyKey: id.New("k"), RequestHash: []byte{1}, CreatedAt: time.Now(),
		}
		if _, _, _, err := seedStore.PublishEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	cfg := delivery.DefaultConfig()
	cfg.Concurrency = 8
	cfg.PollInterval = 5 * time.Millisecond
	cfg.RequestTimeout = 5 * time.Second
	cfg.Lease = time.Minute
	cfg.Backoff = backoff.Policy{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	engineMetrics := make([]*metrics.Metrics, 2)
	for i := range engineMetrics {
		m := metrics.New(prometheus.NewRegistry())
		engineMetrics[i] = m
		e := delivery.New(postgres.New(newPool(t, dsn)), delivery.NewHTTPClient(), cfg, m, logger)
		wg.Go(func() {
			if err := e.Run(runCtx); err != nil {
				t.Errorf("engine %d: %v", i, err)
			}
		})
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err := seedStore.QueueStats(ctx, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if st.Pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d deliveries pending", st.Pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if len(received) != events {
		t.Fatalf("receiver saw %d distinct events, want %d", len(received), events)
	}
	for msgID, n := range received {
		if n != 1 {
			t.Errorf("event %s delivered %d times", msgID, n)
		}
	}
	var total float64
	for i, m := range engineMetrics {
		n := testutil.ToFloat64(m.Deliveries.WithLabelValues("succeeded"))
		if n == 0 {
			t.Errorf("engine %d delivered nothing; work was not shared", i)
		}
		total += n
		t.Logf("engine %d delivered %.0f", i, n)
	}
	if total != events {
		t.Fatalf("engines report %.0f successes, want %d", total, events)
	}
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
