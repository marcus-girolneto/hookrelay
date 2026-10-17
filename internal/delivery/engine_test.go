package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/marcus-girolneto/hookrelay/internal/backoff"
	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/memstore"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

var testSecret = signature.NewSecret()

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fastConfig() Config {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.PollInterval = 2 * time.Millisecond
	cfg.RequestTimeout = 2 * time.Second
	cfg.Lease = 10 * time.Second
	cfg.MaxAttempts = 3
	cfg.Backoff = backoff.Policy{Base: time.Millisecond, Max: 5 * time.Millisecond}
	cfg.StatsInterval = 5 * time.Millisecond
	return cfg
}

func newEngine(t testing.TB, s Store, cfg Config, opts ...Option) (*Engine, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	return New(s, NewHTTPClient(), cfg, m, discardLogger(), opts...), m
}

func newJob(url string, attemptCount int) webhook.Job {
	return webhook.Job{
		Delivery: webhook.Delivery{ID: id.New("dlv"), AttemptCount: attemptCount, Status: webhook.StatusPending},
		Event: webhook.Event{
			ID:        id.New("evt"),
			Type:      "invoice.paid",
			Payload:   json.RawMessage(`{"amount":42}`),
			CreatedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},
		Endpoint:   webhook.Endpoint{ID: id.New("ep"), URL: url, Secret: testSecret},
		LeaseToken: "lease_test",
	}
}

func TestAttemptClassification(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	tests := []struct {
		name         string
		handler      http.HandlerFunc
		attemptCount int
		wantOutcome  webhook.Outcome
		wantStatus   int
		wantDisable  bool
		wantDelay    time.Duration
		wantErr      string
	}{
		{
			name:        "200 succeeds",
			handler:     func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "thanks") },
			wantOutcome: webhook.OutcomeSucceeded,
			wantStatus:  200,
		},
		{
			name:        "204 succeeds",
			handler:     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			wantOutcome: webhook.OutcomeSucceeded,
			wantStatus:  204,
		},
		{
			name:        "500 retries with backoff",
			handler:     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  500,
			wantDelay:   500 * time.Millisecond,
			wantErr:     "unexpected status 500",
		},
		{
			name:         "backoff grows with attempt number",
			handler:      func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) },
			attemptCount: 3,
			wantOutcome:  webhook.OutcomeRetry,
			wantStatus:   502,
			wantDelay:    4 * time.Second,
			wantErr:      "unexpected status 502",
		},
		{
			name:         "last attempt dead-letters",
			handler:      func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			attemptCount: 4,
			wantOutcome:  webhook.OutcomeDead,
			wantStatus:   500,
			wantErr:      "unexpected status 500",
		},
		{
			name:        "410 disables endpoint",
			handler:     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusGone) },
			wantOutcome: webhook.OutcomeDead,
			wantStatus:  410,
			wantDisable: true,
			wantErr:     "endpoint returned 410 Gone; endpoint disabled",
		},
		{
			name: "429 honours Retry-After seconds",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(http.StatusTooManyRequests)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  429,
			wantDelay:   2 * time.Minute,
			wantErr:     "unexpected status 429",
		},
		{
			name: "503 honours Retry-After HTTP date",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", now.Add(90*time.Second).Format(http.TimeFormat))
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  503,
			wantDelay:   90 * time.Second,
			wantErr:     "unexpected status 503",
		},
		{
			name: "Retry-After is capped",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "99999999999")
				w.WriteHeader(http.StatusTooManyRequests)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  429,
			wantDelay:   time.Hour,
			wantErr:     "unexpected status 429",
		},
		{
			name: "Retry-After ignored on 500",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  500,
			wantDelay:   500 * time.Millisecond,
			wantErr:     "unexpected status 500",
		},
		{
			name: "invalid Retry-After falls back to backoff",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "soon")
				w.WriteHeader(http.StatusTooManyRequests)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  429,
			wantDelay:   500 * time.Millisecond,
			wantErr:     "unexpected status 429",
		},
		{
			name: "redirects are not followed",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantStatus:  302,
			wantDelay:   500 * time.Millisecond,
			wantErr:     "unexpected status 302",
		},
		{
			name: "timeout retries",
			handler: func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
				w.WriteHeader(http.StatusOK)
			},
			wantOutcome: webhook.OutcomeRetry,
			wantDelay:   500 * time.Millisecond,
			wantErr:     "request timed out after 100ms",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(tc.handler)
			t.Cleanup(srv.Close)

			cfg := DefaultConfig()
			cfg.MaxAttempts = 5
			cfg.RequestTimeout = 100 * time.Millisecond
			cfg.Backoff = backoff.Policy{Base: time.Second, Max: time.Hour, Rand: func() float64 { return 0.5 }}
			e, _ := newEngine(t, memstore.New(), cfg, WithClock(clock))

			job := newJob(srv.URL, tc.attemptCount)
			res := e.Attempt(t.Context(), job)

			if res.Outcome != tc.wantOutcome {
				t.Fatalf("outcome = %s, want %s (error %q)", res.Outcome, tc.wantOutcome, res.Attempt.Error)
			}
			if res.Attempt.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", res.Attempt.StatusCode, tc.wantStatus)
			}
			if res.DisableEndpoint != tc.wantDisable {
				t.Errorf("disable = %v, want %v", res.DisableEndpoint, tc.wantDisable)
			}
			if res.Attempt.Error != tc.wantErr {
				t.Errorf("error = %q, want %q", res.Attempt.Error, tc.wantErr)
			}
			if res.Attempt.Number != tc.attemptCount+1 {
				t.Errorf("attempt number = %d, want %d", res.Attempt.Number, tc.attemptCount+1)
			}
			if res.DeliveryID != job.Delivery.ID || res.LeaseToken != job.LeaseToken {
				t.Errorf("result not bound to job: %+v", res)
			}
			if tc.wantOutcome == webhook.OutcomeRetry {
				if got := res.NextAttemptAt.Sub(now); got != tc.wantDelay {
					t.Errorf("retry delay = %v, want %v", got, tc.wantDelay)
				}
			}
		})
	}
}

func TestAttemptSendsVerifiableRequest(t *testing.T) {
	t.Parallel()
	verifier, err := signature.NewVerifier(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	type seen struct {
		header http.Header
		body   []byte
	}
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{header: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	e, _ := newEngine(t, memstore.New(), DefaultConfig())
	job := newJob(srv.URL, 0)
	if res := e.Attempt(t.Context(), job); res.Outcome != webhook.OutcomeSucceeded {
		t.Fatalf("outcome = %s: %s", res.Outcome, res.Attempt.Error)
	}
	req := <-got
	if err := verifier.Verify(req.header, req.body); err != nil {
		t.Fatalf("receiver could not verify signature: %v", err)
	}
	if id := req.header.Get(signature.HeaderID); id != job.Event.ID {
		t.Errorf("webhook-id = %q, want event id %q", id, job.Event.ID)
	}
	if ct := req.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if string(req.body) != string(job.Event.Body()) {
		t.Errorf("body = %s, want %s", req.body, job.Event.Body())
	}
}

func TestAttemptResponseSnippet(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("bad\x00\xffbytes"))
		_, _ = w.Write(make([]byte, 4096))
	}))
	t.Cleanup(srv.Close)
	e, _ := newEngine(t, memstore.New(), DefaultConfig())
	res := e.Attempt(t.Context(), newJob(srv.URL, 0))
	if got := res.Attempt.ResponseBody; got != "bad�bytes" {
		t.Fatalf("snippet = %q", got)
	}
}

func TestAttemptTransportErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		job  func() webhook.Job
	}{
		{name: "connection refused", job: func() webhook.Job {
			srv := httptest.NewServer(http.NotFoundHandler())
			srv.Close()
			return newJob(srv.URL, 0)
		}},
		{name: "invalid secret", job: func() webhook.Job {
			j := newJob("http://127.0.0.1:1", 0)
			j.Endpoint.Secret = "nope"
			return j
		}},
		{name: "invalid url", job: func() webhook.Job { return newJob("http://[::1", 0) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, _ := newEngine(t, memstore.New(), DefaultConfig())
			res := e.Attempt(t.Context(), tc.job())
			if res.Outcome != webhook.OutcomeRetry || res.Attempt.StatusCode != 0 || res.Attempt.Error == "" {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in     string
		want   time.Duration
		wantOK bool
	}{
		{in: "", wantOK: false},
		{in: "0", want: 0, wantOK: true},
		{in: " 30 ", want: 30 * time.Second, wantOK: true},
		{in: "-1", wantOK: false},
		{in: "1.5", wantOK: false},
		{in: "99999999999999", want: maxRetryAfterSeconds * time.Second, wantOK: true},
		{in: now.Add(time.Minute).Format(http.TimeFormat), want: time.Minute, wantOK: true},
		{in: now.Add(-time.Minute).Format(http.TimeFormat), want: 0, wantOK: true},
		{in: "tomorrow", wantOK: false},
	}
	for _, tc := range tests {
		got, ok := parseRetryAfter(tc.in, now)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %v, %v; want %v, %v", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero concurrency", func(c *Config) { c.Concurrency = 0 }},
		{"zero poll interval", func(c *Config) { c.PollInterval = 0 }},
		{"zero timeout", func(c *Config) { c.RequestTimeout = 0 }},
		{"lease shorter than two timeouts", func(c *Config) { c.Lease = c.RequestTimeout }},
		{"zero attempts", func(c *Config) { c.MaxAttempts = 0 }},
		{"backoff max below base", func(c *Config) { c.Backoff.Max = c.Backoff.Base / 2 }},
		{"zero stats interval", func(c *Config) { c.StatsInterval = 0 }},
	}
	for _, tc := range tests {
		cfg := DefaultConfig()
		tc.mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
	e, _ := newEngine(t, memstore.New(), Config{})
	if err := e.Run(t.Context()); err == nil {
		t.Fatal("Run accepted an invalid config")
	}
}

type fixture struct {
	store    *memstore.Store
	endpoint webhook.Endpoint
}

func newFixture(t testing.TB, url string) *fixture {
	t.Helper()
	s := memstore.New()
	ep := webhook.Endpoint{ID: id.New("ep"), URL: url, Secret: testSecret, CreatedAt: time.Now()}
	if err := s.CreateEndpoint(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	return &fixture{store: s, endpoint: ep}
}

func (f *fixture) publish(t testing.TB) webhook.Event {
	t.Helper()
	ev := webhook.Event{
		ID:             id.New("evt"),
		Type:           "invoice.paid",
		Payload:        json.RawMessage(`{"amount":42}`),
		IdempotencyKey: id.New("key"),
		RequestHash:    []byte{1},
		CreatedAt:      time.Now(),
	}
	if _, _, _, err := f.store.PublishEvent(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	return ev
}

func (f *fixture) delivery(t testing.TB, eventID string) webhook.DeliveryDetail {
	t.Helper()
	ds, err := f.store.ListDeliveries(context.Background(), eventID)
	if err != nil || len(ds) != 1 {
		t.Fatalf("ListDeliveries = %v, %v", ds, err)
	}
	return ds[0]
}

func runEngine(t *testing.T, e *Engine) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Run returned %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRunRetriesUntilSuccess(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	f := newFixture(t, srv.URL)
	ev := f.publish(t)
	e, m := newEngine(t, f.store, fastConfig())
	runEngine(t, e)

	waitFor(t, "delivery to succeed", func() bool { return f.delivery(t, ev.ID).Status == webhook.StatusSucceeded })
	d := f.delivery(t, ev.ID)
	if d.AttemptCount != 3 || len(d.Attempts) != 3 {
		t.Fatalf("attempt count = %d, attempts = %d, want 3", d.AttemptCount, len(d.Attempts))
	}
	for i, a := range d.Attempts {
		if a.Number != i+1 {
			t.Errorf("attempt %d has number %d", i, a.Number)
		}
	}
	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("retry")); got != 2 {
		t.Errorf("retry counter = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("succeeded")); got != 1 {
		t.Errorf("succeeded counter = %v, want 1", got)
	}
}

func TestRunDeadLettersAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	f := newFixture(t, srv.URL)
	ev := f.publish(t)
	e, m := newEngine(t, f.store, fastConfig())
	stop := runEngine(t, e)

	waitFor(t, "dead letter", func() bool { return f.delivery(t, ev.ID).Status == webhook.StatusDead })
	stop()
	d := f.delivery(t, ev.ID)
	if d.AttemptCount != 3 || calls.Load() != 3 {
		t.Fatalf("attempts = %d, receiver calls = %d, want 3", d.AttemptCount, calls.Load())
	}
	if d.LastError != "unexpected status 500" {
		t.Errorf("last error = %q", d.LastError)
	}
	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("dead")); got != 1 {
		t.Errorf("dead counter = %v, want 1", got)
	}

	if _, err := f.store.ReplayDelivery(t.Context(), d.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	e2, _ := newEngine(t, f.store, fastConfig())
	runEngine(t, e2)
	waitFor(t, "replayed delivery to dead-letter again", func() bool {
		d := f.delivery(t, ev.ID)
		return d.Status == webhook.StatusDead && len(d.Attempts) == 6
	})
}

func TestRun410DisablesEndpoint(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusGone)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	f := newFixture(t, srv.URL)
	first := f.publish(t)
	cfg := fastConfig()
	cfg.Concurrency = 1
	e, _ := newEngine(t, f.store, cfg)
	runEngine(t, e)

	waitFor(t, "first request", func() bool { return inFlight(e) == 1 })
	second := f.publish(t)
	release <- struct{}{}

	waitFor(t, "endpoint disabled", func() bool {
		ep, _ := f.store.GetEndpoint(t.Context(), f.endpoint.ID)
		return ep.Disabled()
	})
	if d := f.delivery(t, first.ID); d.Status != webhook.StatusDead || d.AttemptCount != 1 {
		t.Fatalf("first delivery = %+v", d.Delivery)
	}
	if d := f.delivery(t, second.ID); d.Status != webhook.StatusDead || d.AttemptCount != 0 || d.LastError != webhook.ErrEndpointDisabled.Error() {
		t.Fatalf("queued delivery = %+v", d.Delivery)
	}
}

func inFlight(e *Engine) int { return int(testutil.ToFloat64(e.metrics.InFlight)) }

func TestRunHonoursRetryAfter(t *testing.T) {
	t.Parallel()
	var (
		mu    sync.Mutex
		times []time.Time
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	f := newFixture(t, srv.URL)
	ev := f.publish(t)
	e, _ := newEngine(t, f.store, fastConfig())
	runEngine(t, e)

	waitFor(t, "delivery to succeed", func() bool { return f.delivery(t, ev.ID).Status == webhook.StatusSucceeded })
	mu.Lock()
	defer mu.Unlock()
	if gap := times[1].Sub(times[0]); gap < time.Second {
		t.Fatalf("retried after %v, want at least the 1s Retry-After", gap)
	}
}

func TestRunBoundsConcurrency(t *testing.T) {
	t.Parallel()
	var cur, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		cur.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	f := newFixture(t, srv.URL)
	const n = 40
	for range n {
		f.publish(t)
	}
	cfg := fastConfig()
	cfg.Concurrency = 3
	e, m := newEngine(t, f.store, cfg)
	runEngine(t, e)

	waitFor(t, "all deliveries", func() bool {
		return testutil.ToFloat64(m.Deliveries.WithLabelValues("succeeded")) == n
	})
	if p := peak.Load(); p > 3 {
		t.Fatalf("peak concurrency %d exceeds limit 3", p)
	}
	waitFor(t, "queue depth gauge", func() bool {
		return testutil.ToFloat64(m.QueueDepth.WithLabelValues("pending")) == 0
	})
}

func TestRunShutdownWaitsForInFlight(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	f := newFixture(t, srv.URL)
	ev := f.publish(t)
	e, _ := newEngine(t, f.store, fastConfig())
	stop := runEngine(t, e)
	<-started

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Run returned while a delivery was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped
	if d := f.delivery(t, ev.ID); d.Status != webhook.StatusSucceeded {
		t.Fatalf("in-flight delivery not recorded on shutdown: %+v", d.Delivery)
	}
}

type faultyStore struct {
	*memstore.Store
	claimErr    atomic.Int32
	completeErr error
}

func (s *faultyStore) ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]webhook.Job, error) {
	if s.claimErr.Add(-1) >= 0 {
		return nil, errors.New("connection refused")
	}
	return s.Store.ClaimDue(ctx, now, limit, lease)
}

func (s *faultyStore) Complete(ctx context.Context, r webhook.Result) error {
	if s.completeErr != nil {
		_ = s.Store.Complete(ctx, r)
		return s.completeErr
	}
	return s.Store.Complete(ctx, r)
}

func (s *faultyStore) QueueStats(context.Context, time.Time) (webhook.QueueStats, error) {
	return webhook.QueueStats{}, errors.New("stats unavailable")
}

func TestRunSurvivesStoreErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(srv.Close)
	f := newFixture(t, srv.URL)
	ev := f.publish(t)

	fs := &faultyStore{Store: f.store}
	fs.claimErr.Store(3)
	e, m := newEngine(t, fs, fastConfig())
	runEngine(t, e)

	waitFor(t, "delivery after claim errors", func() bool { return f.delivery(t, ev.ID).Status == webhook.StatusSucceeded })
	if got := testutil.ToFloat64(m.StoreErrors.WithLabelValues("claim")); got != 3 {
		t.Errorf("claim errors = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.StoreErrors.WithLabelValues("queue_stats")); got < 1 {
		t.Errorf("queue_stats errors = %v, want >= 1", got)
	}
}

func TestRunCountsLeaseLossAndCompleteErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		err    error
		metric func(*metrics.Metrics) prometheus.Collector
	}{
		{name: "lease lost", err: webhook.ErrLeaseLost, metric: func(m *metrics.Metrics) prometheus.Collector { return m.LeaseLost }},
		{name: "store failure", err: errors.New("db down"), metric: func(m *metrics.Metrics) prometheus.Collector { return m.StoreErrors.WithLabelValues("complete") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
			t.Cleanup(srv.Close)
			f := newFixture(t, srv.URL)
			f.publish(t)
			e, m := newEngine(t, &faultyStore{Store: f.store, completeErr: tc.err}, fastConfig())
			runEngine(t, e)
			waitFor(t, tc.name+" metric", func() bool { return testutil.ToFloat64(tc.metric(m)) >= 1 })
		})
	}
}

func BenchmarkAttempt(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	b.Cleanup(srv.Close)
	e, _ := newEngine(b, memstore.New(), DefaultConfig())
	job := newJob(srv.URL, 0)
	b.ReportAllocs()
	for b.Loop() {
		if res := e.Attempt(b.Context(), job); res.Outcome != webhook.OutcomeSucceeded {
			b.Fatal(res.Attempt.Error)
		}
	}
}

func BenchmarkDispatch(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	b.Cleanup(srv.Close)
	for _, conc := range []int{1, 8, 32} {
		b.Run("concurrency="+strconv.Itoa(conc), func(b *testing.B) {
			const batch = 500
			cfg := fastConfig()
			cfg.Concurrency = conc
			var total time.Duration
			var delivered int
			for b.Loop() {
				b.StopTimer()
				f := newFixture(b, srv.URL)
				for range batch {
					f.publish(b)
				}
				e, m := newEngine(b, f.store, cfg)
				ctx, cancel := context.WithCancel(b.Context())
				done := make(chan struct{})
				b.StartTimer()
				start := time.Now()
				go func() { _ = e.Run(ctx); close(done) }()
				for testutil.ToFloat64(m.Deliveries.WithLabelValues("succeeded")) < batch {
					time.Sleep(100 * time.Microsecond)
				}
				total += time.Since(start)
				delivered += batch
				cancel()
				<-done
			}
			b.ReportMetric(float64(delivered)/total.Seconds(), "deliveries/s")
		})
	}
}
