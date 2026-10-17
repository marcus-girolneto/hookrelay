// Package delivery implements the at-least-once webhook delivery engine.
package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/backoff"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

// Store is the subset of persistence the engine needs.
type Store interface {
	ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]webhook.Job, error)
	Complete(ctx context.Context, r webhook.Result) error
	QueueStats(ctx context.Context, now time.Time) (webhook.QueueStats, error)
}

// Doer sends HTTP requests. *http.Client satisfies it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config tunes the engine. Use DefaultConfig as a starting point.
type Config struct {
	Concurrency    int
	PollInterval   time.Duration
	RequestTimeout time.Duration
	Lease          time.Duration // keep well above RequestTimeout to avoid double delivery
	MaxAttempts    int
	Backoff        backoff.Policy
	MaxRetryAfter  time.Duration // caps receiver-supplied Retry-After
	StatsInterval  time.Duration
	UserAgent      string
}

// DefaultConfig returns 8 attempts over about 90 minutes, 30s request timeout.
func DefaultConfig() Config {
	return Config{
		Concurrency:    32,
		PollInterval:   500 * time.Millisecond,
		RequestTimeout: 30 * time.Second,
		Lease:          2 * time.Minute,
		MaxAttempts:    8,
		Backoff:        backoff.Policy{Base: 5 * time.Second, Max: time.Hour},
		MaxRetryAfter:  time.Hour,
		StatsInterval:  5 * time.Second,
		UserAgent:      "hookrelay/1",
	}
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	var errs []error
	if c.Concurrency < 1 {
		errs = append(errs, errors.New("concurrency must be at least 1"))
	}
	if c.PollInterval <= 0 {
		errs = append(errs, errors.New("poll interval must be positive"))
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("request timeout must be positive"))
	}
	if c.Lease < 2*c.RequestTimeout {
		errs = append(errs, fmt.Errorf("lease (%s) must be at least twice the request timeout (%s)", c.Lease, c.RequestTimeout))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, errors.New("max attempts must be at least 1"))
	}
	if c.Backoff.Base <= 0 || c.Backoff.Max < c.Backoff.Base {
		errs = append(errs, errors.New("backoff requires 0 < base <= max"))
	}
	if c.StatsInterval <= 0 {
		errs = append(errs, errors.New("stats interval must be positive"))
	}
	return errors.Join(errs...)
}

const (
	maxResponseSnippet = 1 << 10
	maxResponseDrain   = 64 << 10 // read to allow conn reuse, bigger bodies close it
	completeTimeout    = 10 * time.Second
)

// Engine delivers webhooks. Create one with New and start it with Run.
type Engine struct {
	store   Store
	client  Doer
	cfg     Config
	metrics *metrics.Metrics
	log     *slog.Logger
	now     func() time.Time
	freed   chan struct{}
}

// Option customises an Engine.
type Option func(*Engine)

// WithClock overrides the wall clock, for tests.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// New returns an Engine. cfg must be valid (see Config.Validate).
func New(store Store, client Doer, cfg Config, m *metrics.Metrics, logger *slog.Logger, opts ...Option) *Engine {
	e := &Engine{
		store:   store,
		client:  client,
		cfg:     cfg,
		metrics: m,
		log:     logger,
		now:     time.Now,
		freed:   make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// NewHTTPClient returns a delivery client that doesn't follow redirects.
func NewHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Run delivers webhooks until ctx is done, then drains in-flight attempts.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.cfg.Validate(); err != nil {
		return fmt.Errorf("invalid delivery config: %w", err)
	}
	var wg sync.WaitGroup
	defer wg.Wait()

	workCtx := context.WithoutCancel(ctx)
	wg.Go(func() { e.sampleQueue(ctx) })

	sem := make(chan struct{}, e.cfg.Concurrency)
	timer := time.NewTimer(0)
	defer timer.Stop()
	saturated := false
	for {
		var freed <-chan struct{}
		if saturated {
			freed = e.freed
		}
		select {
		case <-ctx.Done():
			e.log.LogAttrs(ctx, slog.LevelInfo, "delivery engine draining", slog.Int("in_flight", len(sem)))
			return nil
		case <-timer.C:
		case <-freed:
		}

		free := cap(sem) - len(sem)
		claimed := 0
		if free > 0 {
			jobs, err := e.store.ClaimDue(ctx, e.now(), free, e.cfg.Lease)
			if err != nil && ctx.Err() == nil {
				e.metrics.StoreErrors.WithLabelValues("claim").Inc()
				e.log.LogAttrs(ctx, slog.LevelError, "claim deliveries", slog.Any("error", err))
			}
			for _, job := range jobs {
				sem <- struct{}{} // never blocks, len(jobs) <= free
				e.metrics.InFlight.Inc()
				wg.Go(func() {
					defer e.release(sem)
					e.process(workCtx, job)
				})
			}
			claimed = len(jobs)
		}
		// full batch, so poll again as soon as a worker frees up
		saturated = claimed == free
		timer.Reset(e.cfg.PollInterval)
	}
}

func (e *Engine) release(sem chan struct{}) {
	<-sem
	e.metrics.InFlight.Dec()
	select {
	case e.freed <- struct{}{}:
	default:
	}
}

func (e *Engine) sampleQueue(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.StatsInterval)
	defer ticker.Stop()
	for {
		st, err := e.store.QueueStats(ctx, e.now())
		switch {
		case err == nil:
			e.metrics.QueueDepth.WithLabelValues("pending").Set(float64(st.Pending))
			e.metrics.QueueDepth.WithLabelValues("due").Set(float64(st.Due))
		case ctx.Err() == nil:
			e.metrics.StoreErrors.WithLabelValues("queue_stats").Inc()
			e.log.LogAttrs(ctx, slog.LevelWarn, "sample queue depth", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Engine) process(ctx context.Context, job webhook.Job) {
	res := e.Attempt(ctx, job)
	e.metrics.Deliveries.WithLabelValues(string(res.Outcome)).Inc()

	cctx, cancel := context.WithTimeout(ctx, completeTimeout)
	defer cancel()
	err := e.store.Complete(cctx, res)

	attrs := []slog.Attr{
		slog.String("delivery_id", job.Delivery.ID),
		slog.String("event_id", job.Event.ID),
		slog.String("endpoint_id", job.Endpoint.ID),
		slog.Int("attempt", res.Attempt.Number),
		slog.Int("status", res.Attempt.StatusCode),
		slog.Float64("duration_ms", float64(res.Attempt.Duration.Microseconds())/1000),
		slog.String("outcome", string(res.Outcome)),
	}
	if res.Attempt.Error != "" {
		attrs = append(attrs, slog.String("error", res.Attempt.Error))
	}
	if res.Outcome == webhook.OutcomeRetry {
		attrs = append(attrs, slog.Time("next_attempt_at", res.NextAttemptAt))
	}
	switch {
	case errors.Is(err, webhook.ErrLeaseLost):
		e.metrics.LeaseLost.Inc()
		e.log.LogAttrs(ctx, slog.LevelWarn, "attempt result discarded: lease lost", attrs...)
	case err != nil:
		// lease expires and it gets retried, receiver may see a duplicate
		e.metrics.StoreErrors.WithLabelValues("complete").Inc()
		e.log.LogAttrs(ctx, slog.LevelError, "record attempt", append(attrs, slog.Any("store_error", err))...)
	case res.Outcome == webhook.OutcomeSucceeded:
		e.log.LogAttrs(ctx, slog.LevelInfo, "delivered", attrs...)
	default:
		e.log.LogAttrs(ctx, slog.LevelWarn, "delivery attempt failed", attrs...)
	}
}

// Attempt performs one signed request for job. Every failure maps to an outcome.
func (e *Engine) Attempt(ctx context.Context, job webhook.Job) webhook.Result {
	startedAt := e.now()
	attempt := webhook.Attempt{
		DeliveryID: job.Delivery.ID,
		Number:     job.Delivery.AttemptCount + 1,
		StartedAt:  startedAt,
	}
	res := webhook.Result{DeliveryID: job.Delivery.ID, LeaseToken: job.LeaseToken}

	resp, duration, err := e.send(ctx, job, startedAt)
	attempt.Duration = duration
	e.metrics.AttemptDuration.WithLabelValues(metrics.StatusClass(resp.statusCode)).Observe(duration.Seconds())

	var retryAfter time.Duration
	var hasRetryAfter bool
	if err != nil {
		attempt.Error = err.Error()
	} else {
		attempt.StatusCode = resp.statusCode
		attempt.ResponseBody = snippet(resp.body)
		if resp.statusCode == http.StatusTooManyRequests || resp.statusCode == http.StatusServiceUnavailable {
			retryAfter, hasRetryAfter = parseRetryAfter(resp.retryAfter, e.now())
		}
	}

	finishedAt := e.now()
	res.FinishedAt = finishedAt
	switch {
	case err == nil && resp.statusCode >= 200 && resp.statusCode < 300:
		res.Outcome = webhook.OutcomeSucceeded
	case err == nil && resp.statusCode == http.StatusGone:
		attempt.Error = "endpoint returned 410 Gone; endpoint disabled"
		res.Outcome = webhook.OutcomeDead
		res.DisableEndpoint = true
	default:
		if attempt.Error == "" {
			attempt.Error = fmt.Sprintf("unexpected status %d", resp.statusCode)
		}
		if attempt.Number >= e.cfg.MaxAttempts {
			res.Outcome = webhook.OutcomeDead
			break
		}
		delay := e.cfg.Backoff.Delay(attempt.Number)
		if hasRetryAfter {
			delay = min(retryAfter, e.cfg.MaxRetryAfter)
		}
		res.Outcome = webhook.OutcomeRetry
		res.NextAttemptAt = finishedAt.Add(delay)
	}
	res.Attempt = attempt
	return res
}

type response struct {
	statusCode int
	retryAfter string
	body       []byte
}

// body read is under RequestTimeout too, so slow-drip bodies can't pin a worker
func (e *Engine) send(ctx context.Context, job webhook.Job, ts time.Time) (response, time.Duration, error) {
	signer, err := signature.NewSigner(job.Endpoint.Secret)
	if err != nil {
		return response{}, 0, fmt.Errorf("endpoint secret: %w", err)
	}
	body := job.Event.Body()

	ctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.Endpoint.URL, bytes.NewReader(body))
	if err != nil {
		return response{}, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", e.cfg.UserAgent)
	signer.SetHeaders(req.Header, job.Event.ID, ts, body)

	start := time.Now()
	resp, err := e.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("request timed out after %s", e.cfg.RequestTimeout)
		}
		return response{}, time.Since(start), err
	}
	defer resp.Body.Close()
	// a read error after the status line still counts as a response
	buf, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseDrain))
	duration := time.Since(start)
	if errors.Is(readErr, context.DeadlineExceeded) {
		return response{}, duration, fmt.Errorf("reading response timed out after %s", e.cfg.RequestTimeout)
	}
	return response{statusCode: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), body: buf}, duration, nil
}

// valid UTF-8 without NULs, which postgres text rejects
func snippet(body []byte) string {
	if len(body) > maxResponseSnippet {
		body = body[:maxResponseSnippet]
	}
	s := strings.ToValidUTF8(string(body), "\uFFFD")
	return strings.ReplaceAll(s, "\x00", "")
}

// avoids Duration overflow, MaxRetryAfter caps it anyway
const maxRetryAfterSeconds = 1 << 31

// seconds or HTTP-date, RFC 9110 10.2.3
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(min(secs, maxRetryAfterSeconds)) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0), true
	}
	return 0, false
}
