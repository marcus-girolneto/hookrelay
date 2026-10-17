// Package receiver is a reference webhook consumer. It shows what a
// receiver must do with hookrelay's at-least-once delivery: verify the
// signature over the raw body, reject stale timestamps, and deduplicate on
// webhook-id before acting.
package receiver

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"

	"github.com/marcus-girolneto/hookrelay/signature"
)

const maxBody = 1 << 20

// Option customises a Handler.
type Option func(*Handler)

// WithFailureRate makes the handler answer 503 for the given fraction of
// first-time messages, to demonstrate retries.
func WithFailureRate(rate float64, rnd func() float64) Option {
	return func(h *Handler) {
		h.failRate = rate
		if rnd != nil {
			h.rand = rnd
		}
	}
}

// Handler verifies and records incoming webhooks.
type Handler struct {
	verifier *signature.Verifier
	log      *slog.Logger
	seen     *dedup
	failRate float64
	rand     func() float64
}

// NewHandler returns a Handler that remembers the last dedupSize message ids.
func NewHandler(v *signature.Verifier, logger *slog.Logger, dedupSize int, opts ...Option) *Handler {
	h := &Handler{verifier: v, log: logger, seen: newDedup(dedupSize), rand: rand.Float64}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

type envelope struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	msgID := r.Header.Get(signature.HeaderID)
	if err := h.verifier.Verify(r.Header, body); err != nil {
		h.log.LogAttrs(r.Context(), slog.LevelWarn, "rejected webhook", slog.String("webhook_id", msgID), slog.Any("error", err))
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	if h.seen.contains(msgID) {
		h.log.LogAttrs(r.Context(), slog.LevelInfo, "duplicate webhook ignored", slog.String("webhook_id", msgID))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if h.failRate > 0 && h.rand() < h.failRate {
		h.log.LogAttrs(r.Context(), slog.LevelInfo, "simulating outage", slog.String("webhook_id", msgID))
		w.Header().Set("Retry-After", "2")
		http.Error(w, "simulated outage", http.StatusServiceUnavailable)
		return
	}

	var ev envelope
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	// A real consumer would apply its side effect and record msgID in the
	// same transaction; here the in-memory set stands in for that.
	h.seen.add(msgID)
	h.log.LogAttrs(r.Context(), slog.LevelInfo, "webhook received",
		slog.String("webhook_id", msgID),
		slog.String("type", ev.Type),
		slog.String("data", string(ev.Data)))
	w.WriteHeader(http.StatusNoContent)
}

// dedup is a fixed-size set that forgets the oldest ids first.
type dedup struct {
	mu   sync.Mutex
	ids  map[string]struct{}
	ring []string
	next int
}

func newDedup(size int) *dedup {
	size = max(size, 1)
	return &dedup{ids: make(map[string]struct{}, size), ring: make([]string, size)}
}

func (d *dedup) contains(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.ids[id]
	return ok
}

func (d *dedup) add(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.ids[id]; ok {
		return
	}
	if old := d.ring[d.next]; old != "" {
		delete(d.ids, old)
	}
	d.ring[d.next] = id
	d.ids[id] = struct{}{}
	d.next = (d.next + 1) % len(d.ring)
}
