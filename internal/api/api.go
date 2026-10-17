// Package api implements hookrelay's HTTP API: endpoint management, event
// publishing with idempotency keys, delivery inspection and replay, and
// health checks.
package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

// Store is the persistence the API needs.
type Store interface {
	Ping(ctx context.Context) error
	CreateEndpoint(ctx context.Context, ep webhook.Endpoint) error
	GetEndpoint(ctx context.Context, id string) (webhook.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]webhook.Endpoint, error)
	EnableEndpoint(ctx context.Context, id string) (webhook.Endpoint, error)
	PublishEvent(ctx context.Context, ev webhook.Event) (webhook.Event, []webhook.Delivery, bool, error)
	GetEvent(ctx context.Context, id string) (webhook.Event, error)
	ListDeliveries(ctx context.Context, eventID string) ([]webhook.DeliveryDetail, error)
	ReplayDelivery(ctx context.Context, id string, now time.Time) (webhook.Delivery, error)
}

const (
	// IdempotencyHeader carries the client-chosen key that deduplicates
	// publishes.
	IdempotencyHeader = "Idempotency-Key"
	// ReplayedHeader is set on responses served from an earlier publish.
	ReplayedHeader = "Idempotent-Replayed"

	maxBodyBytes      = 256 << 10
	maxIdempotencyKey = 255
	maxEventType      = 255
	readyTimeout      = 2 * time.Second
)

// Server serves the API. Create one with New.
type Server struct {
	store    Store
	metrics  *metrics.Metrics
	log      *slog.Logger
	now      func() time.Time
	draining atomic.Bool
}

// Option customises a Server.
type Option func(*Server)

// WithClock overrides the wall clock, for tests.
func WithClock(now func() time.Time) Option {
	return func(s *Server) { s.now = now }
}

// New returns a Server.
func New(store Store, m *metrics.Metrics, logger *slog.Logger, opts ...Option) *Server {
	s := &Server{store: store, metrics: m, log: logger, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetDraining makes /readyz fail so load balancers stop routing new
// traffic while in-flight requests complete.
func (s *Server) SetDraining() { s.draining.Store(true) }

// Handler returns the API's HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("POST /v1/endpoints", s.createEndpoint)
	mux.HandleFunc("GET /v1/endpoints", s.listEndpoints)
	mux.HandleFunc("GET /v1/endpoints/{id}", s.getEndpoint)
	mux.HandleFunc("POST /v1/endpoints/{id}/enable", s.enableEndpoint)
	mux.HandleFunc("POST /v1/events", s.publishEvent)
	mux.HandleFunc("GET /v1/events/{id}", s.getEvent)
	mux.HandleFunc("GET /v1/events/{id}/deliveries", s.listDeliveries)
	mux.HandleFunc("POST /v1/deliveries/{id}/replay", s.replayDelivery)
	return s.metrics.Middleware(mux)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeError(w, http.StatusServiceUnavailable, "draining", "server is shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.log.LogAttrs(r.Context(), slog.LevelWarn, "readiness check failed", slog.Any("error", err))
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "database is unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createEndpointRequest struct {
	URL         string   `json:"url"`
	Description string   `json:"description"`
	EventTypes  []string `json:"event_types"`
	Secret      string   `json:"secret"`
}

func (s *Server) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := webhook.ValidateURL(req.URL); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	for _, t := range req.EventTypes {
		if t == "" || len(t) > maxEventType {
			writeError(w, http.StatusBadRequest, "invalid_request", "event_types entries must be 1-255 characters")
			return
		}
	}
	if req.Secret == "" {
		req.Secret = signature.NewSecret()
	} else if _, err := signature.DecodeSecret(req.Secret); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "secret must be whsec_ followed by 24-64 base64-encoded bytes")
		return
	}
	ep := webhook.Endpoint{
		ID:          id.New("ep"),
		URL:         req.URL,
		Secret:      req.Secret,
		Description: req.Description,
		EventTypes:  req.EventTypes,
		CreatedAt:   s.now().UTC(),
	}
	if err := s.store.CreateEndpoint(r.Context(), ep); err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, endpointResource(ep, true))
}

func (s *Server) listEndpoints(w http.ResponseWriter, r *http.Request) {
	eps, err := s.store.ListEndpoints(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]endpointJSON, 0, len(eps))
	for _, ep := range eps {
		out = append(out, endpointResource(ep, false))
	}
	writeJSON(w, http.StatusOK, listJSON[endpointJSON]{Data: out})
}

func (s *Server) getEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, err := s.store.GetEndpoint(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, endpointResource(ep, false))
}

func (s *Server) enableEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, err := s.store.EnableEndpoint(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, endpointResource(ep, false))
}

type publishRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type publishResponse struct {
	Event      eventJSON      `json:"event"`
	Deliveries []deliveryJSON `json:"deliveries"`
}

func (s *Server) publishEvent(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(IdempotencyHeader)
	if key == "" || len(key) > maxIdempotencyKey {
		writeError(w, http.StatusBadRequest, "invalid_request", "the Idempotency-Key header is required and must be at most 255 characters")
		return
	}
	var req publishRequest
	if !s.decode(w, r, &req) {
		return
	}
	if req.Type == "" || len(req.Type) > maxEventType {
		writeError(w, http.StatusBadRequest, "invalid_request", "type is required and must be at most 255 characters")
		return
	}
	if len(req.Payload) == 0 || bytes.Equal(req.Payload, []byte("null")) {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload is required")
		return
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, req.Payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload must be valid JSON")
		return
	}
	now := s.now().UTC()
	ev := webhook.Event{
		ID:             id.NewAt("evt", now),
		Type:           req.Type,
		Payload:        compact.Bytes(),
		IdempotencyKey: key,
		RequestHash:    requestHash(req.Type, compact.Bytes()),
		CreatedAt:      now,
	}
	got, ds, created, err := s.store.PublishEvent(r.Context(), ev)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	status := http.StatusCreated
	if created {
		s.metrics.EventsPublished.WithLabelValues("created").Inc()
	} else {
		s.metrics.EventsPublished.WithLabelValues("duplicate").Inc()
		w.Header().Set(ReplayedHeader, "true")
		status = http.StatusOK
	}
	resp := publishResponse{Event: eventResource(got), Deliveries: make([]deliveryJSON, 0, len(ds))}
	for _, d := range ds {
		resp.Deliveries = append(resp.Deliveries, deliveryResource(webhook.DeliveryDetail{Delivery: d}, false))
	}
	writeJSON(w, status, resp)
}

// requestHash fingerprints the semantic content of a publish so that a
// retried request matches regardless of JSON whitespace, while a different
// request under the same key is detected.
func requestHash(eventType string, compactPayload []byte) []byte {
	h := sha256.New()
	h.Write([]byte(eventType))
	h.Write([]byte{0})
	h.Write(compactPayload)
	return h.Sum(nil)
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, eventResource(ev))
}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	ds, err := s.store.ListDeliveries(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]deliveryJSON, 0, len(ds))
	for _, d := range ds {
		out = append(out, deliveryResource(d, true))
	}
	writeJSON(w, http.StatusOK, listJSON[deliveryJSON]{Data: out})
}

func (s *Server) replayDelivery(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.ReplayDelivery(r.Context(), r.PathValue("id"), s.now().UTC())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, deliveryResource(webhook.DeliveryDetail{Delivery: d}, false))
}

// decode reads a single JSON object from the request body into dst. On
// failure it writes the error response and returns false.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "malformed request body: "+err.Error())
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return false
	}
	return true
}

func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, webhook.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, webhook.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "Idempotency-Key was already used with a different request")
	case errors.Is(err, webhook.ErrNotDeadLettered):
		writeError(w, http.StatusConflict, "not_dead_lettered", "only dead-lettered deliveries can be replayed")
	case errors.Is(err, webhook.ErrEndpointDisabled):
		writeError(w, http.StatusConflict, "endpoint_disabled", "the endpoint is disabled; enable it before replaying")
	default:
		s.log.LogAttrs(r.Context(), slog.LevelError, "store error",
			slog.String("method", r.Method), slog.String("path", r.URL.Path), slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

type errorJSON struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorJSON{Error: errorBody{Code: code, Message: msg}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v) // the client went away; nothing useful to do.
}
