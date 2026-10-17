package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/marcus-girolneto/hookrelay/internal/memstore"
	"github.com/marcus-girolneto/hookrelay/internal/metrics"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

var fixedNow = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

type harness struct {
	t       *testing.T
	srv     *Server
	store   *memstore.Store
	metrics *metrics.Metrics
	handler http.Handler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store := memstore.New()
	m := metrics.New(prometheus.NewRegistry())
	srv := New(store, m, slog.New(slog.NewTextHandler(io.Discard, nil)), WithClock(func() time.Time { return fixedNow }))
	return &harness{t: t, srv: srv, store: store, metrics: m, handler: srv.Handler()}
}

func (h *harness) do(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequestWithContext(h.t.Context(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func decodeInto[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func expectStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, want, rec.Body.String())
	}
}

func (h *harness) createEndpoint(body string) endpointJSON {
	h.t.Helper()
	rec := h.do(http.MethodPost, "/v1/endpoints", body)
	expectStatus(h.t, rec, http.StatusCreated)
	return decodeInto[endpointJSON](h.t, rec)
}

func TestHealth(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	expectStatus(t, h.do(http.MethodGet, "/healthz", ""), http.StatusOK)
	expectStatus(t, h.do(http.MethodGet, "/readyz", ""), http.StatusOK)
	h.srv.SetDraining()
	expectStatus(t, h.do(http.MethodGet, "/readyz", ""), http.StatusServiceUnavailable)
	expectStatus(t, h.do(http.MethodGet, "/healthz", ""), http.StatusOK)
}

func TestCreateEndpoint(t *testing.T) {
	t.Parallel()
	secret := signature.NewSecret()
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "minimal", body: `{"url":"https://example.com/hook"}`, wantStatus: 201},
		{name: "with filter and secret", body: `{"url":"https://example.com/hook","event_types":["invoice.paid"],"secret":"` + secret + `","description":"billing"}`, wantStatus: 201},
		{name: "bad url", body: `{"url":"ftp://example.com"}`, wantStatus: 400, wantCode: "invalid_request"},
		{name: "missing url", body: `{}`, wantStatus: 400, wantCode: "invalid_request"},
		{name: "bad secret", body: `{"url":"https://example.com/hook","secret":"hunter2"}`, wantStatus: 400, wantCode: "invalid_request"},
		{name: "empty event type", body: `{"url":"https://example.com/hook","event_types":[""]}`, wantStatus: 400, wantCode: "invalid_request"},
		{name: "unknown field", body: `{"url":"https://example.com/hook","colour":"red"}`, wantStatus: 400, wantCode: "invalid_json"},
		{name: "trailing data", body: `{"url":"https://example.com/hook"} {}`, wantStatus: 400, wantCode: "invalid_json"},
		{name: "malformed", body: `{"url":`, wantStatus: 400, wantCode: "invalid_json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			rec := h.do(http.MethodPost, "/v1/endpoints", tc.body)
			expectStatus(t, rec, tc.wantStatus)
			if tc.wantCode != "" {
				if got := decodeInto[errorJSON](t, rec).Error.Code; got != tc.wantCode {
					t.Fatalf("error code = %q, want %q", got, tc.wantCode)
				}
				return
			}
			ep := decodeInto[endpointJSON](t, rec)
			if !strings.HasPrefix(ep.ID, "ep_") || ep.Disabled || ep.EventTypes == nil {
				t.Fatalf("unexpected endpoint %+v", ep)
			}
			if _, err := signature.DecodeSecret(ep.Secret); err != nil {
				t.Fatalf("create response must include a usable secret: %v", err)
			}
		})
	}
}

func TestCreateEndpointRejectsWrongContentType(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	rec := h.do(http.MethodPost, "/v1/endpoints", `{"url":"https://example.com"}`, "Content-Type", "text/plain")
	expectStatus(t, rec, http.StatusUnsupportedMediaType)
}

func TestEndpointReadsHideSecret(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ep := h.createEndpoint(`{"url":"https://example.com/hook"}`)

	rec := h.do(http.MethodGet, "/v1/endpoints/"+ep.ID, "")
	expectStatus(t, rec, http.StatusOK)
	if strings.Contains(rec.Body.String(), ep.Secret) || strings.Contains(rec.Body.String(), `"secret"`) {
		t.Fatalf("GET exposes the secret: %s", rec.Body.String())
	}
	list := decodeInto[listJSON[endpointJSON]](t, h.do(http.MethodGet, "/v1/endpoints", ""))
	if len(list.Data) != 1 || list.Data[0].Secret != "" {
		t.Fatalf("list = %+v", list)
	}
	expectStatus(t, h.do(http.MethodGet, "/v1/endpoints/ep_missing", ""), http.StatusNotFound)
	expectStatus(t, h.do(http.MethodPost, "/v1/endpoints/ep_missing/enable", ""), http.StatusNotFound)
	expectStatus(t, h.do(http.MethodPost, "/v1/endpoints/"+ep.ID+"/enable", ""), http.StatusOK)
}

func TestPublishEvent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.createEndpoint(`{"url":"https://example.com/all"}`)
	h.createEndpoint(`{"url":"https://example.com/paid","event_types":["invoice.paid"]}`)
	h.createEndpoint(`{"url":"https://example.com/other","event_types":["customer.created"]}`)

	body := `{"type":"invoice.paid","payload":{"invoice":"in_1","amount":4200}}`
	rec := h.do(http.MethodPost, "/v1/events", body, IdempotencyHeader, "order-1")
	expectStatus(t, rec, http.StatusCreated)
	first := decodeInto[publishResponse](t, rec)
	if len(first.Deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2", len(first.Deliveries))
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, first.Event.Payload); err != nil {
		t.Fatal(err)
	}
	if compact.String() != `{"invoice":"in_1","amount":4200}` || !first.Event.CreatedAt.Equal(fixedNow) {
		t.Fatalf("event = %+v", first.Event)
	}

	// Same key, same content modulo whitespace: the original is returned.
	retry := `{ "type": "invoice.paid", "payload": { "invoice": "in_1", "amount": 4200 } }`
	rec = h.do(http.MethodPost, "/v1/events", retry, IdempotencyHeader, "order-1")
	expectStatus(t, rec, http.StatusOK)
	if rec.Header().Get(ReplayedHeader) != "true" {
		t.Fatal("replayed response missing Idempotent-Replayed header")
	}
	again := decodeInto[publishResponse](t, rec)
	if again.Event.ID != first.Event.ID || len(again.Deliveries) != 2 {
		t.Fatalf("retry returned %+v, want original %s", again.Event, first.Event.ID)
	}

	// Same key, different content: rejected.
	rec = h.do(http.MethodPost, "/v1/events", `{"type":"invoice.paid","payload":{"amount":1}}`, IdempotencyHeader, "order-1")
	expectStatus(t, rec, http.StatusUnprocessableEntity)

	if got := testutil.ToFloat64(h.metrics.EventsPublished.WithLabelValues("created")); got != 1 {
		t.Errorf("created counter = %v", got)
	}
	if got := testutil.ToFloat64(h.metrics.EventsPublished.WithLabelValues("duplicate")); got != 1 {
		t.Errorf("duplicate counter = %v", got)
	}

	rec = h.do(http.MethodGet, "/v1/events/"+first.Event.ID, "")
	expectStatus(t, rec, http.StatusOK)
	if got := decodeInto[eventJSON](t, rec); got.Type != "invoice.paid" {
		t.Fatalf("GET event = %+v", got)
	}
	expectStatus(t, h.do(http.MethodGet, "/v1/events/evt_missing", ""), http.StatusNotFound)
}

func TestPublishEventValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		key        string
		body       string
		wantStatus int
	}{
		{name: "missing key", body: `{"type":"a","payload":{}}`, wantStatus: 400},
		{name: "key too long", key: strings.Repeat("k", 256), body: `{"type":"a","payload":{}}`, wantStatus: 400},
		{name: "missing type", key: "k", body: `{"payload":{}}`, wantStatus: 400},
		{name: "type too long", key: "k", body: `{"type":"` + strings.Repeat("t", 256) + `","payload":{}}`, wantStatus: 400},
		{name: "missing payload", key: "k", body: `{"type":"a"}`, wantStatus: 400},
		{name: "null payload", key: "k", body: `{"type":"a","payload":null}`, wantStatus: 400},
		{name: "too large", key: "k", body: `{"type":"a","payload":"` + strings.Repeat("x", maxBodyBytes) + `"}`, wantStatus: 413},
		{name: "scalar payload ok", key: "k", body: `{"type":"a","payload":42}`, wantStatus: 201},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			var headers []string
			if tc.key != "" {
				headers = []string{IdempotencyHeader, tc.key}
			}
			expectStatus(t, h.do(http.MethodPost, "/v1/events", tc.body, headers...), tc.wantStatus)
		})
	}
}

func TestDeliveriesAndReplay(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.createEndpoint(`{"url":"https://example.com/hook"}`)
	rec := h.do(http.MethodPost, "/v1/events", `{"type":"a","payload":{}}`, IdempotencyHeader, "k")
	expectStatus(t, rec, http.StatusCreated)
	pub := decodeInto[publishResponse](t, rec)
	dlvID := pub.Deliveries[0].ID

	// A pending delivery cannot be replayed.
	rec = h.do(http.MethodPost, "/v1/deliveries/"+dlvID+"/replay", "")
	expectStatus(t, rec, http.StatusConflict)

	// Drive the delivery to dead-letter through the store, as the engine would.
	ctx := t.Context()
	jobs, err := h.store.ClaimDue(ctx, fixedNow, 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	if err := h.store.Complete(ctx, webhook.Result{
		DeliveryID: dlvID, LeaseToken: jobs[0].LeaseToken, Outcome: webhook.OutcomeDead, FinishedAt: fixedNow,
		Attempt: webhook.Attempt{DeliveryID: dlvID, Number: 1, StartedAt: fixedNow, Duration: 1500 * time.Microsecond, StatusCode: 500, Error: "unexpected status 500", ResponseBody: "oops"},
	}); err != nil {
		t.Fatal(err)
	}

	rec = h.do(http.MethodGet, "/v1/events/"+pub.Event.ID+"/deliveries", "")
	expectStatus(t, rec, http.StatusOK)
	list := decodeInto[listJSON[deliveryJSON]](t, rec)
	if len(list.Data) != 1 {
		t.Fatalf("deliveries = %+v", list)
	}
	d := list.Data[0]
	if d.Status != "dead" || d.NextAttemptAt != nil || len(d.Attempts) != 1 {
		t.Fatalf("delivery = %+v", d)
	}
	if a := d.Attempts[0]; a.StatusCode != 500 || a.DurationMS != 1.5 || a.ResponseBody != "oops" {
		t.Fatalf("attempt = %+v", a)
	}

	rec = h.do(http.MethodPost, "/v1/deliveries/"+dlvID+"/replay", "")
	expectStatus(t, rec, http.StatusAccepted)
	replayed := decodeInto[deliveryJSON](t, rec)
	if replayed.Status != "pending" || replayed.AttemptCount != 0 || replayed.NextAttemptAt == nil {
		t.Fatalf("replayed = %+v", replayed)
	}
	expectStatus(t, h.do(http.MethodPost, "/v1/deliveries/dlv_missing/replay", ""), http.StatusNotFound)
	expectStatus(t, h.do(http.MethodGet, "/v1/events/evt_missing/deliveries", ""), http.StatusNotFound)
}

func TestReplayToDisabledEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ep := h.createEndpoint(`{"url":"https://example.com/hook"}`)
	pub := decodeInto[publishResponse](t, h.do(http.MethodPost, "/v1/events", `{"type":"a","payload":{}}`, IdempotencyHeader, "k"))
	jobs, _ := h.store.ClaimDue(t.Context(), fixedNow, 1, time.Minute)
	_ = h.store.Complete(t.Context(), webhook.Result{
		DeliveryID: jobs[0].Delivery.ID, LeaseToken: jobs[0].LeaseToken, Outcome: webhook.OutcomeDead, DisableEndpoint: true, FinishedAt: fixedNow,
		Attempt: webhook.Attempt{Number: 1, StatusCode: 410, Error: "gone"},
	})

	got := decodeInto[endpointJSON](t, h.do(http.MethodGet, "/v1/endpoints/"+ep.ID, ""))
	if !got.Disabled || got.DisabledAt == nil || got.DisabledReason != "gone" {
		t.Fatalf("endpoint = %+v", got)
	}
	rec := h.do(http.MethodPost, "/v1/deliveries/"+pub.Deliveries[0].ID+"/replay", "")
	expectStatus(t, rec, http.StatusConflict)
	if code := decodeInto[errorJSON](t, rec).Error.Code; code != "endpoint_disabled" {
		t.Fatalf("code = %q", code)
	}
}

type brokenStore struct{ Store }

var errBroken = errors.New("connection reset")

func (brokenStore) Ping(context.Context) error { return errBroken }
func (brokenStore) ListEndpoints(context.Context) ([]webhook.Endpoint, error) {
	return nil, errBroken
}

func TestStoreFailures(t *testing.T) {
	t.Parallel()
	m := metrics.New(prometheus.NewRegistry())
	srv := New(brokenStore{memstore.New()}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := &harness{t: t, srv: srv, metrics: m, handler: srv.Handler()}

	expectStatus(t, h.do(http.MethodGet, "/readyz", ""), http.StatusServiceUnavailable)
	rec := h.do(http.MethodGet, "/v1/endpoints", "")
	expectStatus(t, rec, http.StatusInternalServerError)
	if strings.Contains(rec.Body.String(), errBroken.Error()) {
		t.Fatal("internal error details leaked to the client")
	}
}

func TestMetricsUseRoutePatterns(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.do(http.MethodGet, "/v1/events/evt_a", "")
	h.do(http.MethodGet, "/v1/events/evt_b", "")
	h.do(http.MethodGet, "/nope", "")
	if n := testutil.CollectAndCount(h.metrics.HTTPRequests); n != 2 {
		t.Fatalf("got %d series, want 2 (one per route pattern)", n)
	}
	if n := testutil.CollectAndCount(h.metrics.HTTPRequests, "hookrelay_http_request_duration_seconds"); n != 2 {
		t.Fatalf("got %d series", n)
	}
}
