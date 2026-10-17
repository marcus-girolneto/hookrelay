package receiver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcus-girolneto/hookrelay/signature"
)

func setup(t *testing.T, opts ...Option) (*Handler, *signature.Signer) {
	t.Helper()
	secret := signature.NewSecret()
	v, err := signature.NewVerifier(secret)
	if err != nil {
		t.Fatal(err)
	}
	s, err := signature.NewSigner(secret)
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(v, slog.New(slog.NewTextHandler(io.Discard, nil)), 2, opts...), s
}

func send(t *testing.T, h http.Handler, s *signature.Signer, msgID, body string, tamper bool) int {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhooks", strings.NewReader(body))
	s.SetHeaders(req.Header, msgID, time.Now(), []byte(body))
	if tamper {
		req.Header.Set(signature.HeaderSignature, "v1,AAAA")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const body = `{"id":"evt_1","type":"invoice.paid","timestamp":"2026-05-01T00:00:00Z","data":{"amount":1}}`

func TestReceiver(t *testing.T) {
	t.Parallel()
	h, s := setup(t)
	if code := send(t, h, s, "msg_1", body, false); code != http.StatusNoContent {
		t.Fatalf("valid webhook: %d", code)
	}
	if code := send(t, h, s, "msg_1", body, false); code != http.StatusNoContent {
		t.Fatalf("duplicate webhook: %d", code)
	}
	if !h.seen.contains("msg_1") {
		t.Fatal("message not recorded")
	}
	if code := send(t, h, s, "msg_2", body, true); code != http.StatusUnauthorized {
		t.Fatalf("tampered webhook: %d", code)
	}
	if code := send(t, h, s, "msg_3", "not json", false); code != http.StatusBadRequest {
		t.Fatalf("invalid json: %d", code)
	}
}

func TestReceiverSimulatedFailure(t *testing.T) {
	t.Parallel()
	h, s := setup(t, WithFailureRate(0.5, func() float64 { return 0.1 }))
	if code := send(t, h, s, "msg_1", body, false); code != http.StatusServiceUnavailable {
		t.Fatalf("expected simulated 503, got %d", code)
	}
	if h.seen.contains("msg_1") {
		t.Fatal("failed message must not be recorded as seen")
	}
}

func TestReceiverBodyLimit(t *testing.T) {
	t.Parallel()
	h, s := setup(t)
	if code := send(t, h, s, "msg_1", strings.Repeat("x", maxBody+1), false); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %d", code)
	}
}

func TestDedupEvictsOldest(t *testing.T) {
	t.Parallel()
	d := newDedup(2)
	d.add("a")
	d.add("b")
	d.add("b")
	d.add("c")
	if d.contains("a") || !d.contains("b") || !d.contains("c") {
		t.Fatalf("unexpected contents: %v", d.ids)
	}
	if z := newDedup(0); len(z.ring) != 1 {
		t.Fatal("size must be clamped to 1")
	}
}
