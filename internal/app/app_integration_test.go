//go:build integration

package app_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/app"
	"github.com/marcus-girolneto/hookrelay/internal/config"
	"github.com/marcus-girolneto/hookrelay/internal/pgtest"
	"github.com/marcus-girolneto/hookrelay/signature"
)

func TestEndToEnd(t *testing.T) {
	secret := signature.NewSecret()
	verifier, err := signature.NewVerifier(secret)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := verifier.Verify(r.Header, body); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)

	cfg, err := config.Load([]string{
		"-addr", "127.0.0.1:0",
		"-database-url", pgtest.NewDatabase(t),
		"-poll-interval", "10ms",
		"-request-timeout", "2s",
		"-lease", "10s",
		"-backoff-base", "10ms",
		"-backoff-max", "50ms",
	}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	ready := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), "test", ready)
	}()
	var base string
	select {
	case addr := <-ready:
		base = "http://" + addr.String()
	case err := <-done:
		t.Fatalf("app exited during startup: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start")
	}

	c := &client{t: t, base: base}
	c.expect(http.MethodGet, "/readyz", "", nil, http.StatusOK)

	var ep struct{ ID string }
	c.expect(http.MethodPost, "/v1/endpoints", `{"url":"`+receiver.URL+`","secret":"`+secret+`"}`, &ep, http.StatusCreated)

	var pub struct {
		Event struct{ ID string }
	}
	c.expect(http.MethodPost, "/v1/events", `{"type":"invoice.paid","payload":{"amount":4200}}`, &pub, http.StatusCreated, "Idempotency-Key", "inv-1")
	c.expect(http.MethodPost, "/v1/events", `{"type":"invoice.paid","payload":{"amount":4200}}`, nil, http.StatusOK, "Idempotency-Key", "inv-1")

	type deliveries struct {
		Data []struct {
			Status       string
			AttemptCount int `json:"attempt_count"`
			Attempts     []struct {
				StatusCode int `json:"status_code"`
			}
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	var got deliveries
	for {
		c.expect(http.MethodGet, "/v1/events/"+pub.Event.ID+"/deliveries", "", &got, http.StatusOK)
		if len(got.Data) == 1 && got.Data[0].Status == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not succeed: %+v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	d := got.Data[0]
	if d.AttemptCount != 2 || len(d.Attempts) != 2 || d.Attempts[0].StatusCode != 503 || d.Attempts[1].StatusCode != 204 {
		t.Fatalf("unexpected attempt history: %+v", d)
	}

	resp := c.do(http.MethodGet, "/metrics", "")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	for _, want := range []string{
		`hookrelay_delivery_attempts_total{outcome="succeeded"} 1`,
		`hookrelay_delivery_attempts_total{outcome="retry"} 1`,
		`hookrelay_events_published_total{result="duplicate"} 1`,
		`hookrelay_delivery_attempt_duration_seconds_count{class="5xx"} 1`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics missing %q", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("app did not shut down")
	}
}

type client struct {
	t    *testing.T
	base string
}

func (c *client) do(method, path, body string, headers ...string) *http.Response {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.base+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func (c *client) expect(method, path, body string, out any, status int, headers ...string) {
	c.t.Helper()
	resp := c.do(method, path, body, headers...)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != status {
		c.t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, status, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			c.t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
}
