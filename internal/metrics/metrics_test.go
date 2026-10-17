package metrics

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestStatusClass(t *testing.T) {
	t.Parallel()
	tests := map[int]string{0: "error", 200: "2xx", 204: "2xx", 302: "3xx", 410: "4xx", 503: "5xx", 99: "other", 600: "other"}
	for code, want := range tests {
		if got := StatusClass(code); got != want {
			t.Errorf("StatusClass(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestMiddlewareRecordsRouteAndStatus(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := New(reg)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("hi")) })
	mux.HandleFunc("GET /teapot/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		w.WriteHeader(http.StatusOK) // superfluous; must not relabel the request.
	})
	h := m.Middleware(mux)
	for _, path := range []string{"/ok", "/teapot/1", "/teapot/2", "/missing"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range families {
		if f.GetName() != "hookrelay_http_request_duration_seconds" {
			continue
		}
		for _, metric := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range metric.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			got = append(got, labels["route"]+" "+labels["code"])
		}
	}
	slices.Sort(got)
	want := []string{"GET /ok 200", "GET /teapot/{id} 418", "unmatched 404"}
	if !slices.Equal(got, want) {
		t.Fatalf("series = %q, want %q", got, want)
	}
}
