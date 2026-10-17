// Package metrics defines the Prometheus instruments exported by hookrelay.
//
// Instruments are registered on an injected Registerer rather than the global
// default registry, so tests can create isolated instances.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "hookrelay"

// Metrics holds every instrument. The zero value is not usable; use New.
type Metrics struct {
	// Deliveries counts attempt verdicts by outcome (succeeded, retry, dead).
	Deliveries *prometheus.CounterVec
	// AttemptDuration observes HTTP attempt latency by response class
	// (2xx, 3xx, 4xx, 5xx, error).
	AttemptDuration *prometheus.HistogramVec
	// QueueDepth reports pending deliveries, split into all pending and
	// those already due.
	QueueDepth *prometheus.GaugeVec
	// InFlight reports attempts currently executing in this process.
	InFlight prometheus.Gauge
	// LeaseLost counts completions rejected because another worker had
	// re-claimed the delivery; each one is a potential duplicate delivery.
	LeaseLost prometheus.Counter
	// StoreErrors counts failed store operations by operation.
	StoreErrors *prometheus.CounterVec
	// EventsPublished counts publish requests by result (created, duplicate).
	EventsPublished *prometheus.CounterVec
	// HTTPRequests observes API request latency by route pattern and status.
	HTTPRequests *prometheus.HistogramVec
}

// New creates the instruments and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "delivery_attempts_total",
			Help:      "Delivery attempts by outcome.",
		}, []string{"outcome"}),
		AttemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "delivery_attempt_duration_seconds",
			Help:      "Latency of outbound webhook requests by response class.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"class"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Pending deliveries; state=due counts those whose next attempt time has passed.",
		}, []string{"state"}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "deliveries_in_flight",
			Help:      "Delivery attempts currently executing in this process.",
		}),
		LeaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "lease_lost_total",
			Help:      "Attempt results discarded because the delivery lease had expired and been re-claimed.",
		}),
		StoreErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "store_errors_total",
			Help:      "Failed store operations by operation.",
		}, []string{"op"}),
		EventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_published_total",
			Help:      "Publish requests by result.",
		}, []string{"result"}),
		HTTPRequests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "API request latency by route and status code.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route", "code"}),
	}
	reg.MustRegister(m.Deliveries, m.AttemptDuration, m.QueueDepth, m.InFlight, m.LeaseLost, m.StoreErrors, m.EventsPublished, m.HTTPRequests)
	return m
}

// StatusClass maps an HTTP status code to a low-cardinality label value;
// 0 means no response was received.
func StatusClass(code int) string {
	switch {
	case code == 0:
		return "error"
	case code >= 100 && code < 600:
		return strconv.Itoa(code/100) + "xx"
	default:
		return "other"
	}
}

// Middleware records request latency for h. Routes are labelled with the
// matched ServeMux pattern, never the raw path, to bound cardinality.
func (m *Metrics) Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
