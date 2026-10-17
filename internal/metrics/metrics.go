// Package metrics defines hookrelay's Prometheus instruments.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "hookrelay"

// Metrics holds every instrument. Use New.
type Metrics struct {
	Deliveries      *prometheus.CounterVec
	AttemptDuration *prometheus.HistogramVec
	QueueDepth      *prometheus.GaugeVec
	InFlight        prometheus.Gauge
	LeaseLost       prometheus.Counter // each one is a potential duplicate delivery
	StoreErrors     *prometheus.CounterVec
	EventsPublished *prometheus.CounterVec
	HTTPRequests    *prometheus.HistogramVec
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

// StatusClass maps a status code to "2xx" etc. 0 maps to "error".
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

// Middleware records request latency, labelled by mux pattern.
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
