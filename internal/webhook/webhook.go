// Package webhook defines the shared domain model.
package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"
)

// Store errors.
var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
	ErrNotDeadLettered     = errors.New("delivery is not dead-lettered")
	ErrEndpointDisabled    = errors.New("endpoint is disabled")
	ErrLeaseLost           = errors.New("delivery lease lost")
)

// Endpoint is a receiver URL that subscribes to events.
type Endpoint struct {
	ID             string
	URL            string
	Secret         string
	Description    string
	EventTypes     []string // empty means all
	CreatedAt      time.Time
	DisabledAt     *time.Time
	DisabledReason string
}

// Disabled reports whether the endpoint no longer receives deliveries.
func (e Endpoint) Disabled() bool { return e.DisabledAt != nil }

// Accepts reports whether the endpoint subscribes to eventType.
func (e Endpoint) Accepts(eventType string) bool {
	return len(e.EventTypes) == 0 || slices.Contains(e.EventTypes, eventType)
}

// ValidateURL checks that raw is an absolute http or https URL.
func ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid url: scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("invalid url: missing host")
	}
	if u.User != nil {
		return errors.New("invalid url: credentials in the URL are not allowed")
	}
	return nil
}

// Event is a message published by a producer.
type Event struct {
	ID             string
	Type           string
	Payload        json.RawMessage
	IdempotencyKey string
	RequestHash    []byte // detects a reused key with a different body
	CreatedAt      time.Time
}

// Body returns the JSON sent to receivers. It is byte-identical across retries.
func (e Event) Body() []byte {
	var b bytes.Buffer
	b.Grow(len(e.Payload) + len(e.ID) + len(e.Type) + 80)
	b.WriteString(`{"id":`)
	writeJSONString(&b, e.ID)
	b.WriteString(`,"type":`)
	writeJSONString(&b, e.Type)
	b.WriteString(`,"timestamp":"`)
	b.WriteString(e.CreatedAt.UTC().Format(time.RFC3339Nano))
	b.WriteString(`","data":`)
	b.Write(e.Payload)
	b.WriteByte('}')
	return b.Bytes()
}

func writeJSONString(b *bytes.Buffer, s string) {
	enc, _ := json.Marshal(s)
	b.Write(enc)
}

// DeliveryStatus is the lifecycle state of a delivery.
type DeliveryStatus string

// Delivery states. Replay moves dead back to pending.
const (
	StatusPending   DeliveryStatus = "pending"
	StatusSucceeded DeliveryStatus = "succeeded"
	StatusDead      DeliveryStatus = "dead"
)

// Delivery tracks one event being delivered to one endpoint.
type Delivery struct {
	ID            string
	EventID       string
	EndpointID    string
	Status        DeliveryStatus
	AttemptCount  int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Attempt records a single HTTP request made for a delivery.
type Attempt struct {
	DeliveryID   string
	Number       int
	StartedAt    time.Time
	Duration     time.Duration
	StatusCode   int // 0 if no response
	Error        string
	ResponseBody string // truncated
}

// DeliveryDetail is a delivery together with its attempt history.
type DeliveryDetail struct {
	Delivery
	Attempts []Attempt
}

// Job is a claimed delivery with everything needed to perform an attempt.
type Job struct {
	Delivery   Delivery
	Event      Event
	Endpoint   Endpoint
	LeaseToken string // fences completion
}

// Outcome is the result of an attempt as decided by the delivery engine.
type Outcome string

// Attempt outcomes.
const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeRetry     Outcome = "retry"
	OutcomeDead      Outcome = "dead"
)

// Result is the engine's verdict on an attempt.
type Result struct {
	DeliveryID      string
	LeaseToken      string
	Attempt         Attempt
	Outcome         Outcome
	NextAttemptAt   time.Time // OutcomeRetry only
	DisableEndpoint bool      // on 410, also dead-letters the endpoint's backlog
	FinishedAt      time.Time
}

// QueueStats summarises the pending backlog.
type QueueStats struct {
	Pending int
	Due     int
}
