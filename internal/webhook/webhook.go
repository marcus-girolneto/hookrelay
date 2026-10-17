// Package webhook defines the domain model shared by the API, the delivery
// engine and the storage implementations.
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

// Sentinel errors returned by stores. Callers match them with errors.Is.
var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
	ErrNotDeadLettered     = errors.New("delivery is not dead-lettered")
	ErrEndpointDisabled    = errors.New("endpoint is disabled")
	ErrLeaseLost           = errors.New("delivery lease lost")
)

// Endpoint is a receiver URL that subscribes to events.
type Endpoint struct {
	ID          string
	URL         string
	Secret      string
	Description string
	// EventTypes restricts which events are delivered. Empty means all.
	EventTypes     []string
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
	// RequestHash fingerprints the publish request so that a reused
	// idempotency key with a different body can be rejected.
	RequestHash []byte
	CreatedAt   time.Time
}

// Body returns the JSON document sent to receivers. It is a pure function of
// the stored event, so every retry carries byte-identical content and the
// same signature input apart from the timestamp.
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
	enc, _ := json.Marshal(s) // marshaling a string cannot fail.
	b.Write(enc)
}

// DeliveryStatus is the lifecycle state of a delivery.
type DeliveryStatus string

// Delivery states. A delivery starts pending and ends either succeeded or
// dead; replay moves a dead delivery back to pending.
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
	DeliveryID string
	Number     int
	StartedAt  time.Time
	Duration   time.Duration
	// StatusCode is 0 when no response was received.
	StatusCode int
	Error      string
	// ResponseBody holds a truncated prefix of the receiver's response.
	ResponseBody string
}

// DeliveryDetail is a delivery together with its attempt history.
type DeliveryDetail struct {
	Delivery
	Attempts []Attempt
}

// Job is a claimed delivery with everything needed to perform an attempt.
type Job struct {
	Delivery Delivery
	Event    Event
	Endpoint Endpoint
	// LeaseToken fences completion: only the claimant holding the current
	// token may change the delivery's state.
	LeaseToken string
}

// Outcome is the result of an attempt as decided by the delivery engine.
type Outcome string

// Attempt outcomes.
const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeRetry     Outcome = "retry"
	OutcomeDead      Outcome = "dead"
)

// Result is the engine's verdict on an attempt, persisted atomically by
// the store together with the attempt itself.
type Result struct {
	DeliveryID string
	LeaseToken string
	Attempt    Attempt
	Outcome    Outcome
	// NextAttemptAt is meaningful only for OutcomeRetry.
	NextAttemptAt time.Time
	// DisableEndpoint is set when the receiver asked to stop deliveries
	// (HTTP 410). The store disables the endpoint and dead-letters its other
	// pending deliveries.
	DisableEndpoint bool
	FinishedAt      time.Time
}

// QueueStats summarises the pending backlog.
type QueueStats struct {
	// Pending counts deliveries not yet succeeded or dead.
	Pending int
	// Due counts pending deliveries whose next attempt time has passed.
	Due int
}
