package api

import (
	"encoding/json"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/webhook"
)

type listJSON[T any] struct {
	Data []T `json:"data"`
}

type endpointJSON struct {
	ID             string     `json:"id"`
	URL            string     `json:"url"`
	Description    string     `json:"description,omitempty"`
	EventTypes     []string   `json:"event_types"`
	Disabled       bool       `json:"disabled"`
	DisabledAt     *time.Time `json:"disabled_at,omitempty"`
	DisabledReason string     `json:"disabled_reason,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	Secret         string     `json:"secret,omitempty"` // only on create
}

func endpointResource(ep webhook.Endpoint, withSecret bool) endpointJSON {
	out := endpointJSON{
		ID:             ep.ID,
		URL:            ep.URL,
		Description:    ep.Description,
		EventTypes:     ep.EventTypes,
		Disabled:       ep.Disabled(),
		DisabledAt:     utcPtr(ep.DisabledAt),
		DisabledReason: ep.DisabledReason,
		CreatedAt:      ep.CreatedAt.UTC(),
	}
	if out.EventTypes == nil {
		out.EventTypes = []string{}
	}
	if withSecret {
		out.Secret = ep.Secret
	}
	return out
}

type eventJSON struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func eventResource(ev webhook.Event) eventJSON {
	return eventJSON{ID: ev.ID, Type: ev.Type, Payload: ev.Payload, CreatedAt: ev.CreatedAt.UTC()}
}

type deliveryJSON struct {
	ID            string        `json:"id"`
	EventID       string        `json:"event_id"`
	EndpointID    string        `json:"endpoint_id"`
	Status        string        `json:"status"`
	AttemptCount  int           `json:"attempt_count"`
	NextAttemptAt *time.Time    `json:"next_attempt_at,omitempty"`
	LastError     string        `json:"last_error,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
	Attempts      []attemptJSON `json:"attempts,omitempty"`
}

type attemptJSON struct {
	Number       int       `json:"number"`
	StartedAt    time.Time `json:"started_at"`
	DurationMS   float64   `json:"duration_ms"`
	StatusCode   int       `json:"status_code,omitempty"`
	Error        string    `json:"error,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
}

func deliveryResource(d webhook.DeliveryDetail, withAttempts bool) deliveryJSON {
	out := deliveryJSON{
		ID:           d.ID,
		EventID:      d.EventID,
		EndpointID:   d.EndpointID,
		Status:       string(d.Status),
		AttemptCount: d.AttemptCount,
		LastError:    d.LastError,
		CreatedAt:    d.CreatedAt.UTC(),
		UpdatedAt:    d.UpdatedAt.UTC(),
	}
	if d.Status == webhook.StatusPending {
		out.NextAttemptAt = utcPtr(&d.NextAttemptAt)
	}
	if withAttempts {
		out.Attempts = make([]attemptJSON, 0, len(d.Attempts))
		for _, a := range d.Attempts {
			out.Attempts = append(out.Attempts, attemptJSON{
				Number:       a.Number,
				StartedAt:    a.StartedAt.UTC(),
				DurationMS:   float64(a.Duration.Microseconds()) / 1000,
				StatusCode:   a.StatusCode,
				Error:        a.Error,
				ResponseBody: a.ResponseBody,
			})
		}
	}
	return out
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
