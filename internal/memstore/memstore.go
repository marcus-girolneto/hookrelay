// Package memstore is an in-memory implementation of the hookrelay store.
//
// It mirrors the semantics of the Postgres store closely enough to run the
// shared conformance suite in package storetest, which makes it a trustworthy
// fake for unit-testing the API and the delivery engine. It is not intended
// for production use: nothing is persisted.
package memstore

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
)

type delivery struct {
	webhook.Delivery
	leaseToken string
}

// Store is an in-memory store safe for concurrent use.
type Store struct {
	mu          sync.Mutex
	endpoints   map[string]webhook.Endpoint
	events      map[string]webhook.Event
	idempotency map[string]string
	deliveries  map[string]*delivery
	attempts    map[string][]webhook.Attempt
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		endpoints:   make(map[string]webhook.Endpoint),
		events:      make(map[string]webhook.Event),
		idempotency: make(map[string]string),
		deliveries:  make(map[string]*delivery),
		attempts:    make(map[string][]webhook.Attempt),
	}
}

// Ping always succeeds.
func (s *Store) Ping(context.Context) error { return nil }

// CreateEndpoint stores ep.
func (s *Store) CreateEndpoint(_ context.Context, ep webhook.Endpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints[ep.ID] = cloneEndpoint(ep)
	return nil
}

// GetEndpoint returns the endpoint with the given id.
func (s *Store) GetEndpoint(_ context.Context, id string) (webhook.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.endpoints[id]
	if !ok {
		return webhook.Endpoint{}, webhook.ErrNotFound
	}
	return cloneEndpoint(ep), nil
}

// ListEndpoints returns all endpoints ordered by id.
func (s *Store) ListEndpoints(context.Context) ([]webhook.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]webhook.Endpoint, 0, len(s.endpoints))
	for _, ep := range s.endpoints {
		out = append(out, cloneEndpoint(ep))
	}
	slices.SortFunc(out, func(a, b webhook.Endpoint) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

// EnableEndpoint clears a previous disable.
func (s *Store) EnableEndpoint(_ context.Context, id string) (webhook.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.endpoints[id]
	if !ok {
		return webhook.Endpoint{}, webhook.ErrNotFound
	}
	ep.DisabledAt = nil
	ep.DisabledReason = ""
	s.endpoints[id] = ep
	return cloneEndpoint(ep), nil
}

// PublishEvent stores ev and fans it out to every enabled, subscribed
// endpoint. If ev.IdempotencyKey was seen before with the same request hash,
// the original event and deliveries are returned with created=false.
func (s *Store) PublishEvent(_ context.Context, ev webhook.Event) (webhook.Event, []webhook.Delivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, ok := s.idempotency[ev.IdempotencyKey]; ok {
		existing := s.events[existingID]
		if !bytes.Equal(existing.RequestHash, ev.RequestHash) {
			return webhook.Event{}, nil, false, webhook.ErrIdempotencyMismatch
		}
		return cloneEvent(existing), s.deliveriesForEvent(existingID), false, nil
	}

	ev = cloneEvent(ev)
	s.events[ev.ID] = ev
	s.idempotency[ev.IdempotencyKey] = ev.ID

	var created []webhook.Delivery
	for _, ep := range s.endpoints {
		if ep.Disabled() || !ep.Accepts(ev.Type) {
			continue
		}
		d := webhook.Delivery{
			ID:            id.New("dlv"),
			EventID:       ev.ID,
			EndpointID:    ep.ID,
			Status:        webhook.StatusPending,
			NextAttemptAt: ev.CreatedAt,
			CreatedAt:     ev.CreatedAt,
			UpdatedAt:     ev.CreatedAt,
		}
		s.deliveries[d.ID] = &delivery{Delivery: d}
		created = append(created, d)
	}
	sortDeliveries(created)
	return cloneEvent(ev), created, true, nil
}

// GetEvent returns the event with the given id.
func (s *Store) GetEvent(_ context.Context, id string) (webhook.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[id]
	if !ok {
		return webhook.Event{}, webhook.ErrNotFound
	}
	return cloneEvent(ev), nil
}

// ListDeliveries returns the deliveries of an event with their attempts.
func (s *Store) ListDeliveries(_ context.Context, eventID string) ([]webhook.DeliveryDetail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[eventID]; !ok {
		return nil, webhook.ErrNotFound
	}
	ds := s.deliveriesForEvent(eventID)
	out := make([]webhook.DeliveryDetail, 0, len(ds))
	for _, d := range ds {
		out = append(out, webhook.DeliveryDetail{
			Delivery: d,
			Attempts: slices.Clone(s.attempts[d.ID]),
		})
	}
	return out, nil
}

// ReplayDelivery moves a dead delivery back to pending with a fresh attempt
// budget, due immediately.
func (s *Store) ReplayDelivery(_ context.Context, id string, now time.Time) (webhook.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok {
		return webhook.Delivery{}, webhook.ErrNotFound
	}
	if d.Status != webhook.StatusDead {
		return webhook.Delivery{}, webhook.ErrNotDeadLettered
	}
	if s.endpoints[d.EndpointID].Disabled() {
		return webhook.Delivery{}, webhook.ErrEndpointDisabled
	}
	d.Status = webhook.StatusPending
	d.AttemptCount = 0
	d.NextAttemptAt = now
	d.LastError = ""
	d.UpdatedAt = now
	d.leaseToken = ""
	return d.Delivery, nil
}

// ClaimDue leases up to limit due deliveries by pushing their next attempt
// time lease into the future. A claimant that crashes simply lets the lease
// expire, after which the delivery becomes due again.
func (s *Store) ClaimDue(_ context.Context, now time.Time, limit int, lease time.Duration) ([]webhook.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var due []*delivery
	for _, d := range s.deliveries {
		if d.Status != webhook.StatusPending || d.NextAttemptAt.After(now) {
			continue
		}
		if s.endpoints[d.EndpointID].Disabled() {
			continue
		}
		due = append(due, d)
	}
	slices.SortFunc(due, func(a, b *delivery) int {
		return cmp.Or(a.NextAttemptAt.Compare(b.NextAttemptAt), cmp.Compare(a.ID, b.ID))
	})
	if len(due) > limit {
		due = due[:limit]
	}

	token := id.New("lease")
	jobs := make([]webhook.Job, 0, len(due))
	for _, d := range due {
		d.leaseToken = token
		d.NextAttemptAt = now.Add(lease)
		d.UpdatedAt = now
		jobs = append(jobs, webhook.Job{
			Delivery:   d.Delivery,
			Event:      cloneEvent(s.events[d.EventID]),
			Endpoint:   cloneEndpoint(s.endpoints[d.EndpointID]),
			LeaseToken: token,
		})
	}
	return jobs, nil
}

// Complete records an attempt and applies the engine's verdict. The attempt
// is always recorded; the state change is applied only if r.LeaseToken still
// holds the lease, otherwise webhook.ErrLeaseLost is returned.
func (s *Store) Complete(_ context.Context, r webhook.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[r.DeliveryID]
	if !ok {
		return webhook.ErrNotFound
	}
	s.attempts[d.ID] = append(s.attempts[d.ID], r.Attempt)

	if d.leaseToken == "" || d.leaseToken != r.LeaseToken || d.Status != webhook.StatusPending {
		return webhook.ErrLeaseLost
	}
	d.leaseToken = ""
	d.AttemptCount = r.Attempt.Number
	d.UpdatedAt = r.FinishedAt
	d.LastError = r.Attempt.Error
	switch r.Outcome {
	case webhook.OutcomeSucceeded:
		d.Status = webhook.StatusSucceeded
		d.LastError = ""
	case webhook.OutcomeRetry:
		d.NextAttemptAt = r.NextAttemptAt
	case webhook.OutcomeDead:
		d.Status = webhook.StatusDead
	}

	if r.DisableEndpoint {
		ep := s.endpoints[d.EndpointID]
		if !ep.Disabled() {
			at := r.FinishedAt
			ep.DisabledAt = &at
			ep.DisabledReason = r.Attempt.Error
			s.endpoints[ep.ID] = ep
		}
		for _, other := range s.deliveries {
			if other.EndpointID == d.EndpointID && other.Status == webhook.StatusPending {
				other.Status = webhook.StatusDead
				other.LastError = webhook.ErrEndpointDisabled.Error()
				other.UpdatedAt = r.FinishedAt
				other.leaseToken = ""
			}
		}
	}
	return nil
}

// QueueStats reports the pending backlog.
func (s *Store) QueueStats(_ context.Context, now time.Time) (webhook.QueueStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st webhook.QueueStats
	for _, d := range s.deliveries {
		if d.Status != webhook.StatusPending {
			continue
		}
		st.Pending++
		if !d.NextAttemptAt.After(now) {
			st.Due++
		}
	}
	return st, nil
}

func (s *Store) deliveriesForEvent(eventID string) []webhook.Delivery {
	var out []webhook.Delivery
	for _, d := range s.deliveries {
		if d.EventID == eventID {
			out = append(out, d.Delivery)
		}
	}
	sortDeliveries(out)
	return out
}

func sortDeliveries(ds []webhook.Delivery) {
	slices.SortFunc(ds, func(a, b webhook.Delivery) int { return cmp.Compare(a.ID, b.ID) })
}

func cloneEndpoint(ep webhook.Endpoint) webhook.Endpoint {
	ep.EventTypes = slices.Clone(ep.EventTypes)
	if ep.DisabledAt != nil {
		at := *ep.DisabledAt
		ep.DisabledAt = &at
	}
	return ep
}

func cloneEvent(ev webhook.Event) webhook.Event {
	ev.Payload = slices.Clone(ev.Payload)
	ev.RequestHash = slices.Clone(ev.RequestHash)
	return ev
}
