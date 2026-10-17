// Package storetest is a conformance suite for hookrelay stores.
package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
	"github.com/marcus-girolneto/hookrelay/signature"
)

// Store is the full method set exercised by the suite.
type Store interface {
	Ping(ctx context.Context) error
	CreateEndpoint(ctx context.Context, ep webhook.Endpoint) error
	GetEndpoint(ctx context.Context, id string) (webhook.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]webhook.Endpoint, error)
	EnableEndpoint(ctx context.Context, id string) (webhook.Endpoint, error)
	PublishEvent(ctx context.Context, ev webhook.Event) (webhook.Event, []webhook.Delivery, bool, error)
	GetEvent(ctx context.Context, id string) (webhook.Event, error)
	ListDeliveries(ctx context.Context, eventID string) ([]webhook.DeliveryDetail, error)
	ReplayDelivery(ctx context.Context, id string, now time.Time) (webhook.Delivery, error)
	ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]webhook.Job, error)
	Complete(ctx context.Context, r webhook.Result) error
	QueueStats(ctx context.Context, now time.Time) (webhook.QueueStats, error)
}

// Run executes the suite. newStore must return a fresh, empty store each call.
func Run(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(t *testing.T, s Store)
	}{
		{"EndpointCRUD", testEndpointCRUD},
		{"PublishFansOutToSubscribedEndpoints", testPublishFanOut},
		{"PublishIsIdempotent", testPublishIdempotent},
		{"ClaimLeasesDueDeliveries", testClaimLeases},
		{"CompleteSuccess", testCompleteSuccess},
		{"CompleteRetryReschedules", testCompleteRetry},
		{"CompleteWithStaleLease", testStaleLease},
		{"DisableEndpointDeadLettersBacklog", testDisableEndpoint},
		{"ReplayDeadDelivery", testReplay},
		{"QueueStats", testQueueStats},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

var base = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

func newEndpoint(t *testing.T, s Store, types ...string) webhook.Endpoint {
	t.Helper()
	ep := webhook.Endpoint{
		ID:         id.New("ep"),
		URL:        "https://example.com/hooks",
		Secret:     signature.NewSecret(),
		EventTypes: types,
		CreatedAt:  base,
	}
	if err := s.CreateEndpoint(t.Context(), ep); err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	return ep
}

func newEvent(typ, key, payload string) webhook.Event {
	h := sha256.Sum256([]byte(typ + "\x00" + payload))
	return webhook.Event{
		ID:             id.New("evt"),
		Type:           typ,
		Payload:        json.RawMessage(payload),
		IdempotencyKey: key,
		RequestHash:    h[:],
		CreatedAt:      base,
	}
}

func publish(t *testing.T, s Store, ev webhook.Event) []webhook.Delivery {
	t.Helper()
	_, ds, created, err := s.PublishEvent(t.Context(), ev)
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if !created {
		t.Fatal("PublishEvent: expected created=true")
	}
	return ds
}

func claim(t *testing.T, s Store, now time.Time, limit int) []webhook.Job {
	t.Helper()
	jobs, err := s.ClaimDue(t.Context(), now, limit, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	return jobs
}

func deliveryByID(t *testing.T, s Store, eventID, deliveryID string) webhook.DeliveryDetail {
	t.Helper()
	ds, err := s.ListDeliveries(t.Context(), eventID)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	for _, d := range ds {
		if d.ID == deliveryID {
			return d
		}
	}
	t.Fatalf("delivery %s not found", deliveryID)
	return webhook.DeliveryDetail{}
}

func testEndpointCRUD(t *testing.T, s Store) {
	ctx := t.Context()
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	a := newEndpoint(t, s, "invoice.paid", "invoice.voided")
	b := newEndpoint(t, s)

	got, err := s.GetEndpoint(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if got.URL != a.URL || got.Secret != a.Secret || len(got.EventTypes) != 2 || !got.CreatedAt.Equal(a.CreatedAt) || got.Disabled() {
		t.Fatalf("GetEndpoint = %+v, want %+v", got, a)
	}
	if _, err := s.GetEndpoint(ctx, "ep_missing"); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("GetEndpoint(missing) err = %v, want ErrNotFound", err)
	}
	list, err := s.ListEndpoints(ctx)
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	first, second := min(a.ID, b.ID), max(a.ID, b.ID)
	if len(list) != 2 || list[0].ID != first || list[1].ID != second {
		t.Fatalf("ListEndpoints = %+v", list)
	}
	if got, err := s.EnableEndpoint(ctx, a.ID); err != nil || got.Disabled() {
		t.Fatalf("EnableEndpoint = %+v, %v", got, err)
	}
	if _, err := s.EnableEndpoint(ctx, "ep_missing"); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("EnableEndpoint(missing) err = %v, want ErrNotFound", err)
	}
}

func testPublishFanOut(t *testing.T, s Store) {
	ctx := t.Context()
	all := newEndpoint(t, s)
	paid := newEndpoint(t, s, "invoice.paid")
	newEndpoint(t, s, "customer.created")

	ev := newEvent("invoice.paid", "k1", `{"amount": 42, "currency": "EUR"}`)
	ds := publish(t, s, ev)
	if len(ds) != 2 {
		t.Fatalf("got %d deliveries, want 2", len(ds))
	}
	targets := map[string]bool{}
	for _, d := range ds {
		targets[d.EndpointID] = true
		if d.Status != webhook.StatusPending || d.AttemptCount != 0 || !d.NextAttemptAt.Equal(base) {
			t.Fatalf("unexpected new delivery %+v", d)
		}
	}
	if !targets[all.ID] || !targets[paid.ID] {
		t.Fatalf("deliveries target %v, want %s and %s", targets, all.ID, paid.ID)
	}

	got, err := s.GetEvent(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if string(got.Payload) != string(ev.Payload) {
		t.Fatalf("payload not preserved byte-for-byte: %s", got.Payload)
	}
	if got.Type != ev.Type || got.IdempotencyKey != "k1" || !got.CreatedAt.Equal(base) {
		t.Fatalf("GetEvent = %+v", got)
	}
	if _, err := s.GetEvent(ctx, "evt_missing"); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("GetEvent(missing) err = %v", err)
	}
	if _, err := s.ListDeliveries(ctx, "evt_missing"); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("ListDeliveries(missing) err = %v", err)
	}
}

func testPublishIdempotent(t *testing.T, s Store) {
	ctx := t.Context()
	newEndpoint(t, s)
	first := newEvent("invoice.paid", "same-key", `{"n":1}`)
	ds := publish(t, s, first)

	retry := newEvent("invoice.paid", "same-key", `{"n":1}`)
	got, ds2, created, err := s.PublishEvent(ctx, retry)
	if err != nil {
		t.Fatalf("PublishEvent(retry): %v", err)
	}
	if created || got.ID != first.ID {
		t.Fatalf("retry created=%v id=%s, want original %s", created, got.ID, first.ID)
	}
	if len(ds2) != len(ds) || ds2[0].ID != ds[0].ID {
		t.Fatalf("retry deliveries %+v, want %+v", ds2, ds)
	}

	conflict := newEvent("invoice.paid", "same-key", `{"n":2}`)
	if _, _, _, err := s.PublishEvent(ctx, conflict); !errors.Is(err, webhook.ErrIdempotencyMismatch) {
		t.Fatalf("PublishEvent(conflict) err = %v, want ErrIdempotencyMismatch", err)
	}
	if _, err := s.GetEvent(ctx, conflict.ID); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatal("conflicting publish must not store an event")
	}
}

func testClaimLeases(t *testing.T, s Store) {
	ep := newEndpoint(t, s)
	for i := range 3 {
		ev := newEvent("t", id.New("k"), `{}`)
		ev.CreatedAt = base.Add(time.Duration(i) * time.Second)
		publish(t, s, ev)
	}

	if jobs := claim(t, s, base.Add(-time.Second), 10); len(jobs) != 0 {
		t.Fatalf("claimed %d deliveries before they were due", len(jobs))
	}
	now := base.Add(5 * time.Second)
	jobs := claim(t, s, now, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d, want 2", len(jobs))
	}
	if !jobs[0].Event.CreatedAt.Before(jobs[1].Event.CreatedAt) {
		t.Fatal("claims are not ordered by due time")
	}
	j := jobs[0]
	if j.LeaseToken == "" || j.Endpoint.ID != ep.ID || j.Endpoint.Secret != ep.Secret || j.Endpoint.URL != ep.URL {
		t.Fatalf("job missing endpoint data: %+v", j)
	}
	if j.Event.Type != "t" || string(j.Event.Payload) != `{}` {
		t.Fatalf("job missing event data: %+v", j.Event)
	}

	rest := claim(t, s, now, 10)
	if len(rest) != 1 {
		t.Fatalf("second claim got %d, want the 1 unleased delivery", len(rest))
	}
	if again := claim(t, s, now.Add(30*time.Second), 10); len(again) != 0 {
		t.Fatalf("leased deliveries were claimed again before lease expiry: %d", len(again))
	}
	expired := claim(t, s, now.Add(time.Minute+time.Second), 10)
	if len(expired) != 3 {
		t.Fatalf("after lease expiry claimed %d, want 3", len(expired))
	}
}

func testCompleteSuccess(t *testing.T, s Store) {
	newEndpoint(t, s)
	ev := newEvent("t", "k", `{}`)
	publish(t, s, ev)
	j := claim(t, s, base, 1)[0]

	finished := base.Add(150 * time.Millisecond)
	err := s.Complete(t.Context(), webhook.Result{
		DeliveryID: j.Delivery.ID,
		LeaseToken: j.LeaseToken,
		Outcome:    webhook.OutcomeSucceeded,
		FinishedAt: finished,
		Attempt: webhook.Attempt{
			DeliveryID:   j.Delivery.ID,
			Number:       1,
			StartedAt:    base,
			Duration:     150 * time.Millisecond,
			StatusCode:   204,
			ResponseBody: "ok",
		},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	d := deliveryByID(t, s, ev.ID, j.Delivery.ID)
	if d.Status != webhook.StatusSucceeded || d.AttemptCount != 1 || d.LastError != "" {
		t.Fatalf("delivery after success = %+v", d.Delivery)
	}
	if len(d.Attempts) != 1 {
		t.Fatalf("got %d attempts, want 1", len(d.Attempts))
	}
	a := d.Attempts[0]
	if a.Number != 1 || a.StatusCode != 204 || a.Duration != 150*time.Millisecond || a.ResponseBody != "ok" || !a.StartedAt.Equal(base) {
		t.Fatalf("attempt = %+v", a)
	}
	if jobs := claim(t, s, base.Add(time.Hour), 10); len(jobs) != 0 {
		t.Fatal("succeeded delivery was claimed again")
	}
}

func testCompleteRetry(t *testing.T, s Store) {
	newEndpoint(t, s)
	ev := newEvent("t", "k", `{}`)
	publish(t, s, ev)
	j := claim(t, s, base, 1)[0]
	next := base.Add(10 * time.Second)

	err := s.Complete(t.Context(), webhook.Result{
		DeliveryID:    j.Delivery.ID,
		LeaseToken:    j.LeaseToken,
		Outcome:       webhook.OutcomeRetry,
		NextAttemptAt: next,
		FinishedAt:    base,
		Attempt:       webhook.Attempt{DeliveryID: j.Delivery.ID, Number: 1, StartedAt: base, StatusCode: 500, Error: "unexpected status 500"},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	d := deliveryByID(t, s, ev.ID, j.Delivery.ID)
	if d.Status != webhook.StatusPending || d.AttemptCount != 1 || !d.NextAttemptAt.Equal(next) || d.LastError != "unexpected status 500" {
		t.Fatalf("delivery after retry = %+v", d.Delivery)
	}
	if jobs := claim(t, s, next.Add(-time.Millisecond), 10); len(jobs) != 0 {
		t.Fatal("delivery claimed before its retry time")
	}
	jobs := claim(t, s, next, 10)
	if len(jobs) != 1 || jobs[0].Delivery.AttemptCount != 1 {
		t.Fatalf("retry claim = %+v", jobs)
	}
}

func testStaleLease(t *testing.T, s Store) {
	newEndpoint(t, s)
	ev := newEvent("t", "k", `{}`)
	publish(t, s, ev)
	stale := claim(t, s, base, 1)[0]
	fresh := claim(t, s, base.Add(2*time.Minute), 1)[0]
	if stale.LeaseToken == fresh.LeaseToken {
		t.Fatal("reclaim did not rotate the lease token")
	}

	err := s.Complete(t.Context(), webhook.Result{
		DeliveryID: stale.Delivery.ID,
		LeaseToken: stale.LeaseToken,
		Outcome:    webhook.OutcomeDead,
		FinishedAt: base,
		Attempt:    webhook.Attempt{DeliveryID: stale.Delivery.ID, Number: 1, StartedAt: base, Error: "timeout"},
	})
	if !errors.Is(err, webhook.ErrLeaseLost) {
		t.Fatalf("Complete(stale) err = %v, want ErrLeaseLost", err)
	}
	d := deliveryByID(t, s, ev.ID, stale.Delivery.ID)
	if d.Status != webhook.StatusPending {
		t.Fatalf("stale completion changed status to %s", d.Status)
	}
	if len(d.Attempts) != 1 {
		t.Fatalf("attempt from stale lease not recorded: %d attempts", len(d.Attempts))
	}
	if err := s.Complete(t.Context(), webhook.Result{
		DeliveryID: fresh.Delivery.ID,
		LeaseToken: fresh.LeaseToken,
		Outcome:    webhook.OutcomeSucceeded,
		FinishedAt: base,
		Attempt:    webhook.Attempt{DeliveryID: fresh.Delivery.ID, Number: 1, StartedAt: base, StatusCode: 200},
	}); err != nil {
		t.Fatalf("Complete(fresh): %v", err)
	}
	if err := s.Complete(t.Context(), webhook.Result{DeliveryID: "dlv_missing"}); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("Complete(missing) err = %v, want ErrNotFound", err)
	}
}

func testDisableEndpoint(t *testing.T, s Store) {
	ctx := t.Context()
	gone := newEndpoint(t, s)
	other := newEndpoint(t, s)
	ev1 := newEvent("t", "k1", `{}`)
	ev2 := newEvent("t", "k2", `{}`)
	publish(t, s, ev1)
	publish(t, s, ev2)

	var target webhook.Job
	for _, j := range claim(t, s, base, 10) {
		if j.Endpoint.ID == gone.ID && j.Event.ID == ev1.ID {
			target = j
		}
	}
	err := s.Complete(ctx, webhook.Result{
		DeliveryID:      target.Delivery.ID,
		LeaseToken:      target.LeaseToken,
		Outcome:         webhook.OutcomeDead,
		DisableEndpoint: true,
		FinishedAt:      base,
		Attempt:         webhook.Attempt{DeliveryID: target.Delivery.ID, Number: 1, StartedAt: base, StatusCode: 410, Error: "endpoint returned 410 Gone"},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	ep, err := s.GetEndpoint(ctx, gone.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ep.Disabled() || ep.DisabledReason != "endpoint returned 410 Gone" {
		t.Fatalf("endpoint not disabled: %+v", ep)
	}
	ds, err := s.ListDeliveries(ctx, ev2.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		want := webhook.StatusPending
		if d.EndpointID == gone.ID {
			want = webhook.StatusDead
		}
		if d.Status != want {
			t.Fatalf("delivery to %s has status %s, want %s", d.EndpointID, d.Status, want)
		}
	}

	ev3 := newEvent("t", "k3", `{}`)
	ds3 := publish(t, s, ev3)
	if len(ds3) != 1 || ds3[0].EndpointID != other.ID {
		t.Fatalf("disabled endpoint still receives new events: %+v", ds3)
	}

	if _, err := s.ReplayDelivery(ctx, target.Delivery.ID, base); !errors.Is(err, webhook.ErrEndpointDisabled) {
		t.Fatalf("Replay to disabled endpoint err = %v, want ErrEndpointDisabled", err)
	}
	if _, err := s.EnableEndpoint(ctx, gone.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReplayDelivery(ctx, target.Delivery.ID, base); err != nil {
		t.Fatalf("Replay after re-enable: %v", err)
	}
}

func testReplay(t *testing.T, s Store) {
	ctx := t.Context()
	newEndpoint(t, s)
	ev := newEvent("t", "k", `{}`)
	publish(t, s, ev)
	j := claim(t, s, base, 1)[0]

	if _, err := s.ReplayDelivery(ctx, j.Delivery.ID, base); !errors.Is(err, webhook.ErrNotDeadLettered) {
		t.Fatalf("Replay(pending) err = %v, want ErrNotDeadLettered", err)
	}
	if _, err := s.ReplayDelivery(ctx, "dlv_missing", base); !errors.Is(err, webhook.ErrNotFound) {
		t.Fatalf("Replay(missing) err = %v, want ErrNotFound", err)
	}
	if err := s.Complete(ctx, webhook.Result{
		DeliveryID: j.Delivery.ID,
		LeaseToken: j.LeaseToken,
		Outcome:    webhook.OutcomeDead,
		FinishedAt: base,
		Attempt:    webhook.Attempt{DeliveryID: j.Delivery.ID, Number: 5, StartedAt: base, StatusCode: 500, Error: "unexpected status 500"},
	}); err != nil {
		t.Fatal(err)
	}

	later := base.Add(time.Hour)
	d, err := s.ReplayDelivery(ctx, j.Delivery.ID, later)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if d.Status != webhook.StatusPending || d.AttemptCount != 0 || !d.NextAttemptAt.Equal(later) || d.LastError != "" {
		t.Fatalf("replayed delivery = %+v", d)
	}
	jobs := claim(t, s, later, 10)
	if len(jobs) != 1 || jobs[0].Delivery.ID != j.Delivery.ID {
		t.Fatalf("replayed delivery not claimable: %+v", jobs)
	}
	if got := deliveryByID(t, s, ev.ID, j.Delivery.ID); len(got.Attempts) != 1 {
		t.Fatalf("replay must keep attempt history, got %d attempts", len(got.Attempts))
	}
}

func testQueueStats(t *testing.T, s Store) {
	newEndpoint(t, s)
	newEndpoint(t, s)
	for i := range 3 {
		ev := newEvent("t", id.New("k"), `{}`)
		ev.CreatedAt = base.Add(time.Duration(i) * time.Minute)
		publish(t, s, ev)
	}
	st, err := s.QueueStats(t.Context(), base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if st.Pending != 6 || st.Due != 4 {
		t.Fatalf("QueueStats = %+v, want pending=6 due=4", st)
	}
}
