// Package postgres implements the hookrelay store on PostgreSQL.
package postgres

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcus-girolneto/hookrelay/internal/id"
	"github.com/marcus-girolneto/hookrelay/internal/webhook"
)

// Store is a PostgreSQL-backed store safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store using pool. Run Migrate first.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const endpointColumns = `id, url, secret, description, event_types, created_at, disabled_at, disabled_reason`

func scanEndpoint(row pgx.Row) (webhook.Endpoint, error) {
	var ep webhook.Endpoint
	err := row.Scan(&ep.ID, &ep.URL, &ep.Secret, &ep.Description, &ep.EventTypes, &ep.CreatedAt, &ep.DisabledAt, &ep.DisabledReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return webhook.Endpoint{}, webhook.ErrNotFound
	}
	return ep, err
}

// CreateEndpoint stores ep.
func (s *Store) CreateEndpoint(ctx context.Context, ep webhook.Endpoint) error {
	types := ep.EventTypes
	if types == nil {
		types = []string{}
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO endpoints (id, url, secret, description, event_types, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		ep.ID, ep.URL, ep.Secret, ep.Description, types, ep.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert endpoint: %w", err)
	}
	return nil
}

// GetEndpoint returns the endpoint with the given id.
func (s *Store) GetEndpoint(ctx context.Context, id string) (webhook.Endpoint, error) {
	ep, err := scanEndpoint(s.pool.QueryRow(ctx, `SELECT `+endpointColumns+` FROM endpoints WHERE id = $1`, id))
	if err != nil {
		return webhook.Endpoint{}, fmt.Errorf("get endpoint: %w", err)
	}
	return ep, nil
}

// ListEndpoints returns all endpoints ordered by id.
func (s *Store) ListEndpoints(ctx context.Context) ([]webhook.Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+endpointColumns+` FROM endpoints ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (webhook.Endpoint, error) { return scanEndpoint(r) })
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	return out, nil
}

// EnableEndpoint clears a previous disable.
func (s *Store) EnableEndpoint(ctx context.Context, id string) (webhook.Endpoint, error) {
	ep, err := scanEndpoint(s.pool.QueryRow(ctx,
		`UPDATE endpoints SET disabled_at = NULL, disabled_reason = '' WHERE id = $1 RETURNING `+endpointColumns, id))
	if err != nil {
		return webhook.Endpoint{}, fmt.Errorf("enable endpoint: %w", err)
	}
	return ep, nil
}

const eventColumns = `id, type, payload, idempotency_key, request_hash, created_at`

func scanEvent(row pgx.Row) (webhook.Event, error) {
	var ev webhook.Event
	var payload []byte
	err := row.Scan(&ev.ID, &ev.Type, &payload, &ev.IdempotencyKey, &ev.RequestHash, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return webhook.Event{}, webhook.ErrNotFound
	}
	ev.Payload = payload
	return ev, err
}

// PublishEvent stores ev and fans it out, deduplicating on idempotency key.
// A reused key with a different body returns webhook.ErrIdempotencyMismatch.
func (s *Store) PublishEvent(ctx context.Context, ev webhook.Event) (webhook.Event, []webhook.Delivery, bool, error) {
	var (
		out     webhook.Event
		ds      []webhook.Delivery
		created bool
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// losers of a same-key race block on the unique index, then read the winner
		tag, err := tx.Exec(ctx,
			`INSERT INTO events (`+eventColumns+`) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING`,
			ev.ID, ev.Type, string(ev.Payload), ev.IdempotencyKey, ev.RequestHash, ev.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		if tag.RowsAffected() == 0 {
			existing, err := scanEvent(tx.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE idempotency_key = $1`, ev.IdempotencyKey))
			if err != nil {
				return fmt.Errorf("load existing event: %w", err)
			}
			if !bytes.Equal(existing.RequestHash, ev.RequestHash) {
				return webhook.ErrIdempotencyMismatch
			}
			out = existing
			ds, err = deliveriesForEvent(ctx, tx, existing.ID)
			return err
		}

		rows, err := tx.Query(ctx,
			`SELECT id FROM endpoints
			 WHERE disabled_at IS NULL AND (cardinality(event_types) = 0 OR $1 = ANY (event_types))`, ev.Type)
		if err != nil {
			return fmt.Errorf("select subscribed endpoints: %w", err)
		}
		endpointIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("select subscribed endpoints: %w", err)
		}
		if len(endpointIDs) > 0 {
			ids := make([]string, len(endpointIDs))
			for i := range ids {
				ids[i] = id.New("dlv")
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO deliveries (id, event_id, endpoint_id, status, next_attempt_at, created_at, updated_at)
				 SELECT d, $3, e, 'pending', $4, $4, $4 FROM unnest($1::text[], $2::text[]) AS t (d, e)`,
				ids, endpointIDs, ev.ID, ev.CreatedAt); err != nil {
				return fmt.Errorf("insert deliveries: %w", err)
			}
		}
		out, created = ev, true
		ds, err = deliveriesForEvent(ctx, tx, ev.ID)
		return err
	})
	if err != nil {
		return webhook.Event{}, nil, false, fmt.Errorf("publish event: %w", err)
	}
	return out, ds, created, nil
}

// GetEvent returns the event with the given id.
func (s *Store) GetEvent(ctx context.Context, id string) (webhook.Event, error) {
	ev, err := scanEvent(s.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id))
	if err != nil {
		return webhook.Event{}, fmt.Errorf("get event: %w", err)
	}
	return ev, nil
}

const deliveryColumns = `id, event_id, endpoint_id, status, attempt_count, next_attempt_at, last_error, created_at, updated_at`

func scanDelivery(row pgx.Row) (webhook.Delivery, error) {
	var d webhook.Delivery
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.NextAttemptAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return webhook.Delivery{}, webhook.ErrNotFound
	}
	return d, err
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func deliveriesForEvent(ctx context.Context, q querier, eventID string) ([]webhook.Delivery, error) {
	rows, err := q.Query(ctx, `SELECT `+deliveryColumns+` FROM deliveries WHERE event_id = $1 ORDER BY id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	ds, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (webhook.Delivery, error) { return scanDelivery(r) })
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	return ds, nil
}

// ListDeliveries returns the deliveries of an event with their attempts.
func (s *Store) ListDeliveries(ctx context.Context, eventID string) ([]webhook.DeliveryDetail, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, eventID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check event: %w", err)
	}
	if !exists {
		return nil, webhook.ErrNotFound
	}
	ds, err := deliveriesForEvent(ctx, s.pool, eventID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT a.delivery_id, a.attempt_number, a.started_at, a.duration_us, a.status_code, a.error, a.response_body
		 FROM attempts a JOIN deliveries d ON d.id = a.delivery_id
		 WHERE d.event_id = $1 ORDER BY a.id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (webhook.Attempt, error) {
		var a webhook.Attempt
		var us int64
		err := r.Scan(&a.DeliveryID, &a.Number, &a.StartedAt, &us, &a.StatusCode, &a.Error, &a.ResponseBody)
		a.Duration = time.Duration(us) * time.Microsecond
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	byDelivery := make(map[string][]webhook.Attempt, len(ds))
	for _, a := range attempts {
		byDelivery[a.DeliveryID] = append(byDelivery[a.DeliveryID], a)
	}
	out := make([]webhook.DeliveryDetail, 0, len(ds))
	for _, d := range ds {
		out = append(out, webhook.DeliveryDetail{Delivery: d, Attempts: byDelivery[d.ID]})
	}
	return out, nil
}

// ReplayDelivery moves a dead delivery back to pending, due at now.
func (s *Store) ReplayDelivery(ctx context.Context, id string, now time.Time) (webhook.Delivery, error) {
	d, err := scanDelivery(s.pool.QueryRow(ctx,
		`UPDATE deliveries d
		 SET status = 'pending', attempt_count = 0, next_attempt_at = $2, last_error = '', lease_token = NULL, updated_at = $2
		 FROM endpoints e
		 WHERE d.id = $1 AND d.status = 'dead' AND e.id = d.endpoint_id AND e.disabled_at IS NULL
		 RETURNING d.id, d.event_id, d.endpoint_id, d.status, d.attempt_count, d.next_attempt_at, d.last_error, d.created_at, d.updated_at`,
		id, now))
	if err == nil {
		return d, nil
	}
	if !errors.Is(err, webhook.ErrNotFound) {
		return webhook.Delivery{}, fmt.Errorf("replay delivery: %w", err)
	}

	var status string
	var disabled bool
	err = s.pool.QueryRow(ctx,
		`SELECT d.status, e.disabled_at IS NOT NULL FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id WHERE d.id = $1`,
		id).Scan(&status, &disabled)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return webhook.Delivery{}, webhook.ErrNotFound
	case err != nil:
		return webhook.Delivery{}, fmt.Errorf("replay delivery: %w", err)
	case status != string(webhook.StatusDead):
		return webhook.Delivery{}, webhook.ErrNotDeadLettered
	default:
		return webhook.Delivery{}, webhook.ErrEndpointDisabled
	}
}

// ClaimDue leases up to limit due deliveries. Safe to run from many workers.
func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]webhook.Job, error) {
	token := id.New("lease")
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT d.id
			FROM deliveries d
			JOIN endpoints e ON e.id = d.endpoint_id
			WHERE d.status = 'pending' AND d.next_attempt_at <= $1 AND e.disabled_at IS NULL
			ORDER BY d.next_attempt_at, d.id
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED
		)
		UPDATE deliveries d
		SET next_attempt_at = $3, lease_token = $4, updated_at = $1
		FROM due, events ev, endpoints e
		WHERE d.id = due.id AND ev.id = d.event_id AND e.id = d.endpoint_id
		RETURNING d.id, d.event_id, d.endpoint_id, d.status, d.attempt_count, d.next_attempt_at, d.last_error, d.created_at, d.updated_at,
		          ev.type, ev.payload, ev.idempotency_key, ev.created_at,
		          e.url, e.secret, e.description, e.event_types, e.created_at`,
		now, limit, now.Add(lease), token)
	if err != nil {
		return nil, fmt.Errorf("claim deliveries: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (webhook.Job, error) {
		var j webhook.Job
		var payload []byte
		d := &j.Delivery
		err := r.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.NextAttemptAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt,
			&j.Event.Type, &payload, &j.Event.IdempotencyKey, &j.Event.CreatedAt,
			&j.Endpoint.URL, &j.Endpoint.Secret, &j.Endpoint.Description, &j.Endpoint.EventTypes, &j.Endpoint.CreatedAt)
		j.Event.ID = d.EventID
		j.Event.Payload = payload
		j.Endpoint.ID = d.EndpointID
		j.LeaseToken = token
		return j, err
	})
	if err != nil {
		return nil, fmt.Errorf("claim deliveries: %w", err)
	}
	// RETURNING doesn't keep the CTE's order
	sortJobs(jobs)
	return jobs, nil
}

// Complete records the attempt and applies r. The attempt is always stored,
// but returns webhook.ErrLeaseLost without a state change if the lease moved on.
func (s *Store) Complete(ctx context.Context, r webhook.Result) error {
	var leaseLost bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var endpointID string
		err := tx.QueryRow(ctx, `SELECT endpoint_id FROM deliveries WHERE id = $1`, r.DeliveryID).Scan(&endpointID)
		if errors.Is(err, pgx.ErrNoRows) {
			return webhook.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("load delivery: %w", err)
		}

		a := r.Attempt
		if _, err := tx.Exec(ctx,
			`INSERT INTO attempts (delivery_id, attempt_number, started_at, duration_us, status_code, error, response_body)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			r.DeliveryID, a.Number, a.StartedAt, a.Duration.Microseconds(), a.StatusCode, a.Error, a.ResponseBody); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}

		status, lastError, next := webhook.StatusPending, a.Error, r.NextAttemptAt
		switch r.Outcome {
		case webhook.OutcomeSucceeded:
			status, lastError = webhook.StatusSucceeded, ""
		case webhook.OutcomeDead:
			status = webhook.StatusDead
		}
		var tag pgconn.CommandTag
		if r.Outcome == webhook.OutcomeRetry {
			tag, err = tx.Exec(ctx,
				`UPDATE deliveries SET status = $3, attempt_count = $4, last_error = $5, next_attempt_at = $6, lease_token = NULL, updated_at = $7
				 WHERE id = $1 AND lease_token = $2 AND status = 'pending'`,
				r.DeliveryID, r.LeaseToken, status, a.Number, lastError, next, r.FinishedAt)
		} else {
			tag, err = tx.Exec(ctx,
				`UPDATE deliveries SET status = $3, attempt_count = $4, last_error = $5, lease_token = NULL, updated_at = $6
				 WHERE id = $1 AND lease_token = $2 AND status = 'pending'`,
				r.DeliveryID, r.LeaseToken, status, a.Number, lastError, r.FinishedAt)
		}
		if err != nil {
			return fmt.Errorf("update delivery: %w", err)
		}
		if tag.RowsAffected() == 0 {
			leaseLost = true
			return nil
		}

		if r.DisableEndpoint {
			if _, err := tx.Exec(ctx,
				`UPDATE endpoints SET disabled_at = $2, disabled_reason = $3 WHERE id = $1 AND disabled_at IS NULL`,
				endpointID, r.FinishedAt, a.Error); err != nil {
				return fmt.Errorf("disable endpoint: %w", err)
			}
			if _, err := tx.Exec(ctx,
				`UPDATE deliveries SET status = 'dead', last_error = $2, lease_token = NULL, updated_at = $3
				 WHERE endpoint_id = $1 AND status = 'pending'`,
				endpointID, webhook.ErrEndpointDisabled.Error(), r.FinishedAt); err != nil {
				return fmt.Errorf("dead-letter endpoint backlog: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("complete delivery: %w", err)
	}
	if leaseLost {
		return webhook.ErrLeaseLost
	}
	return nil
}

// QueueStats reports the pending backlog.
func (s *Store) QueueStats(ctx context.Context, now time.Time) (webhook.QueueStats, error) {
	var st webhook.QueueStats
	err := s.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE next_attempt_at <= $1) FROM deliveries WHERE status = 'pending'`,
		now).Scan(&st.Pending, &st.Due)
	if err != nil {
		return webhook.QueueStats{}, fmt.Errorf("queue stats: %w", err)
	}
	return st, nil
}

func sortJobs(jobs []webhook.Job) {
	slices.SortFunc(jobs, func(a, b webhook.Job) int {
		return cmp.Or(a.Delivery.NextAttemptAt.Compare(b.Delivery.NextAttemptAt), cmp.Compare(a.Delivery.ID, b.Delivery.ID))
	})
}
