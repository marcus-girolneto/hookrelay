CREATE TABLE endpoints (
    id              text        PRIMARY KEY,
    url             text        NOT NULL,
    secret          text        NOT NULL,
    description     text        NOT NULL DEFAULT '',
    event_types     text[]      NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL,
    disabled_at     timestamptz,
    disabled_reason text        NOT NULL DEFAULT ''
);

-- payload is json rather than jsonb so the stored bytes, which are what gets
-- signed and delivered, are never re-serialised (jsonb reorders keys).
CREATE TABLE events (
    id              text        PRIMARY KEY,
    type            text        NOT NULL,
    payload         json        NOT NULL,
    idempotency_key text        NOT NULL UNIQUE,
    request_hash    bytea       NOT NULL,
    created_at      timestamptz NOT NULL
);

CREATE TABLE deliveries (
    id              text        PRIMARY KEY,
    event_id        text        NOT NULL REFERENCES events (id),
    endpoint_id     text        NOT NULL REFERENCES endpoints (id),
    status          text        NOT NULL CHECK (status IN ('pending', 'succeeded', 'dead')),
    attempt_count   integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    lease_token     text,
    last_error      text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    UNIQUE (event_id, endpoint_id)
);

-- The claim query scans only pending rows in due order; a partial index keeps
-- it small no matter how much delivery history accumulates.
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX deliveries_endpoint_pending_idx ON deliveries (endpoint_id) WHERE status = 'pending';

CREATE TABLE attempts (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id    text        NOT NULL REFERENCES deliveries (id),
    attempt_number integer     NOT NULL,
    started_at     timestamptz NOT NULL,
    duration_us    bigint      NOT NULL,
    status_code    integer     NOT NULL DEFAULT 0,
    error          text        NOT NULL DEFAULT '',
    response_body  text        NOT NULL DEFAULT ''
);

CREATE INDEX attempts_delivery_idx ON attempts (delivery_id, id);
