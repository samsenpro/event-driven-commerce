CREATE TABLE outbox_events (
    id               UUID PRIMARY KEY,
    position         BIGINT GENERATED ALWAYS AS IDENTITY,
    aggregate_type   VARCHAR(50)              NOT NULL,
    aggregate_id     VARCHAR(100)             NOT NULL,
    event_type       VARCHAR(60)              NOT NULL,
    topic            VARCHAR(100)             NOT NULL,
    payload          TEXT                     NOT NULL,
    correlation_id   VARCHAR(64)              NOT NULL,
    status           VARCHAR(20)              NOT NULL,
    retry_count      INTEGER                  NOT NULL DEFAULT 0,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    next_attempt_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at     TIMESTAMP WITH TIME ZONE,
    last_error       VARCHAR(500),
    CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

CREATE UNIQUE INDEX ux_outbox_position ON outbox_events (position);
CREATE INDEX idx_outbox_pending ON outbox_events (status, next_attempt_at, position);
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_type, aggregate_id, position);
