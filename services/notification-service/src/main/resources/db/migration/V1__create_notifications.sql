CREATE TABLE notifications (
    id          UUID PRIMARY KEY,
    event_id    UUID                     NOT NULL,
    order_id    BIGINT                   NOT NULL,
    user_id     BIGINT                   NOT NULL,
    type        VARCHAR(30)              NOT NULL,
    channel     VARCHAR(20)              NOT NULL,
    recipient   VARCHAR(255)             NOT NULL,
    message     VARCHAR(500)             NOT NULL,
    sent_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT ck_notifications_type
        CHECK (type IN ('ORDER_CREATED', 'PAYMENT_APPROVED', 'PAYMENT_REJECTED', 'ORDER_CANCELLED', 'ORDER_SHIPPED'))
);

-- Segunda barrera de idempotencia: nunca dos notificaciones del mismo tipo por el mismo evento
CREATE UNIQUE INDEX ux_notifications_event_type ON notifications (event_id, type);
CREATE INDEX idx_notifications_order_id ON notifications (order_id);

CREATE TABLE processed_events (
    consumer      VARCHAR(100)             NOT NULL,
    event_id      UUID                     NOT NULL,
    processed_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
