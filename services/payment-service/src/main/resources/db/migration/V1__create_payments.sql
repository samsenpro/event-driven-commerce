-- Un pago por pedido: el índice único impide cobrar dos veces el mismo pedido.
CREATE TABLE payments (
    id          UUID PRIMARY KEY,
    order_id    BIGINT                   NOT NULL,
    user_id     BIGINT                   NOT NULL,
    amount      NUMERIC(14, 2),
    status      VARCHAR(20)              NOT NULL,
    reason      VARCHAR(200),
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT ck_payments_status CHECK (status IN ('APPROVED', 'REJECTED', 'REFUNDED', 'VOIDED'))
);

CREATE UNIQUE INDEX ux_payments_order_id ON payments (order_id);
