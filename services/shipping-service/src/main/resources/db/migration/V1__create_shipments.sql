-- Un envío por pedido (índice único): un ORDER_CONFIRMED duplicado nunca crea dos envíos.
CREATE TABLE shipments (
    id               UUID PRIMARY KEY,
    order_id         BIGINT                   NOT NULL,
    user_id          BIGINT                   NOT NULL,
    status           VARCHAR(20)              NOT NULL,
    tracking_number  VARCHAR(40),
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT ck_shipments_status CHECK (status IN ('CREATED', 'CANCELLED', 'VOIDED'))
);

CREATE UNIQUE INDEX ux_shipments_order_id ON shipments (order_id);
