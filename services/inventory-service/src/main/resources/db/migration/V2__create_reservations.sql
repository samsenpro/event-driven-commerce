-- Una reserva por pedido. El índice único sobre order_id garantiza que un pedido nunca
-- reserve stock dos veces, aunque el evento llegue duplicado.
CREATE TABLE reservations (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id    BIGINT                   NOT NULL,
    user_id     BIGINT                   NOT NULL,
    status      VARCHAR(20)              NOT NULL,
    reason      VARCHAR(200),
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT ck_reservations_status CHECK (status IN ('RESERVED', 'COMMITTED', 'RELEASED', 'REJECTED', 'VOIDED'))
);

CREATE UNIQUE INDEX ux_reservations_order_id ON reservations (order_id);

CREATE TABLE reservation_lines (
    reservation_id  BIGINT  NOT NULL REFERENCES reservations (id) ON DELETE CASCADE,
    product_id      BIGINT  NOT NULL,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, product_id)
);
