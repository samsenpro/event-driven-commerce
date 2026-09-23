CREATE TABLE orders (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id              BIGINT                   NOT NULL,
    status               VARCHAR(30)              NOT NULL,
    total_amount         NUMERIC(14, 2)           NOT NULL,
    cancellation_reason  VARCHAR(50),
    version              BIGINT                   NOT NULL DEFAULT 0,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT ck_orders_status
        CHECK (status IN ('PENDING', 'INVENTORY_RESERVED', 'CONFIRMED', 'SHIPPED', 'CANCELLED')),
    CONSTRAINT ck_orders_total_positive CHECK (total_amount > 0)
);

CREATE INDEX idx_orders_user_id ON orders (user_id);
CREATE INDEX idx_orders_status ON orders (status);

CREATE TABLE order_items (
    order_id      BIGINT         NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id    BIGINT         NOT NULL,
    product_name  VARCHAR(150)   NOT NULL,
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    PRIMARY KEY (order_id, product_id)
);
