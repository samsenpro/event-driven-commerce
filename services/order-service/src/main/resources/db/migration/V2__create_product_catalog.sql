-- Réplica local del catálogo, alimentada por el topic products.changed (event-carried state transfer).
-- Permite validar y valorar pedidos sin llamar por REST al inventory-service.
CREATE TABLE product_catalog (
    product_id  BIGINT PRIMARY KEY,
    sku         VARCHAR(64)              NOT NULL,
    name        VARCHAR(150)             NOT NULL,
    price       NUMERIC(12, 2)           NOT NULL,
    active      BOOLEAN                  NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL
);
