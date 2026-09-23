CREATE TABLE products (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          VARCHAR(64)              NOT NULL,
    name         VARCHAR(150)             NOT NULL,
    description  VARCHAR(2000),
    price        NUMERIC(12, 2)           NOT NULL CHECK (price > 0),
    active       BOOLEAN                  NOT NULL DEFAULT TRUE,
    version      BIGINT                   NOT NULL DEFAULT 0,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE UNIQUE INDEX ux_products_sku ON products (sku);

-- Stock por producto. Todas las escrituras son UPDATE condicionales y atómicos;
-- los CHECK son la última línea de defensa contra stock negativo.
CREATE TABLE stock_items (
    product_id  BIGINT PRIMARY KEY REFERENCES products (id),
    available   INTEGER                  NOT NULL CHECK (available >= 0),
    reserved    INTEGER                  NOT NULL CHECK (reserved >= 0),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL
);
