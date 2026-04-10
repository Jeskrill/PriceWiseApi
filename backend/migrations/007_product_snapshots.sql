-- Cache product details by source + external_id for product screen

CREATE TABLE IF NOT EXISTS product_snapshots (
    id SERIAL PRIMARY KEY,
    source VARCHAR(255) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    title VARCHAR(512) NOT NULL,
    price BIGINT NOT NULL,
    thumbnail_url VARCHAR(1024) DEFAULT '',
    product_url VARCHAR(2048) DEFAULT '',
    merchant_logo_url VARCHAR(1024) DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_product_snapshots_source_external
    ON product_snapshots (source, external_id);

CREATE INDEX IF NOT EXISTS ix_product_snapshots_source
    ON product_snapshots (source);
