-- Auth + Favorites

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth_tokens (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash CHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_tokens_user_id ON auth_tokens(user_id);

CREATE TABLE IF NOT EXISTS favorite_products (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    external_id VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    title VARCHAR(512) NOT NULL,
    price BIGINT NOT NULL,
    thumbnail_url VARCHAR(1024) DEFAULT '',
    merchant_name VARCHAR(255) NOT NULL,
    merchant_logo_url VARCHAR(1024) DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, external_id, source)
);

CREATE INDEX IF NOT EXISTS idx_favorite_products_user_id ON favorite_products(user_id);
