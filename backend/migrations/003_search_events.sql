-- Search analytics (for "often searched" chips)

CREATE TABLE IF NOT EXISTS search_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NULL REFERENCES users(id) ON DELETE SET NULL,
    query VARCHAR(255) NOT NULL,
    normalized_query VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_events_created_at ON search_events(created_at);
CREATE INDEX IF NOT EXISTS idx_search_events_normalized_query ON search_events(normalized_query);

