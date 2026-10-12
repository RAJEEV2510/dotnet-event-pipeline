CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    type VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NULL,
    retry_count INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_messages(id ASC)
    WHERE processed_at IS NULL AND retry_count < 5;

CREATE INDEX IF NOT EXISTS idx_outbox_cleanup ON outbox_messages(processed_at)
    WHERE processed_at IS NOT NULL;
