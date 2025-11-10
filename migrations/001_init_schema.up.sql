CREATE TABLE IF NOT EXISTS notifications(
    id UUID PRIMARY KEY,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    send_at TIMESTAMPTZ NOT NULL,
    recipient TEXT NOT NULL,
    message TEXT,
    retry_attempts INT,
    next_send_at TIMESTAMPTZ
);