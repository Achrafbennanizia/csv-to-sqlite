CREATE TABLE IF NOT EXISTS customers (
    id           INTEGER PRIMARY KEY,
    name         TEXT NOT NULL,
    email        TEXT UNIQUE,
    signup_date  TEXT,
    spend        REAL DEFAULT 0
);
