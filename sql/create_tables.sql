CREATE TABLE IF NOT EXISTS advice_quotes (
    id SERIAL PRIMARY KEY,
    advice_id INTEGER UNIQUE NOT NULL,
    advice_text TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quotes (
    id SERIAL PRIMARY KEY,
    quote_key BIGINT UNIQUE NOT NULL,
    quote_text TEXT NOT NULL,
    author TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dog_images (
    id SERIAL PRIMARY KEY,
    breed TEXT,
    image_url TEXT UNIQUE NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    triggered_by TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL,
    advice_inserted INTEGER DEFAULT 0,
    advice_skipped INTEGER DEFAULT 0,
    quotes_inserted INTEGER DEFAULT 0,
    quotes_skipped INTEGER DEFAULT 0,
    dogs_inserted INTEGER DEFAULT 0,
    dogs_skipped INTEGER DEFAULT 0,
    duration_seconds NUMERIC(8, 2)
);