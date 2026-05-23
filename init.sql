-- Runs automatically on first PostgreSQL container startup

-- ── Silver layer ──────────────────────────────────────────────────────────────
-- Cleaned, validated, deduplicated departure events

CREATE TABLE IF NOT EXISTS silver_departures (
    id           SERIAL PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trip_id      TEXT        NOT NULL,
    line         TEXT        NOT NULL,
    product      TEXT,                              -- subway, suburban, bus, tram
    direction    TEXT,
    stop_name    TEXT        NOT NULL,
    stop_id      TEXT        NOT NULL,
    planned_when TIMESTAMPTZ,
    actual_when  TIMESTAMPTZ,
    delay_s      FLOAT       NOT NULL,              -- seconds, never NULL in silver
    delay_min    FLOAT       NOT NULL,              -- delay_s / 60, convenience column
    UNIQUE (trip_id, stop_id)                       -- deduplication constraint
);

CREATE INDEX IF NOT EXISTS idx_silver_recorded_at ON silver_departures (recorded_at);
CREATE INDEX IF NOT EXISTS idx_silver_line        ON silver_departures (line);
CREATE INDEX IF NOT EXISTS idx_silver_stop        ON silver_departures (stop_name);

-- ── Gold layer ────────────────────────────────────────────────────────────────
-- Rolling average aggregations, written every N messages by gold_consumer.py

CREATE TABLE IF NOT EXISTS delay_by_line (
    id           SERIAL PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    line         TEXT        NOT NULL,
    avg_delay_s  FLOAT       NOT NULL,
    avg_delay_min FLOAT      NOT NULL,
    sample_count INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS delay_by_line_stop (
    id            SERIAL PRIMARY KEY,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    line          TEXT        NOT NULL,
    stop_name     TEXT        NOT NULL,
    avg_delay_s   FLOAT       NOT NULL,
    avg_delay_min FLOAT       NOT NULL,
    sample_count  INT         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_line_recorded_at      ON delay_by_line (recorded_at);
CREATE INDEX IF NOT EXISTS idx_gold_line_stop_recorded_at ON delay_by_line_stop (recorded_at);