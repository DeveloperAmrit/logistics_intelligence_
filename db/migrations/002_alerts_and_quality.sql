-- Alerts and the data-quality record.

CREATE TABLE IF NOT EXISTS alert (
    alert_id         TEXT PRIMARY KEY,
    alert_type       TEXT NOT NULL,
    severity         TEXT NOT NULL,
    shipment_id      TEXT REFERENCES shipment(shipment_id),
    vehicle_id       TEXT REFERENCES vehicle(vehicle_id),
    rule_id          TEXT NOT NULL,
    trigger_event_id TEXT NOT NULL,
    start_event_time TIMESTAMPTZ NOT NULL,
    end_event_time   TIMESTAMPTZ,
    explanation      TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'OPEN',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_alert_open ON alert (status, created_at DESC);

-- Quarantine, mirrored from the DLQ topic so it can be queried with SQL.
CREATE TABLE IF NOT EXISTS quarantine_record (
    quarantine_id BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL,
    event_type    TEXT,
    reason_codes  TEXT[] NOT NULL,
    raw_payload   JSONB NOT NULL,
    arrival_time  TIMESTAMPTZ NOT NULL,
    source        TEXT,
    replayed_at   TIMESTAMPTZ,
    UNIQUE (event_id, arrival_time)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON quarantine_record USING GIN (reason_codes);

-- Late events kept for the same reason: a number we can defend, not a guess.
CREATE TABLE IF NOT EXISTS late_event (
    late_event_id    BIGSERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,
    lateness_seconds DOUBLE PRECISION NOT NULL,
    disposition      TEXT NOT NULL
);
