-- Sprint 8: the pipeline now writes the operational store. These additions
-- make every write idempotent and record which run produced the rows.

-- Event time of the status currently shown on shipment, so an out-of-order
-- older event can never overwrite a newer status.
ALTER TABLE shipment ADD COLUMN IF NOT EXISTS status_event_time TIMESTAMPTZ;

-- One row per late event and per assignment, whatever replays happen.
CREATE UNIQUE INDEX IF NOT EXISTS uq_late_event_event_id ON late_event (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_assignment
    ON sensor_assignment (sensor_id, shipment_id, valid_from);

-- Provenance: which scenario and seed wrote this database, and what it counted.
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id      BIGSERIAL PRIMARY KEY,
    scenario    TEXT NOT NULL,
    seed        INTEGER NOT NULL,
    counts      JSONB NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
