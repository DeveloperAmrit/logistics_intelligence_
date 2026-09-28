-- Operational core. Relationships, status transitions and updates need
-- transactional consistency, which is why this is relational and not a topic.

CREATE TABLE IF NOT EXISTS carrier (
    carrier_id   TEXT PRIMARY KEY,
    carrier_code TEXT UNIQUE NOT NULL,
    carrier_name TEXT NOT NULL,
    carrier_type TEXT,
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse (
    warehouse_id   TEXT PRIMARY KEY,
    warehouse_code TEXT UNIQUE NOT NULL,
    warehouse_name TEXT NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    capacity       INTEGER,
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle (
    vehicle_id            TEXT PRIMARY KEY,
    carrier_id            TEXT NOT NULL REFERENCES carrier(carrier_id),
    registration_no       TEXT UNIQUE NOT NULL,
    vehicle_type          TEXT,
    refrigeration_capable BOOLEAN NOT NULL DEFAULT FALSE,
    active                BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS route (
    route_id                 TEXT PRIMARY KEY,
    route_name               TEXT,
    origin_warehouse_id      TEXT REFERENCES warehouse(warehouse_id),
    destination_warehouse_id TEXT REFERENCES warehouse(warehouse_id),
    geometry                 JSONB,
    planned_distance_km      DOUBLE PRECISION,
    expected_duration_seconds INTEGER,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensor (
    sensor_id     TEXT PRIMARY KEY,
    sensor_type   TEXT NOT NULL,
    model         TEXT,
    serial_number TEXT UNIQUE,
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A device can be moved between vehicles. Validity is temporal so a historical
-- reading is always interpreted with the assignment that was valid at its
-- event time, not the assignment that happens to be current now.
CREATE TABLE IF NOT EXISTS sensor_assignment (
    assignment_id BIGSERIAL PRIMARY KEY,
    sensor_id     TEXT NOT NULL REFERENCES sensor(sensor_id),
    vehicle_id    TEXT REFERENCES vehicle(vehicle_id),
    shipment_id   TEXT,
    valid_from    TIMESTAMPTZ NOT NULL,
    valid_to      TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX IF NOT EXISTS idx_sensor_assignment_lookup
    ON sensor_assignment (sensor_id, valid_from DESC);

CREATE TABLE IF NOT EXISTS "order" (
    order_id   TEXT PRIMARY KEY,
    placed_at  TIMESTAMPTZ NOT NULL,
    customer   TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shipment (
    shipment_id              TEXT PRIMARY KEY,
    order_id                 TEXT REFERENCES "order"(order_id),
    carrier_id               TEXT REFERENCES carrier(carrier_id),
    vehicle_id               TEXT REFERENCES vehicle(vehicle_id),
    route_id                 TEXT REFERENCES route(route_id),
    status                   TEXT NOT NULL DEFAULT 'CREATED',
    priority                 TEXT,
    temperature_class        TEXT NOT NULL DEFAULT 'AMBIENT',
    promised_delivery_time   TIMESTAMPTZ,
    actual_delivery_time     TIMESTAMPTZ,
    origin_warehouse_id      TEXT REFERENCES warehouse(warehouse_id),
    destination_warehouse_id TEXT REFERENCES warehouse(warehouse_id),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_shipment_status ON shipment (status);

-- History is appended, never rewritten. The current status on shipment is a
-- serving shortcut, not the record of what happened.
CREATE TABLE IF NOT EXISTS shipment_status_history (
    status_event_id BIGSERIAL PRIMARY KEY,
    shipment_id     TEXT NOT NULL REFERENCES shipment(shipment_id),
    old_status      TEXT,
    new_status      TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    event_id        TEXT UNIQUE NOT NULL     -- idempotency: one row per event
);
CREATE INDEX IF NOT EXISTS idx_status_history_shipment
    ON shipment_status_history (shipment_id, event_time);
