# Implementation Phases

## 1. Phase Strategy

The project should be built incrementally. Each phase must end with a demonstrable working state. No later technology should be allowed to block the correctness of an earlier phase.

The rationale comes directly from the project evolution: the original proposal was intentionally comprehensive, but Lab 3 narrowed the semester target to a Jodhpur-centred prototype with controlled simulation, durable streaming, data integration and alerting. MQTT, CDC, Iceberg and ML are explicitly phased additions. fileciteturn0file2L194-L208

## Phase 0 — Repository and Environment Foundation

### Goal

Create a reproducible development environment before implementing business logic.

### Deliverables

```text
README.md
.env.example
Makefile
compose.yml
pyproject.toml / requirements.txt
config/
scripts/
docker/
src/
tests/
data/
```

### Tasks

- Define Python version.
- Create one development environment.
- Establish Docker Compose profiles for lite/full deployments.
- Create `.env.example`.
- Add structured logging.
- Add health checks.
- Create a standard configuration loader.
- Define coding and naming conventions.
- Add pre-commit/linting if practical.
- Establish a single command for starting/stopping the stack.

### Exit criteria

A new machine can clone the repository, configure the environment, start the base stack and receive a health report.

---

## Phase 1 — Domain Model and PostgreSQL Operational Core

### Goal

Build the operational truth before generating high-volume events.

### Core tables

```text
carrier
vehicle
warehouse
route
geofence
sku
sensor
sensor_assignment
order
shipment
shipment_status_history
```

### Important relationships

```text
carrier 1 ─── N vehicle
carrier 1 ─── N shipment
vehicle 1 ─── N shipment (over time)
route 1 ─── N shipment
shipment 1 ─── N sensor_reading
sensor 1 ─── N sensor_assignment
shipment 1 ─── N warehouse_scan
order 1 ─── N shipment
```

### Design rules

- Use stable business IDs.
- Use UUID/event IDs for individual events.
- Record status transitions rather than overwriting history.
- Store `valid_from` and `valid_to` for assignments.
- Keep transactional data separate from telemetry.

Lab 3 explicitly identifies PostgreSQL as the operational relational core and requires sensor assignment validity to preserve historical meaning. fileciteturn0file2L82-L97

### Exit criteria

You can create a route, vehicle, sensor, shipment and assignment; change status; and query the full shipment history consistently.

---

## Phase 2 — Controlled Simulator

### Goal

Generate realistic, repeatable live data without claiming commercial data access.

### Components

1. SUMO network.
2. TraCI bridge.
3. GPS producer.
4. IoT producer.
5. Warehouse producer.
6. Shipment/order scenario generator.
7. Scenario controller.

### Scenario types

```text
NORMAL_DELIVERY
GPS_DROPOUT
DUPLICATE_EVENT
LATE_EVENT
ROUTE_DEVIATION
WAREHOUSE_DWELL_DELAY
COLD_CHAIN_EXCURSION
```

Lab 3 lists these demonstration scenarios explicitly. fileciteturn0file2L141-L145

### Calibration

Use Olist, DataCo and NYC TLC only to derive plausible distributions for:

- trip duration
- delay
- order arrival patterns

Do not expose them as live logistics feeds. Lab 3 makes this distinction explicit. fileciteturn0file2L19-L36

### Exit criteria

A command such as:

```bash
make simulate SCENARIO=ROUTE_DEVIATION SEED=42
```

produces the same scenario family reproducibly and publishes valid events.

---

## Phase 3 — Kafka Event Layer

### Goal

Introduce a durable, replayable event backbone.

### Tasks

- Start Kafka.
- Create topics.
- Define partitions and retention.
- Define keying strategy.
- Add producer retry/idempotency logic where supported.
- Add event envelope.
- Implement consumer groups.
- Implement DLQ/quarantine topic.
- Add basic producer/consumer metrics.

### Topic plan

```text
raw.gps
raw.sensor
raw.warehouse
raw.shipment
alerts.*
quarantine.dlq
```

### Keys

```text
gps       → vehicle_id
sensor    → sensor_id
shipment  → shipment_id
warehouse → shipment_id
```

These keys are specified by the latest lab architecture. fileciteturn0file2L132-L145

### Exit criteria

- Events are published to Kafka.
- Multiple independent consumers can read the same events.
- Events can be replayed.
- Duplicate event injection is observable.
- Kafka retention/restart does not lose already committed retained events.

---

## Phase 4 — Validation, Deduplication and Quarantine

### Goal

Make bad data a first-class engineering case.

### Validation rules

GPS:

```text
-90 <= latitude <= 90
-180 <= longitude <= 180
speed >= 0
known vehicle
```

Sensor:

```text
-30 <= temperature <= 50
0 <= humidity <= 100
known sensor
valid current assignment
```

Event integrity:

```text
non-null event_id
non-null event_time
unique event_id within retention window
valid shipment state transition
```

Lab 3 defines these checks and requires invalid/unmatched data to be quarantined. fileciteturn0file2L103-L131

### Tasks

- Implement validation library.
- Implement deduplication store/state.
- Define rejection reason codes.
- Persist raw rejected payload.
- Route failures to DLQ/quarantine.
- Build a data-quality dashboard/query.

### Exit criteria

Each deliberately injected fault results in a deterministic and inspectable outcome instead of silent data loss.

---

## Phase 5 — Streaming Alert MVP

### Goal

Implement the first real-time business value.

### First two jobs

#### Cold-chain excursion

Input: sensor readings.

Condition: sustained temperature excursion for a 2–8 °C shipment.

Output:

```text
alert_id
shipment_id
sensor_id
start_event_time
end_event_time
max_temperature
rule_id
severity
created_at
```

#### Route deviation

Input: GPS + planned route.

Condition: vehicle leaves accepted route corridor/geofence.

Output:

```text
alert_id
vehicle_id
shipment_id
route_id
observed_location
expected_location/segment
deviation_distance
severity
created_at
```

### Exit criteria

The simulator can deliberately trigger both alerts and the alert is visible without manually querying raw data.

---

## Phase 6 — Event-Time Correctness

### Goal

Move from a simple real-time consumer to true event-time processing.

### Concepts to implement

- event time
- processing time comparison
- watermarks
- bounded out-of-orderness
- allowed lateness
- late-event side output
- event-time windows

Starting values:

```text
watermark delay = 60 seconds
allowed lateness = 5 minutes
checkpoint interval = 30 seconds
```

These are starting configurations from Lab 1 and Lab 3, not immutable production constants. fileciteturn0file0L90-L93 fileciteturn0file2L146-L148

### Test cases

1. In-order events.
2. 20-second late event.
3. 2-minute late event.
4. 6-minute late event.
5. Duplicate late event.
6. Events arriving out of order around a threshold.

### Exit criteria

The system can show why the same event is included/excluded from a window based on event time and lateness policy.

---

## Phase 7 — Flink Production-Style Streaming Jobs

### Goal

Implement the complete streaming job set from the original project proposal.

### Jobs

1. Geofence enter/exit/dwell.
2. Cold-chain excursion.
3. Route deviation.
4. Driver behaviour scoring.
5. SLA breach risk using stream-stream interval join + CEP.
6. Dwell-time anomaly using session windows.

Lab 1 defines these job patterns. fileciteturn0file0L94-L106

### Reliability

- checkpoints
- state backend
- recovery
- backpressure monitoring
- consumer lag monitoring

### Exit criteria

All required job classes run under sustained event generation and recover after a controlled process failure.

---

## Phase 8 — Bronze / Silver / Gold Lakehouse

### Goal

Separate immutable raw history from cleaned and business-ready data.

### Bronze

Store original payload and ingestion metadata.

### Silver

Implement:

- UTC standardization
- validation
- deduplication
- referential integrity
- route/shipment/geofence enrichment

### Gold

Create star-schema business marts:

```text
fact_delivery_performance
fact_cold_chain_excursion
fact_route_deviation

dim_date
dim_vehicle
dim_carrier
dim_route
dim_warehouse
dim_shipment
```

### Storage

Start with Parquet on MinIO. Introduce Iceberg after the base files are correct. The final design should include day-based event-time partitioning, possible vehicle bucketing, compaction and controlled retention. fileciteturn0file2L149-L173

### Exit criteria

Historical analytics can be run without scanning raw event streams for every dashboard request.

---

## Phase 9 — Analytics and Dashboard

### Goal

Turn Gold and live state into understandable operational views.

### Dashboards

1. Live logistics map.
2. Active alerts.
3. Shipment detail.
4. Carrier performance.
5. OTIF.
6. Cold-chain compliance.
7. Warehouse dwell.
8. Data-quality/system health.

### KPIs

- OTIF rate
- average delay
- median delay
- 95th percentile delay
- dwell time
- cold-chain excursion count
- excursion duration
- route deviation count
- alert latency
- event throughput
- consumer lag

### Exit criteria

A evaluator can start the simulation and understand system behaviour from the dashboards without inspecting internal tables.

---

## Phase 10 — Schema Governance

### Goal

Replace ad-hoc versioned JSON with explicit compatibility management.

### Tasks

- Define event schemas.
- Encode with Avro.
- Add Schema Registry.
- Enforce BACKWARD compatibility.
- Test producer/consumer compatibility across schema versions.

Lab 1 specifies versioned Avro schemas with BACKWARD compatibility; Lab 3 states versioned JSON is the initial prototype and Avro + Schema Registry is a planned improvement. fileciteturn0file0L81-L86 fileciteturn0file2L132-L136

### Exit criteria

A compatible schema change succeeds while an incompatible change is rejected.

---

## Phase 11 — CDC and MQTT Extensions

### Goal

Demonstrate realistic ingestion patterns without destabilizing the core pipeline.

### MQTT

Add Mosquitto between sensor devices and the event pipeline.

```text
IoT simulator → MQTT → bridge/consumer → Kafka → processing
```

### CDC

Add Debezium for PostgreSQL lifecycle changes.

```text
PostgreSQL → Debezium → Kafka cdc.* → consumers
```

Lab 3 explicitly treats both as extensions after direct paths are stable. fileciteturn0file2L67-L74

### Exit criteria

The same downstream event-processing logic works regardless of whether the source came from direct simulation or the extension layer.

---

## Phase 12 — Advanced Analytics and ML

### Goal

Add predictive capabilities only after reliable event and historical data exist.

### Models from original proposal

- delivery ETA regression using XGBoost
- late-delivery classification
- demand forecasting using a Prophet baseline
- unsupervised telemetry anomaly detection

Lab 1 specifies offline feature computation in Spark from Silver data and online features in Redis, with a shared transformation path to prevent training-serving skew. fileciteturn0file0L117-L121

### Additional engineering requirements

- point-in-time-correct feature joins
- no future leakage
- model versioning
- model registry
- batch vs online inference comparison
- drift monitoring
- rolling-origin evaluation

### Exit criteria

ML outputs are traceable to historical data and can be reproduced from a model/versioned feature definition.

---

## Phase 13 — Orchestration, Quality and Lineage

### Goal

Move batch transformations toward production-style data engineering.

### Tools

- Airflow
- dbt
- Great Expectations
- Marquez/OpenLineage

### Responsibilities

Airflow:

- schedule transformations
- retries
- backfills
- catchup
- SLA/freshness checks

dbt:

- Gold transformations
- business tests
- documentation
- lineage between transformation models

Great Expectations:

- domain validation suites
- completeness
- validity
- uniqueness
- consistency

Marquez/OpenLineage:

- dataset lineage
- job lineage
- upstream/downstream visibility

Lab 1 identifies these tools and the associated governance concepts. fileciteturn0file0L168-L175 fileciteturn0file0L216-L220

---

## Phase 14 — Benchmarking and Final Evaluation

### Goal

Measure the platform rather than only demonstrating it.

### Benchmarks

#### Throughput

Ramp input rate until processing begins to backpressure.

#### Alert latency

Measure:

```text
alert_timestamp - event_time
```

Report:

- p50
- p95
- p99

#### Recovery

Force a Flink TaskManager failure and measure recovery time.

#### Storage efficiency

Compare:

```text
raw JSON size
vs
Parquet + ZSTD size
```

These benchmarks are defined in Lab 1. fileciteturn0file0L123-L125

### Final acceptance

The project should be able to demonstrate both correctness and performance, with documented limitations.

---

## 2. Dependency Graph

```text
Phase 0
  ↓
Phase 1 ───────────────┐
  ↓                    │
Phase 2 → Phase 3 → Phase 4
                       ↓
                    Phase 5
                       ↓
                    Phase 6
                       ↓
                    Phase 7
                       ↓
                    Phase 8 → Phase 9
                       ↓
          ┌────────────┼───────────────┐
          ↓            ↓               ↓
       Phase 10     Phase 11        Phase 12
          ↓            ↓               ↓
          └────────────┼───────────────┘
                       ↓
                    Phase 13
                       ↓
                    Phase 14
```

## 3. MVP Boundary

The strict MVP is complete after Phase 9 if the following work end-to-end:

```text
SUMO/Python simulator
      ↓
Kafka
      ↓
Validation + deduplication
      ↓
Real-time cold-chain + route alerts
      ↓
PostgreSQL + analytical storage
      ↓
Dashboard
```

Everything after Phase 9 is an extension of the same core architecture.

## 4. Recommended Build Order Inside Each Phase

For every component use this sequence:

```text
schema
→ producer/source
→ raw storage
→ validation
→ processing
→ output persistence
→ API/query
→ dashboard
→ tests
→ failure test
→ performance test
```

This prevents the team from implementing visually impressive dashboards before the underlying data contracts and correctness are stable.
