# Detailed Technical Design

## 1. Design Objective

This document translates the architecture and product requirements into implementation-level decisions. It is intended to be detailed enough that the team can create the repository structure, database schema, event contracts, services, processing jobs, APIs, dashboards and tests directly from it.

The latest project direction is the authoritative implementation target: a Jodhpur-centred controlled logistics simulator with repeatable event generation, Kafka, PostgreSQL, long-term lakehouse storage, validation, replay and alerting. fileciteturn0file2L19-L36

## 2. Suggested Repository Structure

```text
supply-chain-intelligence/
├── README.md
├── LICENSE
├── Makefile
├── compose.yml
├── .env.example
├── .gitignore
├── docs/
│   ├── architecture.md
│   ├── phases.md
│   ├── PRD.md
│   ├── design.md
│   └── adr/
│       ├── 001-kafka-event-backbone.md
│       ├── 002-jodhpur-simulation.md
│       ├── 003-event-time-processing.md
│       └── 004-lakehouse-storage.md
├── config/
│   ├── base.yaml
│   ├── simulation.yaml
│   ├── kafka.yaml
│   └── alerts.yaml
├── infra/
│   ├── docker/
│   ├── postgres/
│   ├── kafka/
│   ├── flink/
│   ├── minio/
│   └── monitoring/
├── db/
│   ├── migrations/
│   ├── seeds/
│   └── sql/
├── schemas/
│   ├── json/
│   └── avro/
├── src/
│   ├── common/
│   ├── domain/
│   ├── simulation/
│   │   ├── sumo/
│   │   ├── gps/
│   │   ├── sensor/
│   │   ├── warehouse/
│   │   └── scenarios/
│   ├── producers/
│   ├── consumers/
│   ├── validation/
│   ├── processing/
│   │   ├── geofence/
│   │   ├── cold_chain/
│   │   ├── route_deviation/
│   │   ├── sla_risk/
│   │   └── dwell_anomaly/
│   ├── storage/
│   ├── api/
│   ├── analytics/
│   └── ml/
├── pipelines/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dbt/
├── dashboard/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── data_quality/
│   ├── scenarios/
│   └── performance/
├── scripts/
└── data/
    ├── raw_public/
    ├── calibration/
    └── scenarios/
```

## 3. Configuration Design

Never hard-code infrastructure endpoints or thresholds in application code.

Example:

```yaml
simulation:
  seed: 42
  duration_seconds: 3600
  gps_interval_seconds: 5
  sensor_interval_seconds: 30
  network: jodhpur_demo

kafka:
  bootstrap_servers: localhost:9092
  topics:
    gps: raw.gps
    sensor: raw.sensor
    warehouse: raw.warehouse
    shipment: raw.shipment
    dlq: quarantine.dlq

streaming:
  watermark_seconds: 60
  allowed_lateness_seconds: 300
  checkpoint_interval_seconds: 30

cold_chain:
  min_celsius: 2
  max_celsius: 8
  sustained_minutes: 15
```

Configuration should be loaded once at service startup and validated before the service begins producing/consuming data.

## 4. Domain Identifiers

Use stable business IDs:

```text
carrier_id
vehicle_id
warehouse_id
route_id
sensor_id
shipment_id
order_id
sku_id
geofence_id
```

Use globally unique IDs for event-level records:

```text
event_id
alert_id
```

Lab 3 explicitly separates stable business IDs from unique event IDs. fileciteturn0file2L89-L95

## 5. Database Design

### 5.1 carrier

```sql
carrier (
    carrier_id       PK,
    carrier_code     UNIQUE NOT NULL,
    carrier_name     NOT NULL,
    carrier_type,
    active            BOOLEAN NOT NULL,
    created_at,
    updated_at
)
```

### 5.2 vehicle

```sql
vehicle (
    vehicle_id       PK,
    carrier_id       FK,
    registration_no  UNIQUE,
    vehicle_type,
    refrigeration_capable,
    active,
    created_at,
    updated_at
)
```

### 5.3 route

```sql
route (
    route_id         PK,
    route_name,
    origin_warehouse_id FK,
    destination_warehouse_id FK,
    geometry,
    planned_distance_km,
    expected_duration_seconds,
    created_at,
    updated_at
)
```

### 5.4 warehouse

```sql
warehouse (
    warehouse_id     PK,
    warehouse_code   UNIQUE,
    warehouse_name,
    latitude,
    longitude,
    capacity,
    active,
    created_at,
    updated_at
)
```

### 5.5 shipment

```sql
shipment (
    shipment_id      PK,
    order_id         FK,
    carrier_id       FK,
    vehicle_id       FK,
    route_id         FK,
    status,
    priority,
    promised_delivery_time,
    actual_delivery_time,
    temperature_class,
    origin_warehouse_id,
    destination_warehouse_id,
    created_at,
    updated_at
)
```

### 5.6 sensor

```sql
sensor (
    sensor_id        PK,
    sensor_type,
    model,
    serial_number UNIQUE,
    active,
    created_at,
    updated_at
)
```

### 5.7 sensor_assignment

```sql
sensor_assignment (
    assignment_id    PK,
    sensor_id        FK,
    vehicle_id       FK,
    shipment_id      FK,
    valid_from       TIMESTAMP,
    valid_to         TIMESTAMP NULL,
    created_at
)
```

This temporal assignment is essential because devices can be reassigned. Historical readings must be interpreted using the assignment that was valid at the event time. fileciteturn0file2L82-L97

### 5.8 shipment_status_history

```sql
shipment_status_history (
    status_event_id  PK,
    shipment_id      FK,
    old_status,
    new_status,
    event_time,
    source,
    event_id UNIQUE
)
```

Never update history by deleting prior rows. Current status may be kept on `shipment` as a serving shortcut, while the history remains append-oriented.

## 6. Event Contract Design

All stream events share a common envelope.

```python
class EventEnvelope:
    event_id: str
    event_type: str
    event_time: datetime
    ingest_time: datetime
    source: str
    schema_version: str
    payload: dict
```

Use UTC internally.

### Event type naming

```text
gps.telemetry
sensor.reading
warehouse.inbound
warehouse.pick
warehouse.pack
warehouse.dispatch
warehouse.receive
shipment.created
shipment.status_changed
weather.observation
```

## 7. Event Validation API

Create one reusable validation interface:

```python
ValidationResult validate(event: EventEnvelope) -> ValidationResult
```

Where:

```python
class ValidationResult:
    valid: bool
    normalized_event: EventEnvelope | None
    errors: list[ValidationError]
```

Example failure object:

```json
{
  "rule_id": "GPS_LATITUDE_RANGE",
  "severity": "ERROR",
  "field": "latitude",
  "observed": 200,
  "expected": "[-90, 90]"
}
```

## 8. Event Deduplication Design

Primary key for deduplication is `event_id`.

Possible implementation locations:

1. Kafka/Flink state for streaming deduplication.
2. PostgreSQL unique constraint for persisted event records.
3. Analytical Silver deduplication for batch correctness.

Do not rely on only one layer. The reason is that the same event may travel through multiple independent consumers.

## 9. Scenario Engine

The scenario engine controls intentional fault injection.

### Interface

```python
class Scenario:
    name: str

    def apply(self, context):
        ...
```

### Scenario implementations

```text
NormalDeliveryScenario
GpsDropoutScenario
DuplicateEventScenario
LateEventScenario
RouteDeviationScenario
WarehouseDwellScenario
ColdChainExcursionScenario
```

### Example route-deviation behaviour

```text
1. Vehicle starts on planned route.
2. At simulation time T, scenario switches waypoint/target.
3. GPS producer continues publishing valid coordinates.
4. Route-deviation processor compares observed path with planned corridor.
5. Alert is produced after threshold crossing.
```

The event remains semantically valid; the anomaly is in the relationship between the event and the expected state.

## 10. SUMO Integration Design

The Python TraCI service should:

1. connect to SUMO;
2. step simulation;
3. query active vehicles;
4. extract vehicle position, speed and route;
5. map SUMO vehicle IDs to internal vehicle IDs;
6. construct event envelopes;
7. publish to Kafka.

Pseudo-flow:

```python
while simulation_running:
    traci.simulationStep()

    for sumo_id in traci.vehicle.getIDList():
        state = read_vehicle_state(sumo_id)
        event = build_gps_event(state)
        publish("raw.gps", key=event.vehicle_id, value=event)
```

Polling interval should be configurable rather than embedded in the code. Lab 3 proposes 5–10 simulated seconds. fileciteturn0file2L48-L55

## 11. Sensor Simulator Design

Maintain a state object per sensor:

```text
current_temperature
current_humidity
current_shock
battery
sensor_id
shipment_id
```

The normal state uses a realistic bounded random process. Fault scenarios apply a controlled shift rather than producing arbitrary impossible values.

Example cold-chain excursion:

```text
Normal: 2–8 °C
Excursion: 9–12 °C
Duration: 15–20 minutes
Recovery: return to normal range
```

The initial exact threshold remains the 2–8 °C business requirement with sustained excursion logic. fileciteturn0file2L121-L127

## 12. Warehouse Simulator Design

Represent the shipment as a state machine.

```text
CREATED
  ↓
INBOUND
  ↓
PICKED
  ↓
PACKED
  ↓
DISPATCHED
  ↓
IN_TRANSIT
  ↓
DELIVERED
```

Failure scenarios may introduce time delays between transitions but should not normally invent illegal transitions.

For example:

```text
IN_TRANSIT → PICKED
```

is invalid and must be rejected by the event-integrity rules.

## 13. Kafka Producer Design

Each producer should:

1. create event ID;
2. assign event time from source/simulation time;
3. add ingestion timestamp at publish time;
4. validate locally where inexpensive;
5. serialize using the current schema version;
6. choose the correct Kafka topic;
7. choose the correct partition key;
8. publish;
9. record success/failure metrics.

Do not use dashboard/API calls from producers.

## 14. Kafka Consumer Design

Consumers should follow:

```text
poll
→ deserialize
→ validate
→ normalize
→ process
→ persist/output
→ commit/checkpoint
```

The exact commit/checkpoint semantics depend on whether the consumer is an early Python MVP or Flink production-style job.

## 15. Cold-Chain Processor Design

### Inputs

`sensor.reading`

### Join context

Shipment temperature class and active sensor assignment.

### Processing

1. Key by shipment or sensor as appropriate.
2. Assign event-time timestamps.
3. Apply watermark.
4. Track readings in a time window.
5. Detect sustained > 8 °C condition.
6. Suppress a one-reading spike when the rule requires persistence.
7. Emit alert event.
8. Persist an alert record.

### Output

`alerts.cold_chain`

## 16. Route-Deviation Processor Design

### Inputs

- GPS telemetry
- planned route

### State

```text
vehicle_id
shipment_id
route_id
last_known_segment
last_position
```

### Detection

The implementation should define a configurable acceptable corridor around the planned route. A vehicle is considered deviated when its observed position remains outside that corridor for the configured persistence period.

This is preferable to triggering on one GPS measurement because simulated/real telemetry may contain positional noise.

## 17. Geofencing Design

A geofence is represented as:

```text
geofence_id
name
geometry
geofence_type
warehouse_id/related entity
active_from
active_to
```

The processor detects:

```text
OUTSIDE → INSIDE = ENTER
INSIDE  → OUTSIDE = EXIT
INSIDE  → INSIDE over threshold = DWELL
```

The original architecture proposes keyed state plus broadcast geofence configuration. fileciteturn0file0L94-L100

## 18. Dwell-Time Design

Warehouse dwell begins at an arrival/inbound event and ends when the shipment transitions onward.

Calculate:

```text
dwell_duration = departure_event_time - arrival_event_time
```

For generic telemetry dwell anomaly detection, a session window with a 30-minute inactivity gap is the target approach from Lab 1. fileciteturn0file0L102-L106

## 19. SLA Risk Design

Future implementation should combine:

- shipment promised delivery time
- current location/state
- expected travel duration
- warehouse delays
- historical delay features

Target processing pattern:

```text
stream-stream interval join + CEP
```

This is a later phase because it requires the preceding event-time and data-quality foundations.

## 20. Bronze Storage Design

Partition raw data by event date/time.

Recommended layout:

```text
s3://supply-chain/bronze/<event_type>/event_date=YYYY-MM-DD/
```

File format: Parquet for compact analytical representation, while preserving original payload fields/metadata required for reconstruction.

Lab 1 targets Parquet + ZSTD and day(event_time) partitioning. fileciteturn0file0L107-L110

## 21. Silver Storage Design

Example normalized schema for GPS:

```text
silver_gps(
    event_id,
    event_time,
    ingest_time,
    vehicle_id,
    shipment_id,
    route_id,
    latitude,
    longitude,
    speed_kph,
    source,
    schema_version,
    quality_status
)
```

Processing order:

```text
Bronze
→ schema normalize
→ type normalize
→ UTC normalize
→ domain validation
→ dedup
→ FK/reference checks
→ route/shipment/geofence enrichment
→ Silver
```

## 22. Gold Star Schema Design

### fact_delivery_performance

Grain: one completed shipment delivery.

Measures:

```text
promised_duration
actual_duration
delay_seconds
otif_flag
on_time_flag
in_full_flag
dwell_seconds
```

### fact_cold_chain_excursion

Grain: one detected excursion episode.

Measures:

```text
start_time
end_time
duration_seconds
max_temperature
min_temperature
exceedance_degree_seconds
```

### fact_route_deviation

Grain: one route-deviation episode.

Measures:

```text
start_time
end_time
duration_seconds
max_deviation_distance
```

The fact-grain principle follows the original project's Kimball star-schema direction. fileciteturn0file0L197-L201

## 23. Gold Dimensions

### dim_date

```text
date_key
full_date
year
quarter
month
week
day_of_week
```

### dim_vehicle

Use a surrogate key for analytical history if SCD management is introduced later.

### dim_carrier

Track carrier attributes.

### dim_route

Track planned route metadata.

### dim_warehouse

Track warehouse location/type/capacity.

### dim_shipment

Track shipment classification and SLA information.

## 24. API Implementation

Recommended technology: FastAPI.

Layers:

```text
router
  ↓
service
  ↓
repository
  ↓
PostgreSQL / analytical query engine
```

Do not put SQL directly in route handlers.

Example:

```python
@router.get("/shipments/{shipment_id}")
def get_shipment(shipment_id: str):
    return shipment_service.get_shipment(shipment_id)
```

## 25. Alert Persistence

Use an `alert` table:

```sql
alert (
    alert_id         PK,
    alert_type,
    severity,
    shipment_id FK,
    vehicle_id FK NULL,
    rule_id,
    trigger_event_id,
    start_event_time,
    end_event_time NULL,
    explanation,
    status,
    created_at,
    acknowledged_at NULL
)
```

Keep both `trigger_event_id` and a human-readable explanation so every alert is auditable.

## 26. Idempotency Strategy

Every state-changing operation should have a deterministic idempotency key where possible.

Examples:

```text
warehouse event → event_id
shipment status transition → event_id
alert creation → rule_id + episode_start + shipment_id
```

A repeated Kafka event must not create two business alerts for the same episode.

## 27. Replay Design

Replay should be a first-class operation.

```text
Kafka retained events
        ↓
start consumer from timestamp/offset
        ↓
reprocess
        ↓
rebuild derived state
```

Replay must not duplicate Gold facts or alerts. Therefore, downstream writes must be idempotent.

## 28. Exactly-Once Strategy

The mature Flink architecture targets:

- 30-second checkpoints
- EXACTLY_ONCE processing
- RocksDB state backend

as originally specified. fileciteturn0file0L90-L93

Because true end-to-end exactly-once depends on the sink, the project should distinguish:

```text
processing guarantee
vs
business result idempotency
```

Even with exactly-once stream semantics, the database layer should still protect against duplicate business writes.

## 29. Schema Evolution

### Initial

Versioned JSON:

```text
schema_version = "1.0"
```

### Target

Avro + Schema Registry.

Compatibility: BACKWARD.

Example compatible change:

```text
add optional battery_voltage
```

Example incompatible change:

```text
rename shipment_id to shipment without compatibility mapping
```

The latter must fail or require an explicit major schema transition.

## 30. Testing Strategy

### Unit tests

Test:

- validators
- event envelope creation
- geofence logic
- route deviation distance/corridor logic
- cold-chain rule
- state transitions
- deduplication

### Integration tests

Test:

```text
producer → Kafka → processor → sink
```

### Scenario tests

Each named simulator scenario is an integration test.

### Data-quality tests

Test valid and invalid examples for every rule.

### Failure tests

- Kafka restart
- consumer restart
- stream processor restart
- invalid payload
- duplicate event
- late event

### Performance tests

- throughput ramp
- latency percentiles
- recovery time
- storage compression

## 31. Test Fixtures

Keep deterministic fixtures in:

```text
tests/fixtures/
```

Example:

```json
{
  "event_id": "evt-test-001",
  "vehicle_id": "veh-test-001",
  "shipment_id": "ship-test-001",
  "event_time": "2026-01-01T00:00:00Z",
  "latitude": 26.2,
  "longitude": 73.0,
  "speed_kph": 30
}
```

## 32. Observability Design

### Structured logs

Every service log should contain:

```text
timestamp
service
level
event_id
shipment_id
vehicle_id
message
error_code
```

### Metrics

Use Prometheus-compatible metrics.

Examples:

```text
events_produced_total
producer_errors_total
events_consumed_total
consumer_lag
validation_failures_total
duplicate_events_total
late_events_total
alerts_created_total
alert_latency_seconds
checkpoint_duration_seconds
```

### Dashboard

Grafana should expose infrastructure and pipeline health; Superset can focus on business analytics.

Lab 1 maps Prometheus + Grafana to monitoring. fileciteturn0file0L171-L177

## 33. Storage Optimization

Target design:

```text
Parquet
+ ZSTD
+ event-time partitioning
+ vehicle bucketing when necessary
+ compaction
```

Lab 1 specifies a 128 MB target file size and nightly compaction to address the small-file problem. fileciteturn0file0L107-L110

Do not over-partition by high-cardinality fields such as `vehicle_id` unless volume demonstrates that bucketing provides a benefit.

## 34. Retention and Cleanup

Prototype baseline:

```text
Kafka raw topics       → 7 days
Bronze/Silver archive  → 90 days
Iceberg snapshots      → 7 days
Gold summaries         → longer retention
Alerts                 → longer retention
```

Implement cleanup as an explicit job rather than deleting data directly from application code.

## 35. Batch Processing Design

Batch jobs should run on the analytical data, not reimplement live processing logic independently wherever the business definition should be identical.

Suggested flow:

```text
Bronze
  ↓
Silver
  ↓
Gold
  ↓
metrics/marts
```

Use Spark where distributed processing is useful; DuckDB is appropriate for lightweight local development and validation. Lab 1 identifies Spark and DuckDB for batch compute. fileciteturn0file0L150-L155

## 36. dbt Model Design

Suggested dbt layers:

```text
staging
intermediate
marts
```

Example:

```text
stg_silver_gps
stg_silver_sensor
stg_shipment_status

int_shipment_timeline
int_delivery_metrics

fct_delivery_performance
fct_cold_chain_excursion
fct_route_deviation
```

Business invariants belong in dbt tests. The original proposal explicitly assigns business-invariant checks to dbt between Silver and Gold. fileciteturn0file0L111-L116

## 37. ML Design

Only after the analytical layer stabilizes.

### ETA regression

Inputs may include:

```text
current location
remaining route distance
elapsed trip time
carrier
vehicle type
historical delay statistics
warehouse dwell
weather
```

### Late-delivery classification

Target:

```text
late = 1
on_time = 0
```

### Demand forecasting

Initial baseline: Prophet.

### Telemetry anomaly detection

Detect unusual combinations of:

```text
speed
acceleration/shock
temperature
humidity
battery
```

Lab 1 defines the proposed ML set. fileciteturn0file0L117-L121

## 38. Online/Offline Feature Consistency

Where ML is implemented, feature transformations must be shared rather than independently rewritten for training and inference.

Target architecture:

```text
Silver
 ├── offline feature computation → training
 └── same transformations → online features → Redis
```

This directly addresses training-serving skew, which is explicitly identified in Lab 1. fileciteturn0file0L117-L121

## 39. CDC Extension Design

Once direct PostgreSQL writes are stable:

```text
PostgreSQL WAL
    ↓
Debezium
    ↓
Kafka cdc.*
    ↓
Silver / stream processors
```

Do not introduce CDC before shipment state transitions are correct in PostgreSQL.

## 40. MQTT Extension Design

Once direct sensor → Kafka generation is stable:

```text
Sensor simulator
      ↓
Mosquitto
      ↓
MQTT consumer/bridge
      ↓
Kafka raw.sensor
```

The downstream sensor schema must remain unchanged so the processing layer remains source-independent.

## 41. Security Design for the Prototype

Even though production-grade IAM is out of scope, implement basic hygiene:

- `.env` for local secrets
- `.env.example` checked in
- no passwords in code
- local network exposure minimized
- separate database users for applications where practical
- API input validation

The goal is sound engineering without expanding the project into production security infrastructure.

## 42. Local Deployment Profiles

### Lite profile

Target: development on approximately 8 GB RAM.

Keep the stack small; Lab 1 specifically identifies a lite profile around Redpanda + DuckDB for development. fileciteturn0file0L183-L187

### Full profile

Target: 16–32 GB RAM for benchmarking and final evaluation.

Use full streaming, analytical, dashboard and monitoring components. fileciteturn0file0L185-L187

## 43. Make Commands

Recommended developer interface:

```bash
make up
make down
make logs
make db-migrate
make db-seed
make simulate
make scenario SCENARIO=COLD_CHAIN_EXCURSION
make test
make integration-test
make quality
make pipeline
make dashboard
make benchmark
make clean
```

The important design principle is that evaluators should not need to remember dozens of Docker commands.

## 44. Definition of Done for a Component

A component is not complete when its code runs once. It is complete when:

```text
code
+ configuration
+ unit tests
+ integration test
+ observability
+ failure handling
+ documentation
+ reproducible startup
```

are present.

## 45. Architecture Decision Records to Maintain

Create ADRs for at least:

1. Why Kafka is the durable event backbone.
2. Why simulation is centred on Jodhpur.
3. Why event time is used instead of processing time.
4. Why narrow event streams are retained instead of one wide table.
5. Why PostgreSQL is used for operational data.
6. Why MinIO/Iceberg are used for analytical history.
7. Why MQTT/CDC/ML are phased rather than mandatory in MVP.

## 46. Design Traceability to the Three Labs

### Lab 1 retained

- end-to-end streaming + batch concept
- Kafka event backbone
- Bronze/Silver/Gold lakehouse
- stream processing jobs
- data quality
- ML direction
- local AWS-equivalent stack
- benchmarking

### Lab 2 retained

- product is positioned within an existing ecosystem rather than as a novel standalone business category
- commercial systems include transportation visibility, fleet telematics, cold-chain monitoring, control towers, analytics and predictive ETA
- research supports the value of integrating IoT and analytics for proactive supply-chain decisions

Lab 2 concludes that existing products are valuable but users can still face integration, configuration, stale/inaccurate tracking and false alert issues. fileciteturn0file1L49-L74

### Lab 3 changes the implementation emphasis

- national model → Jodhpur-centred prototype
- generic synthetic feed → controlled SUMO/Python simulator
- direct dashboard path → Kafka-mediated architecture
- one wide table → narrow event streams + relational master data
- all technologies immediately → phased extensions
- broad interoperability claim → explicit prototype limitation

The direct simulator-to-dashboard and all-in-one-table alternatives were explicitly rejected in Lab 3. fileciteturn0file2L174-L197

## 47. Final End-to-End Flow

```text
               ┌───────────────────────┐
               │ Public data calibration│
               └───────────┬───────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │ Scenario Engine  │
                  └───────┬──────────┘
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
      SUMO/TraCI     IoT simulator    Warehouse sim
          │               │                │
          └───────────────┼────────────────┘
                          ▼
                       Kafka
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
      Validation/DLQ              Raw capture
             │                         │
             ▼                         ▼
      Event-time processing       Bronze
             │                         │
      ┌──────┼─────────────┐          ▼
      ▼      ▼             ▼        Silver
  Cold-chain Route      Geofence      │
  alert     deviation   / dwell       ▼
      │        │             │       Gold
      └────────┼─────────────┘         │
               ▼                       ▼
             Alerts             Analytics marts
               │                       │
               └──────────┬────────────┘
                          ▼
                     FastAPI/UI
                          │
                ┌─────────┴──────────┐
                ▼                    ▼
             Live Ops           Historical BI
```

## 48. Immediate Implementation Priority

The first engineering objective should be:

```text
PostgreSQL domain model
→ Jodhpur SUMO network
→ GPS producer
→ sensor producer
→ warehouse producer
→ Kafka
→ validation/DLQ
→ cold-chain alert
→ route-deviation alert
→ dashboard
```

Only after this chain is stable should the team spend substantial implementation effort on Schema Registry, Flink's complete job set, Iceberg refinements, CDC, MQTT, ML, Airflow, dbt, lineage and advanced benchmarking.

This follows the latest lab's explicit emphasis on validating the core pipeline before extensions. fileciteturn0file2L198-L208
