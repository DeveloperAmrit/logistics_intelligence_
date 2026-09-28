# Product Requirements Document (PRD)

## 1. Product Name

**Real-Time Supply Chain & Logistics Intelligence Platform**

## 2. Product Summary

A locally hosted supply-chain intelligence platform that simulates logistics operations and converts continuous vehicle, cold-chain, warehouse and shipment events into real-time operational alerts and historical analytics.

The original project concept was an end-to-end platform capable of ingesting high-velocity logistics events, processing them in streaming and batch modes, and serving alerts, dashboards and ML predictions. fileciteturn0file0L5-L11

The current product scope has been refined into a controlled, Jodhpur-centred prototype. SUMO + TraCI provide simulated road movement; Python services generate IoT and warehouse events; Kafka acts as the durable event layer; PostgreSQL stores operational data; and MinIO/Apache Iceberg provide the intended analytical lakehouse path. fileciteturn0file2L6-L18

## 3. Problem Statement

A logistics operator needs to know not only where shipments are, but whether they are becoming operationally unsafe or late.

The product must answer questions at multiple latencies:

```text
Seconds:
Is this truck deviating from its planned route?

Seconds/minutes:
Has this refrigerated shipment breached its temperature limits?

Minutes:
Which shipments due today are at risk of missing SLA?

Hours/days:
What was the delivery and carrier performance over a historical period?
```

The original proposal explicitly distinguishes seconds-level streaming decisions, minutes-level risk decisions, and hours-level batch analytics. fileciteturn0file0L12-L28

## 4. Product Vision

Build a reproducible local logistics control-tower prototype that demonstrates how a modern data platform turns raw events into operational decisions.

The project is not primarily a UI product. Its core value is the data-engineering pipeline behind the UI:

```text
event generation
→ durable ingestion
→ validation
→ event-time processing
→ stateful detection
→ historical modelling
→ serving
→ observability
```

## 5. Target Users

### 5.1 Operations controller

Needs a live view of:

- active shipments
- vehicle locations
- alerts
- route deviations
- cold-chain excursions
- warehouse delays

### 5.2 Logistics manager

Needs historical metrics:

- carrier performance
- OTIF
- delays
- dwell time
- exception trends

### 5.3 Data engineer

Needs to inspect:

- event flow
- validation failures
- duplicates
- late events
- Kafka lag
- pipeline health
- replay behaviour

### 5.4 Project evaluator/student developer

Needs deterministic demonstrations of data-engineering concepts and measurable system behaviour.

## 6. Product Goals

### Primary goals

1. Generate controlled logistics events repeatedly.
2. Preserve events through a durable event log.
3. Detect meaningful operational exceptions in near real time.
4. Store a clean historical analytical representation.
5. Provide understandable live and historical dashboards.
6. Demonstrate replay, late-event and failure handling.
7. Keep the complete stack runnable locally at zero infrastructure cost.

The zero-cost, self-hosted motivation and reproducibility requirements come from Lab 1. fileciteturn0file0L67-L80

### Secondary goals

- demonstrate schema evolution
- demonstrate CDC and MQTT extensions
- demonstrate batch processing
- demonstrate ML lifecycle concepts
- compare implementation choices with AWS equivalents

## 7. Non-Goals

The current project does not attempt to provide:

- actual commercial carrier integrations
- national-scale real production coverage
- petabyte-scale infrastructure
- IAM-grade authorization
- multi-AZ fault tolerance
- elastic cloud autoscaling
- a fully autonomous digital twin

Lab 3 explicitly states that the prototype validates data integration and alerting rather than carrier-network coverage, production security or a fully autonomous digital twin. fileciteturn0file2L198-L208

## 8. Functional Requirements

### FR-001 — Simulation setup

The system shall provide a controlled logistics simulator centred on a small Jodhpur road network.

Acceptance:

- road-valid vehicle movement is generated;
- vehicle IDs are stable within a run;
- route and position are exposed;
- simulation time is available to producers.

Lab 3 specifies SUMO + TraCI for this functionality. fileciteturn0file2L30-L36

### FR-002 — GPS events

The system shall generate GPS telemetry containing at least:

```text
vehicle_id
shipment_id
latitude
longitude
speed
route_id
simulation/event time
```

The original feed design polls active vehicles every 5–10 simulated seconds. fileciteturn0file2L48-L55

### FR-003 — Sensor events

The system shall generate:

```text
sensor_id
shipment_id
vehicle_id where known
temperature
humidity
shock
battery
```

at an initial interval of 30–60 seconds per sensor. fileciteturn0file2L55-L60

### FR-004 — Warehouse events

The system shall generate inbound, pick, pack, dispatch and receive events on simulated state transitions. fileciteturn0file2L61-L66

### FR-005 — Shipment lifecycle

The system shall represent shipment creation, assignment and status transitions in PostgreSQL.

### FR-006 — Event envelope

Every stream record shall carry:

```text
event_id
event_time
ingest_time
source
schema_version
```

Lab 3 explicitly requires these fields. fileciteturn0file2L89-L95

### FR-007 — Kafka ingestion

The system shall publish stream records to Kafka and preserve them long enough for replay of recent history.

The initial retention target is seven days for raw Kafka topics. fileciteturn0file2L149-L153

### FR-008 — Ordering

The system shall partition events using entity-specific keys:

```text
GPS       → vehicle_id
Sensor    → sensor_id
Lifecycle → shipment_id
Warehouse → shipment_id
```

fileciteturn0file2L137-L145

### FR-009 — Validation

The system shall reject or quarantine invalid records.

### FR-010 — Deduplication

The system shall identify duplicate event IDs within the configured retention/processing window.

### FR-011 — Quarantine

Every rejected record shall preserve:

```text
reason
raw payload
arrival/ingestion time
source
```

Lab 3 requires these details in the quarantine/DLQ path. fileciteturn0file2L128-L131

### FR-012 — Cold-chain alert

For a 2–8 °C shipment, sustained temperature excursion shall generate an alert according to the configured rule.

Initial rule:

```text
Temperature > 8 °C
AND
(two consecutive readings OR 15-minute excursion)
```

fileciteturn0file2L121-L127

### FR-013 — Route deviation alert

The system shall compare observed vehicle movement with planned route information and emit a route-deviation event when the configured deviation threshold is crossed.

### FR-014 — Event-time processing

The system shall use event time for time-dependent analytics and support bounded out-of-order events.

Initial watermark/lateness configuration:

```text
watermark = 60 seconds
allowed lateness = 5 minutes
```

fileciteturn0file0L90-L93

### FR-015 — Historical storage

The system shall persist historical events in a Bronze/Silver/Gold organization.

### FR-016 — Gold analytics

Gold shall expose data suitable for:

- delivery performance
- cold-chain excursions
- route deviations
- OTIF
- dwell time
- carrier performance

Lab 3 explicitly identifies these Gold use cases. fileciteturn0file2L162-L166

### FR-017 — Current-state serving

The system shall expose current location, shipment state and active alerts to a dashboard/API.

### FR-018 — Replay

After a consumer failure, the system shall be capable of replaying retained Kafka events.

### FR-019 — Failure scenario support

The simulator shall support deliberate:

- dropout
- duplicate events
- late events
- route deviation
- warehouse dwell delay
- cold-chain excursion

fileciteturn0file2L141-L145

### FR-020 — Benchmarking

The system shall record throughput, alert latency and recovery metrics.

## 9. Data Requirements

### 9.1 GPS record

```json
{
  "event_id": "uuid",
  "event_type": "gps.telemetry",
  "vehicle_id": "veh-001",
  "shipment_id": "ship-001",
  "route_id": "route-001",
  "latitude": 26.2389,
  "longitude": 73.0243,
  "speed_kph": 42.4,
  "event_time": "...",
  "ingest_time": "...",
  "source": "sumo",
  "schema_version": "1.0"
}
```

### 9.2 Sensor record

```json
{
  "event_id": "uuid",
  "event_type": "sensor.reading",
  "sensor_id": "sensor-001",
  "shipment_id": "ship-001",
  "temperature_c": 6.7,
  "humidity_pct": 52.0,
  "shock_g": 0.04,
  "battery_pct": 91.0,
  "event_time": "...",
  "ingest_time": "...",
  "source": "iot-simulator",
  "schema_version": "1.0"
}
```

### 9.3 Warehouse event

```json
{
  "event_id": "uuid",
  "event_type": "warehouse.dispatch",
  "shipment_id": "ship-001",
  "warehouse_id": "wh-001",
  "event_time": "...",
  "ingest_time": "...",
  "source": "warehouse-simulator",
  "schema_version": "1.0"
}
```

### 9.4 Shipment status event

```json
{
  "event_id": "uuid",
  "event_type": "shipment.status_changed",
  "shipment_id": "ship-001",
  "old_status": "IN_TRANSIT",
  "new_status": "DELIVERED",
  "event_time": "...",
  "ingest_time": "...",
  "source": "postgres",
  "schema_version": "1.0"
}
```

## 10. Non-Functional Requirements

### NFR-001 — Reproducibility

Same scenario + same seed should produce the same scenario behaviour within defined simulation tolerances.

### NFR-002 — Observability

Every stage should expose logs and metrics sufficient to identify where an event stopped or was rejected.

### NFR-003 — Reliability

The system should recover from consumer/stream-processing failures without requiring a complete data regeneration.

### NFR-004 — Data integrity

No invalid record should become valid analytical data merely because validation was bypassed in one layer.

### NFR-005 — Explainability

Every alert should be linked to the events and rule that caused it.

### NFR-006 — Local execution

The stack must run using open-source components locally through Docker Compose. This is a foundational project requirement. fileciteturn0file0L130-L187

### NFR-007 — Maintainability

Configuration must be externalized and components must have clear input/output contracts.

### NFR-008 — Performance measurement

Alert latency shall be evaluated at p50/p95/p99 and throughput shall be measured up to backpressure. fileciteturn0file0L123-L125

## 11. Alert Requirements

Every alert should contain:

```text
alert_id
alert_type
severity
shipment_id
vehicle_id when applicable
rule_id
trigger_event_id
start_event_time
created_at
status
explanation
```

### Severity model

```text
INFO
WARNING
CRITICAL
```

The exact business severity thresholds can be configured later; initial implementation should prioritize deterministic rule evaluation.

## 12. API Requirements

### Shipments

```text
GET /api/v1/shipments
GET /api/v1/shipments/{shipment_id}
GET /api/v1/shipments/{shipment_id}/timeline
```

### Vehicles

```text
GET /api/v1/vehicles
GET /api/v1/vehicles/{vehicle_id}
GET /api/v1/vehicles/{vehicle_id}/trajectory
```

### Alerts

```text
GET /api/v1/alerts
GET /api/v1/alerts/{alert_id}
```

### Analytics

```text
GET /api/v1/analytics/otif
GET /api/v1/analytics/carriers
GET /api/v1/analytics/cold-chain
GET /api/v1/analytics/dwell-time
```

## 13. Dashboard Requirements

### Live map

The live page shall show:

- simulated vehicles
- shipment identifiers
- route information
- active alerts

### Alerts panel

Show:

- alert type
- severity
- shipment
- time
- explanation
- acknowledgement/status

### Shipment page

Show the chronological combination of:

- shipment lifecycle
- warehouse events
- GPS observations
- sensor readings
- generated alerts

### Analytics page

Show:

- OTIF
- carrier comparison
- delays
- dwell time
- cold-chain compliance
- route deviations

## 14. Data Quality Requirements

The system should monitor six quality dimensions:

```text
accuracy
completeness
consistency
timeliness
validity
uniqueness
```

These are the quality dimensions identified in Lab 1. fileciteturn0file0L216-L220

Each failed check should have:

```text
dataset
record/event ID
rule ID
failure reason
observed value
expected range/condition
first_seen
last_seen
```

## 15. Storage Retention

Initial prototype policy:

```text
Kafka raw events             7 days
Gold summaries               longer-lived / frequently accessed
Bronze/Silver compressed     90 days
Iceberg snapshots            7 days
```

Lab 3 specifies seven-day Kafka retention, 90-day Bronze/Silver prototype archive and longer retention for frequently accessed alerts/completed-shipment summaries. fileciteturn0file2L167-L173

## 16. User Stories

### Operations

As an operations controller, I want to see live vehicles and active alerts so I can identify problematic shipments immediately.

As an operations controller, I want to open a shipment and see its event timeline so I can understand why it is at risk.

As an operations controller, I want a cold-chain alert only when an excursion is sustained so that one noisy sensor reading does not create unnecessary escalation.

### Management

As a logistics manager, I want carrier OTIF metrics so that I can compare delivery performance.

As a logistics manager, I want dwell-time analytics so that I can identify warehouse bottlenecks.

### Data engineering

As a data engineer, I want rejected records stored in a DLQ so that invalid input can be inspected and replayed.

As a data engineer, I want Kafka retention and replay so that a failed consumer can recover without rerunning the entire simulation.

As a data engineer, I want event time and ingestion time stored separately so that I can reason about late and out-of-order events.

## 17. Acceptance Test Scenarios

### AT-001 Normal delivery

Given a valid shipment and vehicle route, when the simulation runs normally, GPS events arrive and no false route-deviation or cold-chain alerts are created.

### AT-002 Cold-chain excursion

Inject sustained temperature > 8 °C. The system creates exactly the expected cold-chain alert according to the configured rule.

### AT-003 Sensor noise

Inject a single abnormal reading followed by normal readings. No sustained excursion alert should be produced solely from that single reading.

### AT-004 Duplicate event

Publish the same event ID twice. Analytical output should count the business event once and the duplicate should be measurable.

### AT-005 Late event

Send an event after the normal event-time order. The event should be either correctly incorporated within allowed lateness or routed to the late-event path after the limit.

### AT-006 Invalid GPS

Publish latitude = 200. The record must not enter the valid Silver dataset and must appear in quarantine/DLQ.

### AT-007 Invalid sensor

Publish humidity = 150. The record must be quarantined.

### AT-008 Route deviation

Move the vehicle outside the permitted route corridor. A route-deviation alert should be generated.

### AT-009 Consumer recovery

Kill a stream-processing consumer. Restart it and confirm that retained Kafka data can be replayed and state restored according to the configured semantics.

### AT-010 Dashboard

After simulation events are generated, the dashboard shall expose current locations, active alerts, and historical KPIs without querying the raw Kafka log directly.

## 18. Success Metrics

### Data pipeline

- sustained events/sec
- percentage of valid events
- duplicate detection rate
- late-event rate
- DLQ rate

### Streaming

- alert p50/p95/p99 latency
- consumer lag
- checkpoint success rate
- recovery time

### Storage

- raw JSON size
- Parquet + ZSTD size
- compression ratio
- query latency for Gold analytics

### Product usability

- evaluator can launch a predefined scenario with one command
- alert explanation is traceable to source events
- dashboard shows scenario outcome clearly

## 19. Future Product Capabilities

These are explicitly downstream of the validated core:

- MQTT-based IoT ingestion
- Debezium PostgreSQL CDC
- Apache Iceberg lifecycle/partition evolution
- Trino ad-hoc lake querying
- Redis online feature store
- ETA prediction
- late-delivery classification
- demand forecasting
- telemetry anomaly detection
- MLflow model registry
- Airflow orchestration
- dbt semantic/business transformations
- lineage through OpenLineage/Marquez

The original project proposal includes these capabilities but the latest lab intentionally phases them rather than requiring all of them in the initial build. fileciteturn0file0L117-L121 fileciteturn0file2L204-L208

## 20. Product Constraints

- zero paid cloud infrastructure
- local hardware execution
- open-source stack
- semester-scale scope
- controlled synthetic simulation
- public datasets used for calibration only
- reproducible demonstration

## 21. Product Decision Summary

The product is deliberately not a direct simulator-to-dashboard application. Lab 3 rejects that architecture because it removes the durable event layer, prevents replay and makes independent alert/storage consumers difficult. fileciteturn0file2L174-L193

The chosen product architecture therefore treats the durable event stream and historical data platform as first-class product functionality.
