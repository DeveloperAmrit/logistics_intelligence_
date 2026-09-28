# Real-Time Supply Chain & Logistics Intelligence Platform

## 1. Purpose

This document defines the implementation architecture for the semester project. It combines the original Lab 1 architecture with the refinements made in Labs 2 and 3.

The project started as a broad end-to-end, AWS-like local supply-chain intelligence platform. The current implementation target is intentionally narrower: a repeatable, Jodhpur-centred logistics simulation that generates GPS, cold-chain sensor, warehouse, and shipment-lifecycle events; moves those events through a durable Kafka event layer; validates and processes them; stores operational and analytical data; and exposes live alerts and historical analytics.

The system is designed to demonstrate data-engineering concepts rather than claim production interoperability with commercial carriers. Lab 3 explicitly establishes controlled simulation, repeatable event generation, replayability, data integration, and alerting as the current prototype focus. fileciteturn0file2L6-L18

## 2. Current Architectural Goal

The first implementation milestone is a correct Jodhpur-centred prototype. Optional technologies such as MQTT, Debezium CDC, Apache Iceberg, and ML are additions after the core pipeline is working. Lab 3 rejects building the full national model and every proposed tool immediately because it is too broad for a semester project. fileciteturn0file2L194-L197

Therefore, the architecture is divided into:

1. Core path: simulator → Kafka → stream processing/validation → operational/live serving → analytics storage/dashboard.
2. Analytical path: Kafka/raw events → Bronze → Silver → Gold → SQL/OLAP/dashboard.
3. Future extensions: MQTT ingestion, CDC through Debezium, Iceberg-backed lakehouse management, ML feature/serving pipeline, advanced geospatial processing, orchestration and lineage.

## 3. High-Level Architecture

```text
                         ┌──────────────────────────┐
                         │       Simulated World     │
                         │  SUMO + TraCI (roads)     │
                         │  Python IoT simulator     │
                         │  Python warehouse events  │
                         │  Python order/shipment    │
                         └────────────┬─────────────┘
                                      │
                          versioned events / JSON
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │          Kafka           │
                         │ durable event log        │
                         │ keyed by business entity │
                         │ replay + consumer groups │
                         └──────┬───────────┬───────┘
                                │           │
                ┌───────────────┘           └──────────────────┐
                ▼                                              ▼
      ┌──────────────────────┐                       ┌──────────────────────┐
      │ Stream Processing    │                       │ Raw / Lakehouse Path │
      │ Flink (target)       │                       │ Bronze               │
      │ or first prototype   │                       │ Silver               │
      │ equivalent Python    │                       │ Gold                 │
      └──────────┬───────────┘                       └──────────┬───────────┘
                 │                                              │
       ┌─────────┼─────────┐                           ┌──────────┴──────────┐
       ▼         ▼         ▼                           ▼                     ▼
   Alerts     Live ETA   Current state          PostgreSQL/MinIO       ClickHouse/Trino
       │         │         │                      Parquet/Iceberg       analytical SQL
       └─────────┼─────────┘                           │                     │
                 ▼                                    └──────────┬──────────┘
          FastAPI / serving                                      ▼
                 │                                      Superset/Grafana
                 ▼
             Dashboard
```

The Lab 1 concept uses Kafka + Schema Registry feeding both Flink and Bronze/Silver/Gold, with ClickHouse/Trino providing analytical access and Superset/Grafana/API as serving layers. fileciteturn0file0L45-L54

Lab 3 modifies the ingestion side by making the simulator explicit: SUMO + TraCI produce road-valid vehicle movement, while Python services generate sensor and warehouse events. PostgreSQL is the operational store, Kafka is the durable event layer, and MinIO + Apache Iceberg are the intended long-term analytical storage direction. fileciteturn0file2L19-L36

## 4. Architectural Principles

### 4.1 Event-driven design

Kafka is the durable boundary between producers and consumers. Producers do not write directly to the dashboard or analytical database. This makes consumers independently deployable and allows event replay.

### 4.2 Narrow event streams instead of one wide table

GPS and sensor data have different sampling rates and attributes. A single wide table would repeatedly store location fields for sensor observations, create null-heavy rows, and make corrections difficult. Lab 3 therefore keeps event streams narrow and connects them with stable identifiers and event time. fileciteturn0file2L42-L81

### 4.3 Event time is authoritative

Events contain both event time and ingestion time. Assignment validity and shipment status are evaluated using event time so later changes to assignments do not rewrite the historical meaning of a reading. fileciteturn0file2L89-L97

### 4.4 No silent data loss

Malformed, unmatched, invalid, duplicate, or excessively late records must be observable. The system stores a rejection reason and keeps the raw payload in a quarantine/DLQ path so failures can be inspected and replayed. Lab 1 specifies quarantine records as replayable rather than silently dropped. fileciteturn0file0L111-L121

### 4.5 Prototype first, extensions second

The core system must work without requiring every optional component. The project should first establish correct simulation, event contracts, Kafka transport, validation, storage, and alerting. Only then should MQTT, CDC, Iceberg refinement, ML, orchestration, lineage, and benchmarking extensions be enabled.

## 5. Component Architecture

### 5.1 Simulation layer

#### SUMO + TraCI

Responsibilities:

- Load a small Jodhpur-centred road network.
- Generate valid vehicle movement on roads.
- Maintain vehicle ID, position, speed, route and simulation time.
- Provide data to a Python producer through TraCI.
- Support repeatable simulation runs using fixed seeds/configuration.
- Allow deliberate scenarios such as route deviation and GPS dropout.

Lab 3 specifies that SUMO simulates road-valid movement and that TraCI exposes position, speed, route and simulation time. The Python producer polls active vehicles every 5–10 simulated seconds. fileciteturn0file2L30-L36 fileciteturn0file2L48-L59

#### IoT simulator

Generate:

- temperature
- humidity
- shock
- battery
- sensor ID
- shipment ID
- event time

Initial frequency: one event every 30–60 seconds per simulated sensor. MQTT may be added later; Kafka is the minimum working path. fileciteturn0file2L55-L60

#### Warehouse simulator

Generate event-driven state changes:

- inbound
- pick
- pack
- dispatch
- receive

The warehouse event should be produced only when simulated state changes. fileciteturn0file2L61-L66

#### Order/shipment simulator

PostgreSQL is the operational source for orders, shipments and master data. Direct inserts/updates are the initial approach. Debezium CDC is a later extension. fileciteturn0file2L67-L74

### 5.2 External/reference data

Public datasets are calibration inputs, not claims of live source data:

- Olist Brazilian E-Commerce
- DataCo Smart Supply Chain
- NYC TLC trips

They are used to estimate plausible order arrival, trip duration and delay distributions. Weather is optional hourly API enrichment and should be cached locally so demonstrations remain repeatable. fileciteturn0file2L30-L36

Lab 1 additionally identifies slowly changing master/reference data including SKU, carrier, warehouse, lane and geofence. fileciteturn0file0L39-L44

## 6. Kafka Architecture

### 6.1 Topics

Recommended initial topics:

```text
raw.gps
raw.sensor
raw.warehouse
raw.shipment
raw.weather
alerts.cold_chain
alerts.route_deviation
alerts.sla_risk
quarantine.dlq
```

Future naming can be consolidated into the Lab 1 convention of `raw.*`, `cdc.*`, `alerts.*`, and `dlq.*`. Raw topics have a target retention of seven days; CDC topics are candidates for compaction. fileciteturn0file0L81-L86

### 6.2 Partition keys

- GPS: `vehicle_id`
- Sensor: `sensor_id`
- Shipment lifecycle: `shipment_id`
- Warehouse: `shipment_id`

Lab 3 explicitly defines these keys so related records remain ordered within their respective entity stream. fileciteturn0file2L135-L145

### 6.3 Event contract

Every event should contain at least:

```json
{
  "event_id": "uuid",
  "event_type": "gps.telemetry",
  "event_time": "2026-09-12T08:30:00Z",
  "ingest_time": "2026-09-12T08:30:03Z",
  "source": "sumo",
  "schema_version": "1.0",
  "vehicle_id": "veh_001",
  "shipment_id": "ship_001",
  "payload": {}
}
```

Lab 3 requires `event id`, `event time`, `ingest time`, `source`, and `schema version` on every stream record. fileciteturn0file2L89-L95

The first prototype can publish versioned JSON. Avro + Schema Registry is the planned compatibility improvement. fileciteturn0file2L132-L136

## 7. Operational Data Model

The operational core is relational.

### Master/transaction entities

```text
carrier
vehicle
warehouse
route
sensor
sensor_assignment
shipment
order
sku
geofence
lane
```

### Event entities

```text
gps_telemetry
sensor_reading
warehouse_scan
shipment_status_event
weather_observation
```

The important relationship is that shipment references its assigned vehicle and route, while `sensor_assignment` records `sensor_id`, `vehicle_id`, `valid_from`, and `valid_to`. This supports device reassignment without corrupting historical sensor interpretation. fileciteturn0file2L82-L97

## 8. Validation Architecture

Validation is layered.

### Layer 1: Structural validation

Reject/quarantine records with:

- missing event ID
- missing timestamp
- unsupported schema version
- malformed JSON/Avro
- required field missing

Schema Registry is planned for structural governance and backward compatibility. Lab 1 places schema validation at produce time. fileciteturn0file0L111-L116

### Layer 2: Domain validation

GPS:

- latitude ∈ [-90, 90]
- longitude ∈ [-180, 180]
- speed ≥ 0
- known vehicle
- valid shipment association where required

Sensor:

- temperature ∈ [-30, 50] °C
- humidity ∈ [0, 100]
- known sensor
- valid sensor-to-vehicle assignment

Event integrity:

- non-null ID/timestamp
- event ID uniqueness within retention window
- legal shipment-status transition

These checks are directly specified by Lab 3. fileciteturn0file2L103-L131

## 9. Streaming Processing

The mature target is Apache Flink using event-time semantics.

Initial common configuration from Lab 1:

- 60-second bounded-out-of-orderness watermark
- 5-minute allowed lateness
- late events to side output
- 30-second checkpoints
- EXACTLY_ONCE mode
- RocksDB state backend

These are starting configurations and should be measured and adjusted after simulator testing. fileciteturn0file0L90-L106 fileciteturn0file2L146-L148

### Streaming jobs

1. Geofence enter/exit/dwell
   - keyed processing by vehicle
   - broadcast state for geofence definitions

2. Cold-chain excursion
   - sliding window
   - suppress sensor noise
   - alert when a sustained 2–8 °C shipment condition is violated

3. Route deviation
   - temporal stream/table join with planned route

4. Driver behaviour score
   - one-hour tumbling aggregation

5. SLA breach risk
   - stream-stream interval join + CEP

6. Dwell-time anomaly
   - session window with 30-minute inactivity gap

The job patterns come from the original architecture and should be implemented incrementally rather than all at once. fileciteturn0file0L90-L106

## 10. Cold-Chain Rule

For a 2–8 °C product, the initial alert rule is:

```text
Temperature > 8 °C
AND
(two consecutive readings OR approximately 15 minutes of sustained violation)
→ cold-chain alert
```

This avoids triggering an alert from one noisy observation. Lab 3 specifies the two-consecutive-readings or 15-minute condition. fileciteturn0file2L121-L127

The exact production implementation should express this using event time and a window rather than relying on wall-clock sleep.

## 11. Lakehouse Architecture

### Bronze

Stores immutable received events plus ingestion metadata and original payload.

### Silver

Stores:

- standardized UTC timestamps
- validated records
- deduplicated records
- normalized fields
- route/shipment/geofence enrichment

### Gold

Business-ready star-schema facts/dimensions.

Initial facts:

- `fact_delivery_performance`
- `fact_cold_chain_excursion`
- `fact_route_deviation`

Dimensions:

- `dim_date`
- `dim_vehicle`
- `dim_carrier`
- `dim_route`
- `dim_warehouse`
- `dim_shipment`

Lab 3 defines these Gold concepts and identifies OTIF, delay, dwell-time and excursion dashboards as their intended use. fileciteturn0file2L158-L173

The intended durable analytical storage is MinIO with Parquet and preferably Apache Iceberg. Lab 1 specifies ZSTD compression, 128 MB target file size, day-based event-time partitioning, vehicle bucketing where needed, nightly compaction, and seven-day snapshot retention. fileciteturn0file0L107-L110

## 12. Serving Layer

### PostgreSQL

Use for:

- current operational entities
- orders
- shipments
- master data
- current shipment status
- planned routes
- sensor assignments

### ClickHouse / Trino

Use for:

- analytical SQL
- aggregate queries
- historical reporting
- ad-hoc investigation

Lab 1 maps ClickHouse to OLAP serving and Trino to lake querying. fileciteturn0file0L157-L167

### FastAPI

Expose stable APIs for dashboard clients, for example:

```text
GET /shipments
GET /shipments/{shipment_id}
GET /vehicles/{vehicle_id}/location
GET /shipments/{shipment_id}/timeline
GET /alerts
GET /alerts/{alert_id}
GET /analytics/ot​​if
GET /analytics/carriers
GET /analytics/cold-chain
```

The API should not become the event pipeline. Kafka remains the event boundary.

## 13. Dashboard

Suggested dashboard views:

### Live Operations

- active vehicles on map
- current shipment status
- active alerts
- route deviations
- cold-chain excursions

### Shipment Detail

- shipment metadata
- planned route
- actual GPS trajectory
- warehouse milestones
- sensor history
- alert timeline

### Carrier Performance

- OTIF
- delay distribution
- exception count
- route deviation rate
- dwell time

### Cold-Chain Monitoring

- excursions by shipment
- duration above/below threshold
- maximum temperature
- sensor health

### Data Quality / System Health

- events received/sec
- validation failures
- duplicate count
- late-event count
- DLQ count
- Kafka consumer lag
- processing latency

## 14. Geospatial Architecture

The simulation currently uses a Jodhpur-centred road network. The system should preserve coordinates in WGS84-compatible latitude/longitude fields.

Target geospatial capabilities:

- route geometry
- geofence representation
- point-in-polygon tests
- distance calculations
- geofence enter/exit
- route deviation detection

Lab 1 identifies H3, OSRM and GeoPandas for this layer. fileciteturn0file0L222-L225

## 15. Failure and Recovery Architecture

Potential failures:

- producer crash
- Kafka broker restart
- consumer crash
- duplicate event
- late event
- schema violation
- invalid business relationship
- Flink TaskManager failure
- database unavailable
- dashboard/API unavailable

Recovery mechanisms:

- Kafka replay
- idempotent event handling
- checkpoints/savepoints
- DLQ/quarantine
- transactional database writes where required
- consumer groups
- event IDs for deduplication

Lab 1 specifically requires benchmarking recovery after a forced TaskManager kill. fileciteturn0file0L123-L125

## 16. Deployment Architecture

All services run in Docker Compose for reproducibility.

Suggested service groups:

```text
simulation/
  sumo
  gps-producer
  sensor-producer
  warehouse-producer
  scenario-controller

messaging/
  kafka
  schema-registry (phase 2)

processing/
  flink-jobmanager
  flink-taskmanager
  stream-jobs

storage/
  postgres
  minio
  iceberg-catalog (phase 2)
  clickhouse
  trino
  redis (phase 4)

quality/
  great-expectations
  dbt

serving/
  fastapi
  superset
  grafana

platform/
  prometheus
  airflow (phase 5)
  mlflow (phase 6)
```

Lab 1 explicitly establishes Docker Compose as the local runtime and maps the local components to AWS equivalents. fileciteturn0file0L130-L180

## 17. Technology Strategy

The architecture should not force every technology into the MVP.

| Concern | MVP | Target/Extension |
|---|---|---|
| Vehicle simulation | SUMO + TraCI | same |
| Event generation | Python | same |
| Message bus | Kafka | Kafka + Schema Registry |
| Stream processing | Python/event workers or early Flink job | Apache Flink |
| Operational DB | PostgreSQL | PostgreSQL + PostGIS |
| Object storage | MinIO | MinIO + Iceberg |
| Query | PostgreSQL/DuckDB/ClickHouse | Trino + ClickHouse |
| API | FastAPI | FastAPI |
| Dashboard | Grafana/Superset | Superset + Grafana |
| IoT protocol | Kafka directly | MQTT/Mosquitto |
| CDC | direct PostgreSQL | Debezium |
| Features | optional | Redis |
| ML | optional | Spark + XGBoost/Prophet/MLflow |

This phased approach is consistent with Lab 3, which states that MQTT, CDC, Iceberg and ML are extensions after the core pipeline has been validated. fileciteturn0file2L204-L208

## 18. Observability

Required metrics:

- producer events/sec
- consumer events/sec
- Kafka consumer lag
- validation failures/sec
- duplicate events/sec
- late events/sec
- DLQ size
- stream processing latency
- alert latency
- API latency
- database write latency
- dashboard refresh latency
- checkpoint duration
- checkpoint failure count

Alert latency should be measured as:

```text
alert_latency = alert_timestamp - event_time
```

Use p50, p95 and p99, as specified in Lab 1. fileciteturn0file0L123-L125

## 19. Security and Scope Boundary

This semester prototype is not intended to provide production IAM-grade authorization or multi-AZ fault tolerance. Those are explicit out-of-scope items in the original project proposal. fileciteturn0file0L126-L129

The implementation should still avoid hard-coded secrets, expose only necessary ports, and separate development configuration from source code.

## 20. AWS Mapping

The original objective is to reproduce cloud data-engineering reasoning locally.

```text
Kafka                 → Kinesis / MSK
Schema Registry       → Glue Schema Registry
Kafka Connect         → Kinesis Firehose
Debezium              → DMS
Mosquitto             → IoT Core
Flink                 → Managed Flink
MinIO                 → S3
Iceberg               → Iceberg on S3
Spark / DuckDB        → Glue ETL / EMR SQL
Trino                 → Athena
ClickHouse            → Redshift
PostgreSQL            → RDS / location-enabled RDS
Redis                 → DynamoDB / ElastiCache
Airflow               → MWAA
Great Expectations    → Glue Data Quality
Marquez/OpenLineage   → DataZone
MLflow                 → SageMaker
Superset              → QuickSight
Prometheus/Grafana    → CloudWatch
FastAPI               → Lambda + API Gateway
SUMO/H3/OSRM          → location-oriented cloud services
Docker/Terraform      → CloudFormation/ECS
```

The mapping and tool set originate in Lab 1. fileciteturn0file0L130-L180

## 21. Architectural Acceptance Criteria

The architecture is considered implemented when the system can:

1. Start from a clean Docker Compose environment.
2. Run the Jodhpur simulation reproducibly.
3. Generate GPS, sensor and warehouse events.
4. Publish events to Kafka.
5. Preserve stable event IDs and timestamps.
6. Validate and quarantine invalid events.
7. Demonstrate duplicate and late-event handling.
8. Detect at least one cold-chain excursion.
9. Detect at least one route deviation.
10. Persist operational shipment/master data.
11. Persist historical analytical data.
12. Show live/current state in a dashboard.
13. Show historical performance analytics.
14. Replay Kafka events after a consumer failure.
15. Produce measurable event-to-alert latency.
