# Real-Time Supply Chain & Logistics Intelligence Platform

Group 16 · Data Engineering Laboratory · Semester 5

A locally hosted logistics intelligence platform. A controlled simulator
generates vehicle GPS, cold-chain sensor, warehouse scan and shipment
lifecycle events over a Jodhpur road network; Kafka carries them; a
validation and event-time processing layer turns them into cold-chain and
route-deviation alerts; PostgreSQL holds the operational state; a
Bronze/Silver/Gold lakehouse turns them into delivery analytics; and models
trained on five weeks of simulated history predict delivery delay, find
reefer and dwell anomalies the fixed rules miss, and forecast demand.

Everything is open source and runs on a laptop.

---

## Set up the environment

The project uses a conda environment kept inside the repository at `./venv`,
so the interpreter and its packages travel with the project rather than
polluting a global install.

```bash
cd logistics_intelligence_
conda create -p venv python=3.10        # or: make venv
conda activate ./venv
pip install -r requirements.txt         # or: make install
```

`make setup` does the create and the install in one step. The environment
must still be activated by you, because a Makefile cannot activate a shell
for its caller:

```bash
make setup
conda activate ./venv
```

The Makefile picks up `./venv/bin/python` automatically when that file
exists, so `make demo` and `make test` work even in a shell where you have
not activated the environment. `make which-python` prints the interpreter
that will actually be used, which is the first thing to check if something
imports in one shell and not another.

Only the core packages are in `requirements.txt`. The Kafka and PostgreSQL
client libraries are in `requirements-kafka.txt` and are needed only for the
Docker path:

```bash
make install-kafka
```

## Run it in one command

No broker, no database, no Docker needed for the demo path:

```bash
make demo                       # one scenario, end to end
```

That simulates an hour of logistics operations, publishes every event
through the bus, validates and deduplicates them, detects excursions and
deviations, lands Bronze, builds Silver and Gold, and prints a report:

```
scenario            COLD_CHAIN_EXCURSION   seed 42   3.12s
----------------------------------------------------------------
published           40206
consumed            40206
valid               39794
quarantined         412  {'GPS_LATITUDE_RANGE': 359, 'SENSOR_HUMIDITY_RANGE': 53}
duplicates dropped  0
on time             39794
late, side output   0
cold-chain alerts   7  (spikes suppressed: 5)
deviation alerts    0
replay count match  True
OTIF                77.8%  median delay -11.8 min  p95 10.2 min
```

Other entry points:

```bash
make scenarios      # run all seven fault scenarios and compare the outcomes
make test           # 57 tests; the PostgreSQL ones skip without a database,
                    # the broker one runs with: make test-kafka
make api            # dashboard and API on http://127.0.0.1:8000
make demo SCENARIO=ROUTE_DEVIATION SEED=7
```

## Five weeks of history and the models

```bash
make history        # 35 days, ~3,900 shipments, ~200,000 events, same pipeline
make ml             # train, evaluate (rolling origin) and register the models
```

The history has demand that follows the hour and the weekday, peak-hour
congestion, carriers of different reliability, warehouse backlog, transit
incidents and a few injected faults whose ground truth goes to
`data/history/labels.json`, which is opened only to score. The models read
Gold only. Results from the group laptop (seed 42, `docs/ml_report.json`):

```
delay at dispatch   rolling origin over 14 days, 1590 shipments, 9.7% late
  model                 MAE min    RMSE    AUC   prec  recall
  promise                  18.9   23.12    0.5      -     0.0
  static_eta               6.76   10.23  0.747   0.87   0.214
  carrier_hour_mean        9.72   17.86  0.674   0.00     0.0
  linear                   6.72    9.66  0.811   0.74   0.221
  gbt                      5.71    8.69  0.821   0.64   0.305

reefer trips        481 test trips, model flags vs the 8 C rule
  drift (under 8 C)          19 trips   rule   0   model  16
  excursion (over 8 C)        6 trips   rule   6   model   6
  single spikes only         71 trips   rule   0   model   3
  normal                    385 trips   rule   0   model   0

demand, day ahead   7 days, 1.39 orders per warehouse-hour
  gbt_poisson            hourly WAPE 0.686  daily WAPE 0.196
  true_rate (floor)      hourly WAPE 0.651  daily WAPE 0.162

online vs batch     3887 of 3890 dispatches identical; differing fields {'origin_orders_2h': 3}
```

For dwell, a fixed 60-minute threshold already finds all 29 held shipments
with no false alarm; the per-warehouse robust score finds the same 29 plus 3
false alarms, so the rule stays. Models are versioned in `data/models/`
by a hash of feature definition, training window, data fingerprint and
parameters (ADR 006).

## Run it against the real stack

```bash
make up             # Apache Kafka (KRaft) + PostgreSQL, pinned images
make install-kafka  # confluent-kafka and psycopg
make topics         # create topics with their keys and 7-day retention
make db-migrate     # apply the SQL migrations
make demo-kafka     # same pipeline, real broker
make demo-postgres  # offline pipeline that also writes the operational store
make stream         # the pipeline as a long-running consumer (checkpoints, Bronze, Postgres)
make produce RATE=2000   # in another terminal: publish a scenario at 2,000 events/s
make bench          # latency, capacity and crash recovery, written to data/bench/
make down           # stop; 'make reset' also wipes the volumes
```

The stream consumer saves its detector state and the offsets it belongs to
in one checkpoint each second; after a crash it restores both, so its counts
are exactly those of an uninterrupted run, and alerts repeated after the
restart carry the same id (ADR 007). Measured on the group laptop (Kafka 3.9
single node, 4-vCPU Linux VM, `docs/bench_report.json`):

| Input rate (events/s) | 1,000 | 4,000 | 16,000 | 32,000 |
|---|---|---|---|---|
| Event latency p50 / p99 (ms) | 10.8 / 35.5 | 11.0 / 32.7 | 13.2 / 41.2 | 16.0 / 40.2 |
| Alert latency p50 (ms) | 17.1 | 18.4 | 17.9 | 21.2 |

One consumer drains 51,600 events/s. Killed with SIGKILL at 2,000 events/s,
it was processing again 0.22 s later, with the same final counts and the
same 12 alerts as the offline run; 7 alert messages were repeated, with the
same ids.

`make reset` exists because wiping a Kafka volume without wiping the stored
cluster id leaves a broker that will not start. The target removes both
together.

## What is in the box

```
src/lil/
  config.py            YAML + environment overrides, validated at start-up
  envelope.py          the event envelope: event_id, event_time, ingest_time,
                       source, schema_version; UTC enforced, naive times rejected
  bus.py               Kafka backend and a file-backed backend with the same
                       semantics (ordering, retention, replay, consumer groups)
  sim/network.py       Jodhpur road graph, Dijkstra routing, corridor distance
  sim/world.py         vehicles, sensors, warehouse state machine, lifecycle
  sim/scenarios.py     seven reproducible fault scenarios
  validation/rules.py  structural then domain checks, reason codes, quarantine
  processing/dedup.py       deduplication on event_id inside a window
  processing/watermark.py   watermark, allowed lateness, late side output
  processing/coldchain.py   sustained excursion detection, spike suppression
  processing/deviation.py   corridor departure with a persistence threshold
  processing/pipeline.py    validate -> dedup -> lateness -> detect -> land
  sim/history.py       five weeks of operations with known injected faults
  storage/lakehouse.py Bronze/Silver/Gold as Parquet, queried with DuckDB
  storage/operational.py  idempotent PostgreSQL writer
  analytics/kpis.py    OTIF, delay, dwell and excursion definitions
  ml/features.py       delay features, one definition for batch and online
  ml/delay.py          delay at dispatch: baselines, linear, gradient boosting
  ml/anomaly.py        reefer trips (isolation forest) and dwell (median/MAD)
  ml/forecast.py       day-ahead demand per warehouse and hour
  ml/online.py         the delay model inside the stream, and the parity check
  ml/registry.py       versioned models with their data fingerprint
  stream.py            long-running Kafka consumer with checkpoints
  bench.py             latency, capacity and recovery on the broker
  api/app.py           FastAPI serving layer over Gold and the model outputs
  runner.py            one end-to-end run, used by the demo and the tests
db/migrations/         PostgreSQL schema: master data, append-only history,
                       alerts, quarantine, late events, run provenance
schemas/json/          the event contracts
dashboard/index.html   live operations, alerts, carriers, data quality, models
docs/                  architecture, PRD, phases, design, eight ADRs and the
                       evidence files the final report quotes
tests/                 57 tests
```

## The rules the system actually enforces

| Rule | Where | Value |
|---|---|---|
| Cold-chain alert | `processing/coldchain.py` | above 8 °C for 2 consecutive readings **or** 15 minutes sustained, for a 2–8 °C shipment |
| Route deviation | `processing/deviation.py` | outside a 250 m corridor for 60 seconds |
| Watermark | `processing/watermark.py` | 60 s bounded out-of-orderness |
| Allowed lateness | `processing/watermark.py` | 5 minutes, then the late side output |
| Deduplication | `processing/dedup.py` | on `event_id`, within a 1 hour window |
| Retention | `compose.yml`, `scripts/create_topics.sh` | 7 days on raw topics |
| Checkpoint | `stream.py` | detector state + offsets together, every second |
| Delay-risk alert | `ml/online.py` | P(late) at dispatch of at least 0.5 |

Every one of these lives in `config/config.yaml`. Nothing in `src/` hard-codes
an endpoint or a threshold.

## The seven scenarios

| Scenario | What it injects | What should happen |
|---|---|---|
| `NORMAL_DELIVERY` | nothing | no alerts at all |
| `GPS_DROPOUT` | a dead zone, then a burst of stale fixes | late but within tolerance |
| `DUPLICATE_EVENT` | the same event id twice | counted once, no repeat alerts |
| `LATE_EVENT` | events delayed past the window | routed to the late side output |
| `ROUTE_DEVIATION` | vehicles leaving the corridor | one deviation alert each |
| `WAREHOUSE_DWELL_DELAY` | extra time parked | dwell visible in Gold |
| `COLD_CHAIN_EXCURSION` | a sustained breach | one alert per episode |

Every run also injects a small rate of impossible values (latitude 200,
humidity 150) so the quarantine path is exercised in all of them.

## Technology and why

| Concern | Choice | Licence | Why |
|---|---|---|---|
| Event log | Apache Kafka 3.9, KRaft | Apache-2.0 | the course target; no ZooKeeper to run |
| Operational store | PostgreSQL 16 | PostgreSQL | transactional status history |
| Lakehouse | Parquet + DuckDB | Apache-2.0 / MIT | no object-store service to operate; see ADR 004 |
| Simulation | built-in road graph | this repo | runs with nothing installed; SUMO is a drop-in backend |
| Serving | FastAPI + static HTML | MIT | one dependency, no BI server |
| Models | scikit-learn | BSD-3 | gradient boosting and isolation forest without another native dependency |

## Limitations we state rather than hide

* The data is simulated. The pipeline is real; the carrier integration is not.
* Stream processing is Python, not Flink. The semantics implemented are the
  ones that matter here: event time, watermarks, lateness, deduplication and
  idempotent alerting.
* Iceberg snapshots and time travel are not implemented; ADR 004 says why.
* One consumer process. Scaling out needs keyed state per partition, which
  is what Flink provides; the checkpoint design is the same idea (ADR 007).
* The watermark is global, so replaying far faster than real time makes
  partition skew show up as extra late events (369 at 4,000 events/s, 712 at
  32,000). Detection counts are unaffected because detectors are keyed and use
  event time; a per-partition watermark would remove the effect.
* The models learn the simulator's world. About a third of late deliveries
  in the test window came from en-route incidents that nothing known at
  dispatch can predict, which caps recall.
* No Avro registry, CDC, MQTT, Airflow or dbt. No production IAM, no
  multi-AZ, no autoscaling. Out of scope from Lab 1.
