# Real-Time Supply Chain & Logistics Intelligence Platform

Group 16 · Data Engineering Laboratory · Semester 5

A locally hosted logistics intelligence platform. A controlled simulator
generates vehicle GPS, cold-chain sensor, warehouse scan and shipment
lifecycle events over a Jodhpur road network; Kafka carries them; a
validation and event-time processing layer turns them into cold-chain and
route-deviation alerts; and a Bronze/Silver/Gold lakehouse turns them into
delivery analytics.

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
scenario            COLD_CHAIN_EXCURSION   seed 42   7.0s
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
make test           # 38 tests: unit, detector and end-to-end scenario tests
make api            # dashboard and API on http://127.0.0.1:8000
make demo SCENARIO=ROUTE_DEVIATION SEED=7
```

## Run it against the real stack

```bash
make up             # Apache Kafka (KRaft) + PostgreSQL, pinned images
make topics         # create topics with their keys and 7-day retention
make db-migrate     # apply the SQL migrations
make demo-kafka     # same pipeline, real broker
make down           # stop; 'make reset' also wipes the volumes
```

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
  storage/lakehouse.py Bronze/Silver/Gold as Parquet, queried with DuckDB
  analytics/kpis.py    OTIF, delay, dwell and excursion definitions
  api/app.py           FastAPI serving layer over Gold
  runner.py            one end-to-end run, used by the demo and the tests
db/migrations/         PostgreSQL schema: master data, append-only history,
                       alerts, quarantine, late events
schemas/json/          the event contracts
dashboard/index.html   live operations, alerts, carriers, data quality
docs/                  architecture, PRD, phases, design and five ADRs
tests/                 38 tests
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

## Limitations we state rather than hide

* The data is simulated. The pipeline is real; the carrier integration is not.
* Stream processing is Python, not Flink. The semantics implemented are the
  ones that matter here: event time, watermarks, lateness, deduplication and
  idempotent alerting.
* Iceberg snapshots and time travel are not implemented; ADR 004 says why.
* No production IAM, no multi-AZ, no autoscaling. Out of scope from Lab 1.
