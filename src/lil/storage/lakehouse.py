"""Bronze, Silver and Gold on local Parquet, queried with DuckDB.

Both are open source and need no server, which is what keeps the whole
prototype runnable on a laptop. The layout is the same one an object store
would hold, so moving Bronze to S3-compatible storage later is a path
change, not a rewrite.

  bronze/<event_type>/event_date=YYYY-MM-DD/part-*.parquet  immutable, as received
  silver/<table>.parquet                                    validated, deduplicated, typed
  silver/ref_<table>.parquet                                master data snapshot
  gold/<fact or dim>.parquet                                business tables
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

BRONZE, SILVER, GOLD = "bronze", "silver", "gold"

ALERT_SCHEMA = pa.schema([
    ("alert_id", pa.string()), ("alert_type", pa.string()), ("rule_id", pa.string()),
    ("severity", pa.string()), ("shipment_id", pa.string()), ("vehicle_id", pa.string()),
    ("sensor_id", pa.string()), ("route_id", pa.string()),
    ("start_event_time", pa.string()), ("trigger_event_id", pa.string()),
    ("trigger_event_time", pa.string()), ("max_temperature", pa.float64()),
    ("deviation_metres", pa.float64()), ("probability", pa.float64()),
    ("explanation", pa.string()),
])

# Reference tables, so Gold can be built when a run has no master snapshot.
_EMPTY_REFS = {
    "ref_carrier": "carrier_id VARCHAR, carrier_code VARCHAR, carrier_name VARCHAR",
    "ref_warehouse": "warehouse_id VARCHAR, node VARCHAR, latitude DOUBLE, longitude DOUBLE",
    "ref_route": ("route_id VARCHAR, origin_warehouse_id VARCHAR, destination_warehouse_id VARCHAR, "
                  "length_km DOUBLE, stops INTEGER, nodes VARCHAR"),
    "ref_vehicle": ("vehicle_id VARCHAR, carrier_id VARCHAR, registration_no VARCHAR, "
                    "refrigeration_capable BOOLEAN"),
}

GOLD_TABLES = ("dim_shipment", "dim_date", "dim_carrier", "dim_warehouse", "dim_route",
               "dim_vehicle", "fact_delivery_performance", "fact_cold_chain_excursion",
               "fact_warehouse_dwell", "fact_hourly_demand", "fact_sensor_trip", "fact_alert")


class Lakehouse:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        for layer in (BRONZE, SILVER, GOLD):
            (self.root / layer).mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- bronze
    def write_bronze(self, rows: list[dict], part: str = "000") -> int:
        """Immutable landing area, partitioned by event date.

        The batch runner writes one part per partition. The streaming sink
        writes a new numbered part per micro-batch, so nothing is rewritten.
        """
        if not rows:
            return 0
        by_partition: dict[tuple[str, str], list[dict]] = {}
        for row in rows:
            by_partition.setdefault((row["event_type"], row["event_date"]), []).append(row)
        written = 0
        for (event_type, event_date), batch in by_partition.items():
            target = self.root / BRONZE / event_type / f"event_date={event_date}"
            target.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(batch)
            pq.write_table(table, target / f"part-{part}.parquet", compression="zstd")
            written += len(batch)
        return written

    # -------------------------------------------------------------- reference
    def write_reference(self, master) -> dict[str, int]:
        """Snapshot of the operational master data the facts refer to.

        In the Docker stack this comes from PostgreSQL; here it comes from the
        same objects the simulator registered there.
        """
        from ..sim.network import NODES  # local import keeps storage free of sim at load time
        tables = {
            "ref_carrier": [{"carrier_id": c.carrier_id, "carrier_code": c.carrier_code,
                             "carrier_name": c.carrier_name} for c in master.carriers],
            "ref_warehouse": [{"warehouse_id": w, "node": node,
                               "latitude": NODES[node][0], "longitude": NODES[node][1]}
                              for w, node in sorted(master.warehouses.items())],
            "ref_route": [{"route_id": r.route_id, "origin_warehouse_id": r.origin_warehouse_id,
                           "destination_warehouse_id": r.destination_warehouse_id,
                           "length_km": round(r.length_m() / 1000.0, 3),
                           "stops": len(r.nodes), "nodes": ">".join(r.nodes)}
                          for r in master.routes],
            "ref_vehicle": [{"vehicle_id": v.vehicle_id, "carrier_id": v.carrier_id,
                             "registration_no": v.registration_no,
                             "refrigeration_capable": v.refrigeration_capable}
                            for v in master.vehicles],
        }
        counts = {}
        for name, rows in tables.items():
            pq.write_table(pa.Table.from_pylist(rows), self.root / SILVER / f"{name}.parquet",
                           compression="zstd")
            counts[name] = len(rows)
        return counts

    def write_alerts(self, alerts: Iterable[dict]) -> int:
        """Alerts are the stream processor's output topic, kept typed in Silver.

        One row per alert_id: a replay that re-emits an alert does not add a row.
        """
        seen, rows = set(), []
        for alert in alerts:
            if alert["alert_id"] in seen:
                continue
            seen.add(alert["alert_id"])
            rows.append({name: alert.get(name) for name in ALERT_SCHEMA.names})
        table = pa.Table.from_pylist(rows, schema=ALERT_SCHEMA)
        pq.write_table(table, self.root / SILVER / "silver_alert.parquet", compression="zstd")
        return len(rows)

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect()

    def _bronze_glob(self) -> str:
        return str(self.root / BRONZE / "**" / "*.parquet")

    # ----------------------------------------------------------------- silver
    def build_silver(self) -> dict[str, int]:
        """Standardise, deduplicate and split the raw stream into clean tables."""
        con = self.connect()
        con.execute(f"""
            CREATE OR REPLACE VIEW bronze AS
            SELECT * FROM read_parquet('{self._bronze_glob()}', union_by_name=true)
        """)
        counts: dict[str, int] = {}

        con.execute("""
            CREATE OR REPLACE TABLE silver_gps AS
            SELECT event_id, CAST(event_time AS TIMESTAMP) AS event_time,
                   CAST(ingest_time AS TIMESTAMP) AS ingest_time,
                   vehicle_id, shipment_id, route_id,
                   latitude, longitude, speed_kph, lateness_class, source, schema_version
            FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) AS rn
                  FROM bronze WHERE event_type = 'gps.telemetry')
            WHERE rn = 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE silver_sensor AS
            SELECT event_id, CAST(event_time AS TIMESTAMP) AS event_time,
                   CAST(ingest_time AS TIMESTAMP) AS ingest_time,
                   sensor_id, shipment_id, vehicle_id,
                   temperature_c, humidity_pct, battery_pct, temperature_class,
                   lateness_class, source, schema_version
            FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) AS rn
                  FROM bronze WHERE event_type = 'sensor.reading')
            WHERE rn = 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE silver_warehouse AS
            SELECT event_id, CAST(event_time AS TIMESTAMP) AS event_time,
                   shipment_id, warehouse_id, event_type, source
            FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) AS rn
                  FROM bronze WHERE event_type LIKE 'warehouse.%')
            WHERE rn = 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE silver_shipment_status AS
            SELECT event_id, CAST(event_time AS TIMESTAMP) AS event_time,
                   shipment_id, vehicle_id, route_id, carrier_id,
                   old_status, new_status, temperature_class,
                   CAST(promised_delivery_time AS TIMESTAMP) AS promised_delivery_time
            FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) AS rn
                  FROM bronze WHERE event_type IN ('shipment.created', 'shipment.status_changed'))
            WHERE rn = 1
        """)

        for table in ("silver_gps", "silver_sensor", "silver_warehouse", "silver_shipment_status"):
            path = self.root / SILVER / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        con.close()
        return counts

    # ------------------------------------------------------------------ gold
    def _silver_views(self, con) -> None:
        silver = self.root / SILVER
        for table in ("silver_gps", "silver_sensor", "silver_warehouse", "silver_shipment_status"):
            con.execute(
                f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{silver / (table + '.parquet')}')")
        for table, columns in _EMPTY_REFS.items():
            path = silver / f"{table}.parquet"
            if path.exists():
                con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
            else:
                con.execute(f"CREATE OR REPLACE TABLE {table} ({columns})")
        alerts = silver / "silver_alert.parquet"
        if alerts.exists():
            con.execute(f"CREATE OR REPLACE VIEW silver_alert AS SELECT * FROM read_parquet('{alerts}')")
        else:
            columns = ", ".join(f"{f.name} {'DOUBLE' if pa.types.is_floating(f.type) else 'VARCHAR'}"
                                for f in ALERT_SCHEMA)
            con.execute(f"CREATE OR REPLACE TABLE silver_alert ({columns})")

    def build_gold(self) -> dict[str, int]:
        """Star-schema business tables built only from Silver."""
        con = self.connect()
        self._silver_views(con)

        con.execute("""
            CREATE OR REPLACE TABLE dim_shipment AS
            SELECT shipment_id,
                   any_value(carrier_id)         AS carrier_id,
                   any_value(vehicle_id)         AS vehicle_id,
                   any_value(route_id)           AS route_id,
                   any_value(temperature_class)  AS temperature_class,
                   min(promised_delivery_time)   AS promised_delivery_time
            FROM silver_shipment_status GROUP BY shipment_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE dim_date AS
            SELECT DISTINCT CAST(event_time AS DATE) AS date_key,
                   year(event_time) AS year, quarter(event_time) AS quarter,
                   month(event_time) AS month, dayofweek(event_time) AS day_of_week
            FROM silver_shipment_status
        """)
        con.execute("CREATE OR REPLACE TABLE dim_carrier AS SELECT * FROM ref_carrier")
        con.execute("CREATE OR REPLACE TABLE dim_warehouse AS SELECT * FROM ref_warehouse")
        con.execute("CREATE OR REPLACE TABLE dim_route AS SELECT * FROM ref_route")
        con.execute("CREATE OR REPLACE TABLE dim_vehicle AS SELECT * FROM ref_vehicle")
        con.execute("""
            CREATE OR REPLACE TABLE fact_delivery_performance AS
            WITH milestones AS (
                SELECT shipment_id,
                       min(CASE WHEN new_status = 'DISPATCHED' THEN event_time END) AS dispatched_at,
                       min(CASE WHEN new_status = 'DELIVERED'  THEN event_time END) AS delivered_at,
                       min(CASE WHEN new_status = 'CREATED'    THEN event_time END) AS created_at
                FROM silver_shipment_status GROUP BY shipment_id
            )
            SELECT d.shipment_id, s.carrier_id, s.vehicle_id, s.route_id,
                   s.temperature_class, s.promised_delivery_time,
                   d.dispatched_at, d.delivered_at,
                   date_diff('second', d.dispatched_at, d.delivered_at) AS actual_duration_seconds,
                   date_diff('second', s.promised_delivery_time, d.delivered_at) AS delay_seconds,
                   CASE WHEN d.delivered_at IS NULL THEN NULL
                        WHEN d.delivered_at <= s.promised_delivery_time THEN 1 ELSE 0 END AS on_time_flag,
                   d.created_at,
                   date_diff('second', d.created_at, d.dispatched_at) AS processing_seconds,
                   r.origin_warehouse_id, r.destination_warehouse_id,
                   r.length_km AS route_length_km
            FROM milestones d JOIN dim_shipment s USING (shipment_id)
            LEFT JOIN dim_route r ON r.route_id = s.route_id
        """)
        # An excursion is a run of consecutive readings above the limit, the
        # same definition the stream rule uses. Counting any two breaches in a
        # trip would turn two unrelated spikes into an excursion.
        con.execute("""
            CREATE OR REPLACE TABLE fact_cold_chain_excursion AS
            WITH ordered AS (
                SELECT shipment_id, sensor_id, event_time, temperature_c,
                       row_number() OVER (PARTITION BY shipment_id, sensor_id
                                          ORDER BY event_time) AS rn
                FROM silver_sensor WHERE temperature_class = 'COLD_2_8'
            ),
            breaches AS (
                SELECT *, rn - row_number() OVER (PARTITION BY shipment_id, sensor_id
                                                  ORDER BY event_time) AS episode
                FROM ordered WHERE temperature_c > 8.0
            )
            SELECT shipment_id, sensor_id,
                   min(event_time) AS start_time, max(event_time) AS end_time,
                   date_diff('second', min(event_time), max(event_time)) AS duration_seconds,
                   max(temperature_c) AS max_temperature,
                   count(*) AS breach_readings
            FROM breaches GROUP BY shipment_id, sensor_id, episode
            HAVING count(*) >= 2
        """)
        con.execute("""
            CREATE OR REPLACE TABLE fact_warehouse_dwell AS
            SELECT shipment_id,
                   min(CASE WHEN event_type = 'warehouse.inbound'  THEN event_time END) AS arrived_at,
                   min(CASE WHEN event_type = 'warehouse.dispatch' THEN event_time END) AS departed_at,
                   date_diff('second',
                             min(CASE WHEN event_type = 'warehouse.inbound'  THEN event_time END),
                             min(CASE WHEN event_type = 'warehouse.dispatch' THEN event_time END))
                     AS dwell_seconds
            FROM silver_warehouse GROUP BY shipment_id
        """)
        # Orders created per origin warehouse per local (IST) hour, with the
        # empty hours filled in: a forecast trained without the zeros learns the
        # wrong level. IST is UTC+5:30, so UTC hour buckets would split every
        # local hour in two.
        con.execute("""
            CREATE OR REPLACE TABLE fact_hourly_demand AS
            WITH created AS (
                SELECT s.shipment_id,
                       date_trunc('hour', s.event_time + INTERVAL 330 MINUTE) AS hour_ist,
                       r.origin_warehouse_id AS warehouse_id
                FROM silver_shipment_status s JOIN dim_route r ON r.route_id = s.route_id
                WHERE s.new_status = 'CREATED'
            ),
            bounds AS (SELECT min(hour_ist) AS lo, max(hour_ist) AS hi FROM created),
            hours AS (SELECT unnest(generate_series(lo, hi, INTERVAL 1 HOUR)) AS hour_ist
                      FROM bounds WHERE lo IS NOT NULL)
            SELECT w.warehouse_id, h.hour_ist,
                   h.hour_ist - INTERVAL 330 MINUTE AS hour_start_utc,
                   count(c.shipment_id) AS orders
            FROM dim_warehouse w CROSS JOIN hours h
            LEFT JOIN created c ON c.warehouse_id = w.warehouse_id AND c.hour_ist = h.hour_ist
            GROUP BY w.warehouse_id, h.hour_ist
            ORDER BY w.warehouse_id, h.hour_ist
        """)
        # One row per refrigerated trip: the summary the anomaly scorer reads.
        con.execute("""
            CREATE OR REPLACE TABLE fact_sensor_trip AS
            WITH r AS (
                SELECT shipment_id, sensor_id, event_time, temperature_c, humidity_pct, battery_pct,
                       row_number() OVER (PARTITION BY shipment_id ORDER BY event_time) AS rn,
                       count(*) OVER (PARTITION BY shipment_id) AS n
                FROM silver_sensor WHERE temperature_class = 'COLD_2_8'
            )
            SELECT shipment_id, any_value(sensor_id) AS sensor_id, count(*) AS readings,
                   min(event_time) AS first_reading_at, max(event_time) AS last_reading_at,
                   round(avg(temperature_c), 3) AS mean_c,
                   max(temperature_c) AS max_c, min(temperature_c) AS min_c,
                   round(coalesce(stddev_samp(temperature_c), 0), 3) AS std_c,
                   round(quantile_cont(temperature_c, 0.5), 3) AS median_c,
                   round(quantile_cont(temperature_c, 0.9), 3) AS p90_c,
                   round(coalesce(regr_slope(temperature_c, epoch(event_time) / 3600.0), 0), 3)
                       AS slope_c_per_h,
                   round(avg(CASE WHEN rn <= 3 THEN temperature_c END), 3) AS start_c,
                   round(avg(CASE WHEN rn > n - 3 THEN temperature_c END), 3) AS end_c,
                   sum(CASE WHEN temperature_c > 8 THEN 1 ELSE 0 END) AS readings_above_8,
                   sum(CASE WHEN temperature_c > 7 THEN 1 ELSE 0 END) AS readings_above_7,
                   round(avg(humidity_pct), 2) AS mean_humidity,
                   round(max(battery_pct) - min(battery_pct), 2) AS battery_drop
            FROM r GROUP BY shipment_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE fact_alert AS
            SELECT alert_id, alert_type, rule_id, severity, shipment_id, vehicle_id, sensor_id,
                   route_id,
                   CAST(start_event_time AS TIMESTAMP)   AS start_event_time,
                   CAST(trigger_event_time AS TIMESTAMP) AS trigger_event_time,
                   CAST(CAST(start_event_time AS TIMESTAMP) AS DATE) AS date_key,
                   max_temperature, deviation_metres, probability
            FROM silver_alert
        """)

        counts: dict[str, int] = {}
        for table in GOLD_TABLES:
            path = self.root / GOLD / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        con.close()
        return counts

    # ------------------------------------------------------------------ read
    def query(self, sql: str, params: list | None = None):
        con = self.connect()
        for layer in (SILVER, GOLD):
            for path in (self.root / layer).glob("*.parquet"):
                con.execute(
                    f"CREATE OR REPLACE VIEW {path.stem} AS SELECT * FROM read_parquet('{path}')")
        try:
            return con.execute(sql, params or []).fetchall()
        finally:
            con.close()

    def frame(self, sql: str, params: list | None = None):
        """Same as query, returned as a pandas DataFrame for the models."""
        con = self.connect()
        for layer in (SILVER, GOLD):
            for path in (self.root / layer).glob("*.parquet"):
                con.execute(
                    f"CREATE OR REPLACE VIEW {path.stem} AS SELECT * FROM read_parquet('{path}')")
        try:
            return con.execute(sql, params or []).df()
        finally:
            con.close()

    def size_report(self) -> dict:
        def total(layer: str) -> int:
            return sum(p.stat().st_size for p in (self.root / layer).rglob("*.parquet"))
        return {"bronze_bytes": total(BRONZE), "silver_bytes": total(SILVER),
                "gold_bytes": total(GOLD)}
