"""Bronze, Silver and Gold on local Parquet, queried with DuckDB.

Both are open source and need no server, which is what keeps the whole
prototype runnable on a laptop. The layout is the same one an object store
would hold, so moving Bronze to S3-compatible storage later is a path
change, not a rewrite.

  bronze/<event_type>/event_date=YYYY-MM-DD/part.parquet   immutable, as received
  silver/<table>.parquet                                   validated, deduplicated, typed
  gold/<fact or dim>.parquet                               business tables
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

BRONZE, SILVER, GOLD = "bronze", "silver", "gold"


class Lakehouse:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        for layer in (BRONZE, SILVER, GOLD):
            (self.root / layer).mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- bronze
    def write_bronze(self, rows: list[dict]) -> int:
        """Immutable landing area, partitioned by event date."""
        if not rows:
            return 0
        by_partition: dict[tuple[str, str], list[dict]] = {}
        for row in rows:
            by_partition.setdefault((row["event_type"], row["event_date"]), []).append(row)
        written = 0
        for (event_type, event_date), part in by_partition.items():
            target = self.root / BRONZE / event_type / f"event_date={event_date}"
            target.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(part)
            pq.write_table(table, target / "part-000.parquet", compression="zstd")
            written += len(part)
        return written

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
    def build_gold(self) -> dict[str, int]:
        """Star-schema business tables built only from Silver."""
        con = self.connect()
        silver = self.root / SILVER
        for table in ("silver_gps", "silver_sensor", "silver_warehouse", "silver_shipment_status"):
            con.execute(
                f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{silver / (table + '.parquet')}')")

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
        con.execute("""
            CREATE OR REPLACE TABLE fact_delivery_performance AS
            WITH milestones AS (
                SELECT shipment_id,
                       min(CASE WHEN new_status = 'DISPATCHED' THEN event_time END) AS dispatched_at,
                       min(CASE WHEN new_status = 'DELIVERED'  THEN event_time END) AS delivered_at
                FROM silver_shipment_status GROUP BY shipment_id
            )
            SELECT d.shipment_id, s.carrier_id, s.vehicle_id, s.route_id,
                   s.temperature_class, s.promised_delivery_time,
                   d.dispatched_at, d.delivered_at,
                   date_diff('second', d.dispatched_at, d.delivered_at) AS actual_duration_seconds,
                   date_diff('second', s.promised_delivery_time, d.delivered_at) AS delay_seconds,
                   CASE WHEN d.delivered_at IS NULL THEN NULL
                        WHEN d.delivered_at <= s.promised_delivery_time THEN 1 ELSE 0 END AS on_time_flag
            FROM milestones d JOIN dim_shipment s USING (shipment_id)
        """)
        con.execute("""
            CREATE OR REPLACE TABLE fact_cold_chain_excursion AS
            WITH breaches AS (
                SELECT shipment_id, sensor_id, event_time, temperature_c
                FROM silver_sensor
                WHERE temperature_class = 'COLD_2_8' AND temperature_c > 8.0
            )
            SELECT shipment_id, sensor_id,
                   min(event_time) AS start_time, max(event_time) AS end_time,
                   date_diff('second', min(event_time), max(event_time)) AS duration_seconds,
                   max(temperature_c) AS max_temperature,
                   count(*) AS breach_readings
            FROM breaches GROUP BY shipment_id, sensor_id
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

        counts: dict[str, int] = {}
        for table in ("dim_shipment", "dim_date", "fact_delivery_performance",
                      "fact_cold_chain_excursion", "fact_warehouse_dwell"):
            path = self.root / GOLD / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        con.close()
        return counts

    # ------------------------------------------------------------------ read
    def query(self, sql: str):
        con = self.connect()
        for layer in (SILVER, GOLD):
            for path in (self.root / layer).glob("*.parquet"):
                con.execute(
                    f"CREATE OR REPLACE VIEW {path.stem} AS SELECT * FROM read_parquet('{path}')")
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def size_report(self) -> dict:
        def total(layer: str) -> int:
            return sum(p.stat().st_size for p in (self.root / layer).rglob("*.parquet"))
        return {"bronze_bytes": total(BRONZE), "silver_bytes": total(SILVER),
                "gold_bytes": total(GOLD)}
