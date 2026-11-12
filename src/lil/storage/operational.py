"""PostgreSQL operational store.

The lakehouse answers questions about history. PostgreSQL holds the current
operational truth that operators act on: master data, the append-only status
history, the current status, open alerts, quarantine and late events (see
db/migrations). Every write is idempotent, so a replayed topic or a restarted
consumer cannot add a second row for the same event:

    shipment_status_history   unique on event_id
    alert                     primary key alert_id
    quarantine_record         unique on (event_id, arrival_time)
    late_event                unique on event_id
    master data               first writer wins

The current status on ``shipment`` only moves forward in event time, so an
older event that arrives late is recorded in history but does not overwrite
a newer status.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from ..config import REPO_ROOT

MIGRATIONS = REPO_ROOT / "db" / "migrations"


def _ts(value):
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class OperationalStore:
    def __init__(self, conninfo: str, schema: str | None = None):
        try:
            import psycopg  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("psycopg is not installed; run 'make install-kafka'") from exc
        self.conn = psycopg.connect(conninfo)
        if schema:                     # tests use their own schema and drop it afterwards
            with self.conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(f'SET search_path TO "{schema}"')
            self.conn.commit()

    @classmethod
    def from_config(cls, cfg, schema: str | None = None) -> "OperationalStore":
        pg = cfg.get_path("postgres")
        return cls(f"host={pg['host']} port={pg['port']} dbname={pg['database']} "
                   f"user={pg['user']} password={pg['password']} connect_timeout=3", schema)

    def __enter__(self):
        return self

    def __exit__(self, kind, value, trace):
        if kind is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

    # --------------------------------------------------------------- schema
    def migrate(self, folder: Path = MIGRATIONS) -> list[str]:
        applied = []
        for path in sorted(folder.glob("*.sql")):
            with self.conn.cursor() as cur:
                cur.execute(path.read_text())
            applied.append(path.name)
        self.conn.commit()
        return applied

    # ---------------------------------------------------------- master data
    def upsert_reference(self, master) -> None:
        """Carriers, warehouses, vehicles, routes and sensors. First writer wins."""
        from ..sim.network import NODES

        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO carrier (carrier_id, carrier_code, carrier_name, carrier_type) "
                "VALUES (%s, %s, %s, 'ROAD') ON CONFLICT DO NOTHING",
                [(c.carrier_id, c.carrier_code, c.carrier_name) for c in master.carriers])
            cur.executemany(
                "INSERT INTO warehouse (warehouse_id, warehouse_code, warehouse_name, latitude, longitude) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [(w, w.upper().replace("-", ""), node.replace("_", " ").title(),
                  NODES[node][0], NODES[node][1]) for w, node in sorted(master.warehouses.items())])
            cur.executemany(
                "INSERT INTO vehicle (vehicle_id, carrier_id, registration_no, vehicle_type, "
                "refrigeration_capable) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [(v.vehicle_id, v.carrier_id, v.registration_no,
                  "reefer_truck" if v.refrigeration_capable else "light_truck",
                  v.refrigeration_capable) for v in master.vehicles])
            cur.executemany(
                "INSERT INTO route (route_id, route_name, origin_warehouse_id, destination_warehouse_id, "
                "geometry, planned_distance_km) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [(r.route_id, f"{r.origin_warehouse_id} to {r.destination_warehouse_id}",
                  r.origin_warehouse_id, r.destination_warehouse_id,
                  json.dumps({"type": "LineString",
                              "coordinates": [[lon, lat] for lat, lon in r.points]}),
                  round(r.length_m() / 1000.0, 3)) for r in master.routes])
            cur.executemany(
                "INSERT INTO sensor (sensor_id, sensor_type, model, serial_number) "
                "VALUES (%s, %s, 'reefer probe', %s) ON CONFLICT DO NOTHING",
                [(s.sensor_id, s.sensor_type, s.serial_number) for s in master.sensors])

    def upsert_shipments(self, status_rows: list[dict], master) -> None:
        """Order and shipment rows from lifecycle events, before their history.

        Any status event can create the row, because in a stream the CREATED
        event may arrive after a later one. The order's placed_at converges on
        the earliest event time seen for it.
        """
        lookup = {s.shipment_id: s for s in master.shipments}
        routes = {r.route_id: r for r in master.routes}
        first: dict[str, dict] = {}
        for row in status_rows:
            seen = first.get(row["shipment_id"])
            if seen is None or _ts(row["event_time"]) < _ts(seen["event_time"]):
                first[row["shipment_id"]] = row
        known = [(lookup[sid], row) for sid, row in first.items() if sid in lookup]
        with self.conn.cursor() as cur:
            cur.executemany(
                'INSERT INTO "order" (order_id, placed_at) VALUES (%s, %s) '
                'ON CONFLICT (order_id) DO UPDATE SET placed_at = LEAST("order".placed_at, EXCLUDED.placed_at)',
                [(s.order_id, _ts(row["event_time"])) for s, row in known])
            cur.executemany(
                "INSERT INTO shipment (shipment_id, order_id, carrier_id, vehicle_id, route_id, "
                "temperature_class, promised_delivery_time, origin_warehouse_id, "
                "destination_warehouse_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                [(s.shipment_id, s.order_id, s.carrier_id, s.vehicle_id, s.route_id,
                  s.temperature_class, s.promised_delivery_time,
                  routes[s.route_id].origin_warehouse_id,
                  routes[s.route_id].destination_warehouse_id) for s, _ in known])
            cur.executemany(
                "INSERT INTO sensor_assignment (sensor_id, vehicle_id, shipment_id, valid_from, valid_to) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [(s.sensor_id, s.vehicle_id, s.shipment_id,
                  s.dispatched_at or _ts(row["event_time"]), s.delivered_at)
                 for s, row in known if s.sensor_id])

    # --------------------------------------------------------------- events
    def apply_status_events(self, rows: Iterable[dict], master) -> int:
        """Append to history (idempotent), then move current status forward."""
        rows = [r for r in rows if r.get("new_status")]
        if not rows:
            return 0
        self.upsert_shipments(rows, master)
        records = [(r["shipment_id"], r.get("old_status"), r["new_status"], _ts(r["event_time"]),
                    r.get("source") or "pipeline", r["event_id"]) for r in rows]
        with self.conn.cursor() as cur:
            before = self._count(cur, "shipment_status_history")
            cur.executemany(
                "INSERT INTO shipment_status_history (shipment_id, old_status, new_status, "
                "event_time, source, event_id) VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (event_id) DO NOTHING", records)
            cur.execute("""
                UPDATE shipment s
                SET status = h.new_status, status_event_time = h.event_time, updated_at = now(),
                    actual_delivery_time = CASE WHEN h.new_status = 'DELIVERED'
                                                THEN h.event_time ELSE s.actual_delivery_time END
                FROM (SELECT DISTINCT ON (shipment_id) shipment_id, new_status, event_time
                      FROM shipment_status_history
                      ORDER BY shipment_id, event_time DESC, status_event_id DESC) h
                WHERE s.shipment_id = h.shipment_id
                  AND (s.status_event_time IS NULL OR h.event_time > s.status_event_time)
            """)
            return self._count(cur, "shipment_status_history") - before

    def write_alerts(self, alerts: Iterable[dict]) -> int:
        records = [(a["alert_id"], a.get("alert_type", "unknown"), a.get("severity", "WARNING"),
                    a.get("shipment_id") or None, a.get("vehicle_id") or None, a["rule_id"],
                    a["trigger_event_id"], _ts(a["start_event_time"]),
                    _ts(a.get("trigger_event_time")), a.get("explanation", ""))
                   for a in alerts]
        if not records:
            return 0
        with self.conn.cursor() as cur:
            before = self._count(cur, "alert")
            cur.executemany(
                "INSERT INTO alert (alert_id, alert_type, severity, shipment_id, vehicle_id, rule_id, "
                "trigger_event_id, start_event_time, end_event_time, explanation) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (alert_id) DO NOTHING",
                records)
            return self._count(cur, "alert") - before

    def write_quarantine(self, records: Iterable[dict]) -> int:
        rows = [(r["event_id"], r.get("event_type"), r["reason_codes"], json.dumps(r["raw_payload"]),
                 _ts(r["arrival_time"]), r.get("source")) for r in records]
        if not rows:
            return 0
        with self.conn.cursor() as cur:
            before = self._count(cur, "quarantine_record")
            cur.executemany(
                "INSERT INTO quarantine_record (event_id, event_type, reason_codes, raw_payload, "
                "arrival_time, source) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
            return self._count(cur, "quarantine_record") - before

    def write_late_events(self, records: Iterable[dict]) -> int:
        rows = [(r["event_id"], _ts(r["event_time"]), r["lateness_seconds"], "SIDE_OUTPUT")
                for r in records]
        if not rows:
            return 0
        with self.conn.cursor() as cur:
            before = self._count(cur, "late_event")
            cur.executemany(
                "INSERT INTO late_event (event_id, event_time, lateness_seconds, disposition) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
            return self._count(cur, "late_event") - before

    # ------------------------------------------------------------------ run
    def write_run(self, world, pipeline, scenario: str = "", seed: int = 0) -> dict:
        status_rows = [r for r in pipeline.bronze
                       if r["event_type"] in ("shipment.created", "shipment.status_changed")]
        self.upsert_reference(world.master)
        inserted = {
            "status_history": self.apply_status_events(status_rows, world.master),
            "alerts": self.write_alerts(pipeline.alerts),
            "quarantine": self.write_quarantine(pipeline.quarantined),
            "late_events": self.write_late_events(pipeline.late_events),
        }
        totals = self.counts()
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO pipeline_run (scenario, seed, counts) VALUES (%s, %s, %s)",
                        (scenario or getattr(world.settings, "name", "history"), seed or world.seed,
                         json.dumps({"inserted": inserted, "totals": totals})))
        self.conn.commit()
        return {"inserted": inserted, "totals": totals}

    def counts(self) -> dict:
        with self.conn.cursor() as cur:
            return {table: self._count(cur, table) for table in (
                "carrier", "warehouse", "vehicle", "route", "sensor", "shipment",
                "shipment_status_history", "alert", "quarantine_record", "late_event")}

    @staticmethod
    def _count(cur, table: str) -> int:
        cur.execute(f'SELECT count(*) FROM "{table}"')
        return cur.fetchone()[0]

    def current_status(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute("SELECT status, count(*) FROM shipment GROUP BY status ORDER BY status")
            return dict(cur.fetchall())
