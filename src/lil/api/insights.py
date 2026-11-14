"""Read-only queries behind the dashboard's charts.

Each function takes a finished run (its Lakehouse and folder) and returns
plain JSON. They read Silver and Gold, the run report and the run's own bus
files; none of them touches the stream.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..sim.network import EDGES, NODES, WAREHOUSES
from ..storage.lakehouse import Lakehouse


def run_listing(runs_dir: Path) -> list[dict]:
    out = []
    for path in sorted(runs_dir.glob("*")):
        report_path = path / "run_report.json"
        if not report_path.exists():
            continue
        report = json.loads(report_path.read_text())
        out.append({"run": path.name, "scenario": report["scenario"], "seed": report["seed"],
                    "published": report["published"], "stats": report["stats"],
                    "kpis": report.get("kpis", {}).get("delivery", {}),
                    "seconds": report.get("seconds"), "updated": report_path.stat().st_mtime})
    return out


def throughput(lake: Lakehouse, run_dir: Path) -> dict:
    """Events per minute of event time, by type, plus quarantine and alerts."""
    rows = lake.query("""
        WITH e AS (
            SELECT date_trunc('minute', event_time) AS m, 'gps' AS kind FROM silver_gps
            UNION ALL SELECT date_trunc('minute', event_time), 'sensor' FROM silver_sensor
            UNION ALL SELECT date_trunc('minute', event_time), 'warehouse' FROM silver_warehouse
            UNION ALL SELECT date_trunc('minute', event_time), 'status' FROM silver_shipment_status)
        SELECT m, kind, count(*) FROM e GROUP BY 1, 2 ORDER BY 1
    """)
    minutes: dict[str, dict] = {}
    for minute, kind, count in rows:
        key = str(minute)[11:16]
        minutes.setdefault(key, {"minute": key, "gps": 0, "sensor": 0, "warehouse": 0,
                                 "status": 0, "quarantined": 0, "alerts": 0})[kind] = count
    dlq = run_dir / "bus" / "quarantine.dlq.jsonl"
    if dlq.exists():
        with dlq.open() as handle:
            for line in handle:
                key = json.loads(line)["value"]["event_time"][11:16]
                if key in minutes:
                    minutes[key]["quarantined"] += 1
    alerts_path = run_dir / "alerts.json"
    if alerts_path.exists():
        for alert in json.loads(alerts_path.read_text()):
            key = (alert.get("trigger_event_time") or alert.get("start_event_time") or "")[11:16]
            if key in minutes:
                minutes[key]["alerts"] += 1
    return {"minutes": list(minutes.values())}


def deliveries(lake: Lakehouse) -> dict:
    rows = lake.query("""
        SELECT f.shipment_id, f.carrier_id, c.carrier_name, f.temperature_class,
               round(f.delay_seconds / 60.0, 1), f.on_time_flag, f.route_id,
               f.origin_warehouse_id, f.destination_warehouse_id
        FROM fact_delivery_performance f LEFT JOIN dim_carrier c USING (carrier_id)
        WHERE f.delivered_at IS NOT NULL
    """)
    keys = ("shipment_id", "carrier_id", "carrier_name", "temperature_class", "delay_min",
            "on_time", "route_id", "origin", "destination")
    return {"shipments": [dict(zip(keys, r)) for r in rows]}


def carriers(lake: Lakehouse) -> dict:
    rows = lake.query("""
        SELECT f.carrier_id, any_value(c.carrier_name), count(*) AS shipments,
               count(f.delivered_at) AS delivered,
               round(100.0 * avg(CASE WHEN f.delivered_at IS NOT NULL THEN f.on_time_flag END), 1),
               round(median(f.delay_seconds) / 60.0, 1),
               round(quantile_cont(f.delay_seconds, 0.95) / 60.0, 1),
               sum(CASE WHEN f.temperature_class = 'COLD_2_8' THEN 1 ELSE 0 END)
        FROM fact_delivery_performance f LEFT JOIN dim_carrier c USING (carrier_id)
        GROUP BY f.carrier_id ORDER BY 5 DESC NULLS LAST
    """)
    keys = ("carrier_id", "carrier_name", "shipments", "delivered", "otif_pct",
            "median_delay_min", "p95_delay_min", "cold_shipments")
    return {"carriers": [dict(zip(keys, r)) for r in rows]}


def network(lake: Lakehouse) -> dict:
    routes = lake.query("SELECT route_id, origin_warehouse_id, destination_warehouse_id, "
                        "length_km, nodes FROM dim_route ORDER BY route_id")
    return {
        "nodes": [{"id": k, "lat": v[0], "lon": v[1]} for k, v in NODES.items()],
        "edges": [list(e) for e in EDGES],
        "warehouses": [{"id": k, "node": v, "lat": NODES[v][0], "lon": NODES[v][1]}
                       for k, v in WAREHOUSES.items()],
        "routes": [{"route_id": r[0], "origin": r[1], "destination": r[2], "length_km": r[3],
                    "nodes": r[4].split(">")} for r in routes],
    }


def fleet(lake: Lakehouse, step: int = 30) -> dict:
    """Every vehicle's track, one fix per `step` seconds of event time."""
    step = max(5, int(step))
    rows = lake.query(f"""
        SELECT vehicle_id, shipment_id, route_id, t, latitude, longitude, speed_kph FROM (
            SELECT vehicle_id, shipment_id, route_id,
                   CAST(epoch(event_time) AS BIGINT) AS t, latitude, longitude, speed_kph,
                   row_number() OVER (PARTITION BY vehicle_id,
                       CAST(epoch(event_time) AS BIGINT) // {step} ORDER BY event_time) AS rn
            FROM silver_gps) WHERE rn = 1 ORDER BY vehicle_id, t
    """)
    vehicles: dict[str, dict] = {}
    for vid, sid, rid, t, lat, lon, speed in rows:
        v = vehicles.setdefault(vid, {"vehicle_id": vid, "track": []})
        v["track"].append([t, round(lat, 5), round(lon, 5), round(speed or 0, 1), sid, rid])
    meta = lake.query("""
        SELECT v.vehicle_id, v.carrier_id, c.carrier_name, v.registration_no, v.refrigeration_capable
        FROM dim_vehicle v LEFT JOIN dim_carrier c USING (carrier_id)
    """)
    for vid, carrier, name, reg, reefer in meta:
        if vid in vehicles:
            vehicles[vid].update({"carrier_id": carrier, "carrier_name": name,
                                  "registration": reg, "reefer": bool(reefer)})
    cold = {r[0] for r in lake.query(
        "SELECT shipment_id FROM dim_shipment WHERE temperature_class = 'COLD_2_8'")}
    times = [p[0] for v in vehicles.values() for p in v["track"]]
    return {"step": step, "start": min(times) if times else None, "end": max(times) if times else None,
            "cold_shipments": sorted(cold), "vehicles": list(vehicles.values())}


def shipment_detail(lake: Lakehouse, run_dir: Path, shipment_id: str) -> dict | None:
    head = lake.query("""
        SELECT s.shipment_id, s.carrier_id, c.carrier_name, s.vehicle_id, s.route_id,
               s.temperature_class, s.promised_delivery_time
        FROM dim_shipment s LEFT JOIN dim_carrier c USING (carrier_id) WHERE s.shipment_id = ?
    """, [shipment_id])
    if not head:
        return None
    keys = ("shipment_id", "carrier_id", "carrier_name", "vehicle_id", "route_id",
            "temperature_class", "promised_delivery_time")
    out = {k: (str(v) if k == "promised_delivery_time" and v is not None else v)
           for k, v in zip(keys, head[0])}
    perf = lake.query("""
        SELECT dispatched_at, delivered_at, round(delay_seconds / 60.0, 1), on_time_flag,
               origin_warehouse_id, destination_warehouse_id
        FROM fact_delivery_performance WHERE shipment_id = ?""", [shipment_id])
    if perf:
        p = perf[0]
        out.update({"dispatched_at": str(p[0]) if p[0] else None,
                    "delivered_at": str(p[1]) if p[1] else None, "delay_min": p[2],
                    "on_time": p[3], "origin": p[4], "destination": p[5]})
    out["timeline"] = [{"event_time": str(r[0]), "status": r[1]} for r in lake.query(
        "SELECT event_time, new_status FROM silver_shipment_status WHERE shipment_id = ? "
        "ORDER BY event_time", [shipment_id])]
    out["readings"] = [{"t": str(r[0]), "temperature_c": r[1], "humidity_pct": r[2]}
                       for r in lake.query(
        "SELECT event_time, temperature_c, humidity_pct FROM silver_sensor "
        "WHERE shipment_id = ? ORDER BY event_time", [shipment_id])]
    out["track"] = [[str(r[0])[11:19], round(r[1], 5), round(r[2], 5)] for r in lake.query(
        "SELECT event_time, latitude, longitude FROM silver_gps WHERE shipment_id = ? "
        "ORDER BY event_time", [shipment_id])][::3]
    route = lake.query("SELECT nodes FROM dim_route WHERE route_id = ?", [out["route_id"]])
    out["route_nodes"] = route[0][0].split(">") if route else []
    alerts_path = run_dir / "alerts.json"
    alerts = json.loads(alerts_path.read_text()) if alerts_path.exists() else []
    out["alerts"] = [a for a in alerts if a.get("shipment_id") == shipment_id]
    return out


def lateness(run_dir: Path) -> dict:
    report = json.loads((run_dir / "run_report.json").read_text())
    s = report["stats"]
    return {"on_time": s.get("on_time", 0), "late_tolerated": s.get("late_within_tolerance", 0),
            "late_side_output": s.get("late_side_output", 0), "duplicates": s.get("duplicates", 0),
            "quarantined": s.get("quarantined", 0),
            "by_reason": s.get("quarantine_by_reason", {})}
