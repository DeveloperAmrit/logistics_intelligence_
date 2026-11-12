"""Delay-model features: one definition, two paths.

Training reads Gold. The online scorer reads the event stream. Both reduce
what they know about a shipment at the moment it is dispatched to the same
raw fields and pass them through ``vector()``, so the transformation a model
was trained with is the one it is served with. The raw fields can still be
gathered differently by the two paths (a late event is in Gold but was not in
the stream yet), which is what the parity check in ml/online.py measures.

Point in time: every field uses only what had happened before dispatch.
Carrier reliability counts deliveries completed in the trailing window before
dispatch, never the shipment's own outcome.
"""
from __future__ import annotations

import bisect
import math
from datetime import datetime, timedelta, timezone

import numpy as np

FEATURE_SPEC = "delay-features-v1"
IST = timezone(timedelta(hours=5, minutes=30))
CARRIERS = ["car-001", "car-002", "car-003", "car-004", "car-005", "car-006"]
WAREHOUSES = ["wh-001", "wh-002", "wh-003", "wh-004", "wh-005"]

FEATURE_NAMES = [
    "route_length_km", "slack_min", "processing_min", "dispatch_hour_ist", "weekday", "cold",
    "carrier", "origin_warehouse", "carrier_ontime_7d", "carrier_delay_7d_min",
    "carrier_trips_7d", "origin_orders_2h",
]
CATEGORICAL = [FEATURE_NAMES.index("carrier"), FEATURE_NAMES.index("origin_warehouse")]


def delay_minutes(promised: datetime, delivered: datetime) -> float:
    """Delay as Gold defines it: DuckDB's date_diff('second', ...) counts whole
    second boundaries, so the online path must count them the same way."""
    return (math.floor(delivered.timestamp()) - math.floor(promised.timestamp())) / 60.0


def _code(value, categories) -> float:
    return float(categories.index(value)) if value in categories else math.nan


def raw_fields(*, route_length_km, origin_warehouse_id, carrier_id, temperature_class,
               created_at: datetime, dispatched_at: datetime, promised_at: datetime,
               carrier_history: list[tuple[datetime, int, float]],
               origin_created: list[datetime], carrier_window_days: int = 7,
               backlog_window_hours: int = 2) -> dict:
    """What is known when the truck leaves. Both paths call this.

    carrier_history: (delivered_at, on_time, delay_min) for the carrier,
    sorted by delivered_at. origin_created: creation times at the origin
    warehouse, sorted.
    """
    local = dispatched_at.astimezone(IST) if dispatched_at.tzinfo else \
        (dispatched_at + timedelta(hours=5, minutes=30))
    window_start = dispatched_at - timedelta(days=carrier_window_days)
    times = [h[0] for h in carrier_history]
    lo, hi = bisect.bisect_left(times, window_start), bisect.bisect_left(times, dispatched_at)
    recent = carrier_history[lo:hi]
    backlog_from = dispatched_at - timedelta(hours=backlog_window_hours)
    orders = bisect.bisect_right(origin_created, dispatched_at) - \
        bisect.bisect_right(origin_created, backlog_from)
    return {
        "route_length_km": float(route_length_km),
        "slack_min": (promised_at - dispatched_at).total_seconds() / 60.0,
        "processing_min": (dispatched_at - created_at).total_seconds() / 60.0,
        "dispatch_hour_ist": local.hour,
        "weekday": local.weekday(),
        "cold": temperature_class == "COLD_2_8",
        "carrier_id": carrier_id,
        "origin_warehouse_id": origin_warehouse_id,
        "carrier_ontime_7d": (sum(r[1] for r in recent) / len(recent)) if recent else math.nan,
        "carrier_delay_7d_min": (sum(r[2] for r in recent) / len(recent)) if recent else math.nan,
        "carrier_trips_7d": len(recent),
        "origin_orders_2h": orders,
    }


def vector(raw: dict) -> list[float]:
    """The transformation itself. Changing this changes FEATURE_SPEC."""
    return [
        float(raw["route_length_km"]),
        float(raw["slack_min"]),
        float(raw["processing_min"]),
        float(raw["dispatch_hour_ist"]),
        float(raw["weekday"]),
        1.0 if raw["cold"] else 0.0,
        _code(raw["carrier_id"], CARRIERS),
        _code(raw["origin_warehouse_id"], WAREHOUSES),
        float(raw["carrier_ontime_7d"]),
        float(raw["carrier_delay_7d_min"]),
        float(raw["carrier_trips_7d"]),
        float(raw["origin_orders_2h"]),
    ]


def matrix(raws: list[dict]) -> np.ndarray:
    return np.array([vector(r) for r in raws], dtype=float)


# --------------------------------------------------------------- batch path
def delivery_frame(lake):
    """Dispatched shipments from Gold with their dispatch-time context.

    Rows not yet delivered are kept: they count towards warehouse backlog even
    though they cannot be training examples yet.
    """
    return lake.frame("""
        SELECT shipment_id, carrier_id, origin_warehouse_id, route_length_km,
               temperature_class, created_at, dispatched_at, delivered_at,
               promised_delivery_time, delay_seconds, on_time_flag
        FROM fact_delivery_performance
        WHERE dispatched_at IS NOT NULL AND created_at IS NOT NULL
        ORDER BY dispatched_at, shipment_id
    """)


def batch_raw_fields(frame, carrier_window_days: int = 7, backlog_window_hours: int = 2) -> list[dict]:
    """Gather the raw fields for every row from Gold, point in time."""
    utc = timezone.utc

    def ts(value) -> datetime:
        return value.to_pydatetime().replace(tzinfo=utc)

    history: dict[str, list[tuple[datetime, int, float]]] = {}
    delivered = frame[frame["delivered_at"].notna()].sort_values("delivered_at")
    for row in delivered.itertuples(index=False):
        history.setdefault(row.carrier_id, []).append(
            (ts(row.delivered_at), int(row.on_time_flag), row.delay_seconds / 60.0))
    created: dict[str, list[datetime]] = {}
    for row in frame.sort_values("created_at").itertuples(index=False):
        created.setdefault(row.origin_warehouse_id, []).append(ts(row.created_at))

    raws = []
    for row in frame.itertuples(index=False):
        raw = raw_fields(
            route_length_km=row.route_length_km, origin_warehouse_id=row.origin_warehouse_id,
            carrier_id=row.carrier_id, temperature_class=row.temperature_class,
            created_at=ts(row.created_at), dispatched_at=ts(row.dispatched_at),
            promised_at=ts(row.promised_delivery_time),
            carrier_history=history.get(row.carrier_id, []),
            origin_created=created.get(row.origin_warehouse_id, []),
            carrier_window_days=carrier_window_days, backlog_window_hours=backlog_window_hours)
        raw["shipment_id"] = row.shipment_id
        raws.append(raw)
    return raws
