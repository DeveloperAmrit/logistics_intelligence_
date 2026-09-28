"""Business KPIs, computed only from Gold.

The definitions live here so the dashboard, the API and any report agree on
what OTIF means. Nothing recomputes them from the raw stream.
"""
from __future__ import annotations

from ..storage.lakehouse import Lakehouse


def delivery_kpis(lake: Lakehouse) -> dict:
    rows = lake.query("""
        SELECT count(*)                                              AS shipments,
               count(delivered_at)                                   AS delivered,
               round(100.0 * avg(CASE WHEN on_time_flag IS NOT NULL
                                      THEN on_time_flag END), 1)     AS otif_pct,
               round(median(delay_seconds) / 60.0, 1)                AS median_delay_min,
               round(quantile_cont(delay_seconds, 0.95) / 60.0, 1)   AS p95_delay_min
        FROM fact_delivery_performance
    """)
    shipments, delivered, otif, median_delay, p95_delay = rows[0]
    return {"shipments": shipments, "delivered": delivered, "otif_pct": otif,
            "median_delay_min": median_delay, "p95_delay_min": p95_delay}


def carrier_performance(lake: Lakehouse) -> list[dict]:
    rows = lake.query("""
        SELECT carrier_id,
               count(*) AS shipments,
               round(100.0 * avg(CASE WHEN on_time_flag IS NOT NULL
                                      THEN on_time_flag END), 1) AS otif_pct,
               round(median(delay_seconds) / 60.0, 1) AS median_delay_min
        FROM fact_delivery_performance
        GROUP BY carrier_id ORDER BY otif_pct DESC NULLS LAST
    """)
    return [{"carrier_id": r[0], "shipments": r[1], "otif_pct": r[2],
             "median_delay_min": r[3]} for r in rows]


def cold_chain_summary(lake: Lakehouse) -> dict:
    rows = lake.query("""
        SELECT count(*) AS excursions,
               round(max(max_temperature), 2) AS peak_temperature,
               round(avg(duration_seconds) / 60.0, 1) AS mean_duration_min
        FROM fact_cold_chain_excursion
    """)
    excursions, peak, mean_duration = rows[0]
    return {"excursions": excursions, "peak_temperature": peak,
            "mean_duration_min": mean_duration}


def dwell_summary(lake: Lakehouse) -> dict:
    rows = lake.query("""
        SELECT count(*) AS shipments,
               round(median(dwell_seconds) / 60.0, 1) AS median_dwell_min,
               round(max(dwell_seconds) / 60.0, 1)    AS max_dwell_min
        FROM fact_warehouse_dwell WHERE dwell_seconds IS NOT NULL
    """)
    shipments, median_dwell, max_dwell = rows[0]
    return {"shipments": shipments, "median_dwell_min": median_dwell,
            "max_dwell_min": max_dwell}
