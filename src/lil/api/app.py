"""FastAPI serving layer.

Routers stay thin: they call a service, the service queries Gold or the run
report. No SQL in a route handler, and the API is never part of the event
path. Kafka remains the boundary.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from ..config import REPO_ROOT, load_config
from ..storage.lakehouse import Lakehouse

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
    from fastapi.responses import FileResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None

CFG = load_config()
RUNS = CFG.lake_root.parent / "runs"
HISTORY = CFG.lake_root.parent / "history"
DASHBOARD = Path(__file__).resolve().parents[3] / "dashboard" / "index.html"


def _latest_run(run: Optional[str] = None) -> Path:
    """The named run, or the most recent one. A name is only accepted if it is
    one of the run folders, so it can never point outside data/runs."""
    candidates = {p.name: p for p in RUNS.glob("*") if (p / "run_report.json").exists()}
    if not candidates:
        raise FileNotFoundError("no run found; run 'make demo' first")
    if run:
        if run not in candidates:
            raise FileNotFoundError(f"unknown run '{run}'")
        return candidates[run]
    return max(candidates.values(), key=lambda p: (p / "run_report.json").stat().st_mtime)


def _run_or_404(run: Optional[str] = None) -> Path:
    try:
        return _latest_run(run)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


def _report(run: Optional[str] = None) -> dict:
    return json.loads((_run_or_404(run) / "run_report.json").read_text())


def _lake(run: Optional[str] = None) -> Lakehouse:
    return Lakehouse(_run_or_404(run) / "lake")


def create_app():
    if FastAPI is None:  # pragma: no cover
        raise RuntimeError("fastapi is not installed; pip install fastapi uvicorn")
    app = FastAPI(title="Logistics Intelligence Platform", version="1.0.0")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                       allow_headers=["*"])
    app.add_middleware(GZipMiddleware, minimum_size=2048)

    @app.get("/")
    def dashboard():
        return FileResponse(DASHBOARD)

    @app.get("/api/v1/health")
    def health():
        return {"status": "ok", "runs_available": len(list(RUNS.glob("*")))}

    @app.get("/api/v1/runs")
    def runs():
        from .insights import run_listing
        listing = run_listing(RUNS)
        latest = max(listing, key=lambda r: r["updated"])["run"] if listing else None
        return {"latest": latest, "runs": listing}

    @app.get("/api/v1/run")
    def run_summary(run: Optional[str] = None):
        report = _report(run)
        return {"run": _run_or_404(run).name, "scenario": report["scenario"], "seed": report["seed"],
                "published": report["published"], "stats": report["stats"],
                "sizes": report["sizes"], "replay_match": report["replay_match"],
                "seconds": report["seconds"]}

    @app.get("/api/v1/alerts")
    def alerts(limit: int = 50, run: Optional[str] = None):
        path = _run_or_404(run) / "alerts.json"
        data = json.loads(path.read_text()) if path.exists() else []
        return {"count": len(data), "alerts": data[:limit]}

    @app.get("/api/v1/quarantine")
    def quarantine(limit: int = 25, run: Optional[str] = None):
        path = _run_or_404(run) / "quarantine.json"
        data = json.loads(path.read_text()) if path.exists() else []
        return {"count": len(data), "records": data[:limit]}

    @app.get("/api/v1/analytics/otif")
    def otif(run: Optional[str] = None):
        from ..analytics.kpis import delivery_kpis
        return delivery_kpis(_lake(run))

    @app.get("/api/v1/analytics/carriers")
    def carriers(run: Optional[str] = None):
        from ..analytics.kpis import carrier_performance
        return {"carriers": carrier_performance(_lake(run))}

    @app.get("/api/v1/analytics/cold-chain")
    def cold_chain(run: Optional[str] = None):
        from ..analytics.kpis import cold_chain_summary
        return cold_chain_summary(_lake(run))

    @app.get("/api/v1/analytics/dwell-time")
    def dwell(run: Optional[str] = None):
        from ..analytics.kpis import dwell_summary
        return dwell_summary(_lake(run))

    @app.get("/api/v1/shipments/{shipment_id}/timeline")
    def timeline(shipment_id: str, run: Optional[str] = None):
        lake = _lake(run)
        rows = lake.query("""
            SELECT event_time, new_status FROM silver_shipment_status
            WHERE shipment_id = ? ORDER BY event_time
        """, [shipment_id])
        if not rows:
            raise HTTPException(status_code=404, detail="shipment not found")
        return {"shipment_id": shipment_id,
                "timeline": [{"event_time": str(r[0]), "status": r[1]} for r in rows]}

    # ------------------------------------------- dashboard views over one run
    @app.get("/api/v1/shipments/{shipment_id}")
    def shipment(shipment_id: str, run: Optional[str] = None):
        from .insights import shipment_detail
        detail = shipment_detail(_lake(run), _run_or_404(run), shipment_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="shipment not found")
        return detail

    @app.get("/api/v1/analytics/throughput")
    def throughput(run: Optional[str] = None):
        from .insights import throughput as query
        return query(_lake(run), _run_or_404(run))

    @app.get("/api/v1/analytics/deliveries")
    def deliveries(run: Optional[str] = None):
        from .insights import deliveries as query
        return query(_lake(run))

    @app.get("/api/v1/analytics/carrier-detail")
    def carrier_detail(run: Optional[str] = None):
        from .insights import carriers as query
        return query(_lake(run))

    @app.get("/api/v1/analytics/lateness")
    def lateness(run: Optional[str] = None):
        from .insights import lateness as query
        return query(_run_or_404(run))

    @app.get("/api/v1/network")
    def network(run: Optional[str] = None):
        from .insights import network as query
        return query(_lake(run))

    @app.get("/api/v1/fleet")
    def fleet(run: Optional[str] = None, step: int = 30):
        from .insights import fleet as query
        return query(_lake(run), step)

    @app.get("/api/v1/stream/trace")
    def stream_trace(run: Optional[str] = None, limit: int = 900, focus: str = "auto"):
        """The run replayed through a fresh pipeline, one decision per event."""
        from .trace import cached_trace
        run_dir = _run_or_404(run)
        if not (run_dir / "bus").exists():
            raise HTTPException(status_code=404, detail="this run kept no bus files")
        limit = max(50, min(int(limit), 3000))
        mtime = (run_dir / "run_report.json").stat().st_mtime
        return cached_trace(str(run_dir), mtime, limit, focus)

    # ------------------------------------------------- history and the models
    def _history_file(name: str) -> Path:
        path = HISTORY / name
        if not path.exists():
            raise HTTPException(status_code=404,
                                detail=f"{name} not found; run 'make history' and 'make ml'")
        return path

    @app.get("/api/v1/history/summary")
    def history_summary():
        report = json.loads(_history_file("run_report.json").read_text())
        return {"scenario": report["scenario"], "published": report["published"],
                "stats": report["stats"], "gold": report["gold"], "kpis": report["kpis"],
                "sizes": report["sizes"], "seconds": report["seconds"]}

    @app.get("/api/v1/ml/report")
    def ml_report():
        return json.loads(_history_file("ml_report.json").read_text())

    @app.get("/api/v1/ml/delay-risk")
    def delay_risk(limit: int = 15):
        _history_file("ml_report.json")
        lake = Lakehouse(HISTORY / "lake")
        path = HISTORY / "lake" / "ml" / "delay_scores.parquet"
        rows = lake.query(f"""
            SELECT s.shipment_id, f.carrier_id, f.origin_warehouse_id, s.p_late,
                   s.predicted_delay_min, s.actual_delay_min
            FROM read_parquet('{path}') s JOIN fact_delivery_performance f USING (shipment_id)
            ORDER BY f.dispatched_at DESC, s.p_late DESC LIMIT ?
        """, [limit])
        keys = ("shipment_id", "carrier_id", "origin_warehouse_id", "p_late",
                "predicted_delay_min", "actual_delay_min")
        return {"shipments": [dict(zip(keys, r)) for r in rows]}

    @app.get("/api/v1/ml/forecast")
    def demand_forecast():
        _history_file("ml_report.json")
        path = HISTORY / "lake" / "ml" / "demand_forecast_next_day.parquet"
        rows = Lakehouse(HISTORY / "lake").query(f"""
            SELECT warehouse_id, round(sum(forecast_orders), 1) AS orders,
                   arg_max(hour_ist, forecast_orders) AS busiest_hour
            FROM read_parquet('{path}') GROUP BY warehouse_id ORDER BY warehouse_id
        """)
        return {"warehouses": [{"warehouse_id": r[0], "orders": r[1], "busiest_hour": r[2]}
                               for r in rows]}

    @app.get("/api/v1/ml/forecast-hourly")
    def demand_forecast_hourly():
        _history_file("ml_report.json")
        path = HISTORY / "lake" / "ml" / "demand_forecast_next_day.parquet"
        rows = Lakehouse(HISTORY / "lake").query(f"""
            SELECT warehouse_id, substr(hour_ist, 12, 5), round(forecast_orders, 2)
            FROM read_parquet('{path}') ORDER BY warehouse_id, hour_ist
        """)
        out: dict[str, list] = {}
        for wh, hour, orders in rows:
            out.setdefault(wh, []).append({"hour": hour, "orders": orders})
        return {"warehouses": out}

    @app.get("/api/v1/models")
    def models():
        from ..ml.registry import ModelRegistry
        root = Path(CFG.get_path("ml.registry", "data/models"))
        registry = ModelRegistry(root if root.is_absolute() else REPO_ROOT / root)
        return registry.catalogue()

    return app


app = create_app() if FastAPI is not None else None
