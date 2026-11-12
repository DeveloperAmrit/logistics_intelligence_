"""FastAPI serving layer.

Routers stay thin: they call a service, the service queries Gold or the run
report. No SQL in a route handler, and the API is never part of the event
path. Kafka remains the boundary.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..config import REPO_ROOT, load_config
from ..storage.lakehouse import Lakehouse

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None

CFG = load_config()
RUNS = CFG.lake_root.parent / "runs"
HISTORY = CFG.lake_root.parent / "history"
DASHBOARD = Path(__file__).resolve().parents[3] / "dashboard" / "index.html"


def _latest_run() -> Path:
    candidates = [p for p in RUNS.glob("*") if (p / "run_report.json").exists()]
    if not candidates:
        raise FileNotFoundError("no run found; run 'make demo' first")
    return max(candidates, key=lambda p: (p / "run_report.json").stat().st_mtime)


def _report() -> dict:
    return json.loads((_latest_run() / "run_report.json").read_text())


def _lake() -> Lakehouse:
    return Lakehouse(_latest_run() / "lake")


def create_app():
    if FastAPI is None:  # pragma: no cover
        raise RuntimeError("fastapi is not installed; pip install fastapi uvicorn")
    app = FastAPI(title="Logistics Intelligence Platform", version="1.0.0")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                       allow_headers=["*"])

    @app.get("/")
    def dashboard():
        return FileResponse(DASHBOARD)

    @app.get("/api/v1/health")
    def health():
        return {"status": "ok", "runs_available": len(list(RUNS.glob("*")))}

    @app.get("/api/v1/run")
    def run_summary():
        try:
            report = _report()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"scenario": report["scenario"], "seed": report["seed"],
                "published": report["published"], "stats": report["stats"],
                "sizes": report["sizes"], "replay_match": report["replay_match"],
                "seconds": report["seconds"]}

    @app.get("/api/v1/alerts")
    def alerts(limit: int = 50):
        path = _latest_run() / "alerts.json"
        data = json.loads(path.read_text()) if path.exists() else []
        return {"count": len(data), "alerts": data[:limit]}

    @app.get("/api/v1/quarantine")
    def quarantine(limit: int = 25):
        path = _latest_run() / "quarantine.json"
        data = json.loads(path.read_text()) if path.exists() else []
        return {"count": len(data), "records": data[:limit]}

    @app.get("/api/v1/analytics/otif")
    def otif():
        from ..analytics.kpis import delivery_kpis
        return delivery_kpis(_lake())

    @app.get("/api/v1/analytics/carriers")
    def carriers():
        from ..analytics.kpis import carrier_performance
        return {"carriers": carrier_performance(_lake())}

    @app.get("/api/v1/analytics/cold-chain")
    def cold_chain():
        from ..analytics.kpis import cold_chain_summary
        return cold_chain_summary(_lake())

    @app.get("/api/v1/analytics/dwell-time")
    def dwell():
        from ..analytics.kpis import dwell_summary
        return dwell_summary(_lake())

    @app.get("/api/v1/shipments/{shipment_id}/timeline")
    def timeline(shipment_id: str):
        lake = _lake()
        rows = lake.query("""
            SELECT event_time, new_status FROM silver_shipment_status
            WHERE shipment_id = ? ORDER BY event_time
        """, [shipment_id])
        if not rows:
            raise HTTPException(status_code=404, detail="shipment not found")
        return {"shipment_id": shipment_id,
                "timeline": [{"event_time": str(r[0]), "status": r[1]} for r in rows]}

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

    @app.get("/api/v1/models")
    def models():
        from ..ml.registry import ModelRegistry
        root = Path(CFG.get_path("ml.registry", "data/models"))
        registry = ModelRegistry(root if root.is_absolute() else REPO_ROOT / root)
        return registry.catalogue()

    return app


app = create_app() if FastAPI is not None else None
