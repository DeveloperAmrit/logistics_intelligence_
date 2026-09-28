"""FastAPI serving layer.

Routers stay thin: they call a service, the service queries Gold or the run
report. No SQL in a route handler, and the API is never part of the event
path. Kafka remains the boundary.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..config import load_config
from ..storage.lakehouse import Lakehouse

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None

CFG = load_config()
RUNS = CFG.lake_root.parent / "runs"
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
    app = FastAPI(title="Logistics Intelligence Platform", version="0.8.0")
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
        rows = lake.query(f"""
            SELECT event_time, new_status FROM silver_shipment_status
            WHERE shipment_id = '{shipment_id}' ORDER BY event_time
        """)
        if not rows:
            raise HTTPException(status_code=404, detail="shipment not found")
        return {"shipment_id": shipment_id,
                "timeline": [{"event_time": str(r[0]), "status": r[1]} for r in rows]}

    return app


app = create_app() if FastAPI is not None else None
