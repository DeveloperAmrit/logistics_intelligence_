"""The serving layer answers from Gold and the model outputs, never the stream."""
import importlib

import pytest

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("httpx")


@pytest.fixture
def client(small_history, small_ml, monkeypatch):
    from fastapi.testclient import TestClient

    from lil.runner import run_once
    root = small_history["root"]
    monkeypatch.setenv("LIL_STORAGE__ROOT", str(root / "lake"))
    monkeypatch.setenv("LIL_ML__REGISTRY", str(root / "models"))
    config = small_history["cfg"]
    run_once("ROUTE_DEVIATION", seed=42, cfg=_small(config), run_dir=root / "runs" / "route_deviation")
    import lil.api.app as module
    module = importlib.reload(module)
    return TestClient(module.app)


def _small(config):
    import copy
    from lil.config import Config
    small = Config(copy.deepcopy(dict(config)))
    small["simulation"].update({"duration_seconds": 1200, "shipments": 16, "vehicles": 8, "sensors": 6})
    return small


def test_run_endpoints(client):
    run = client.get("/api/v1/run").json()
    assert run["scenario"] == "ROUTE_DEVIATION"
    alerts = client.get("/api/v1/alerts").json()
    assert alerts["count"] == run["stats"]["route_deviation_alerts"] > 0
    assert client.get("/api/v1/analytics/otif").status_code == 200

    # the views behind the dashboard, all scoped to one run
    runs = client.get("/api/v1/runs").json()
    assert runs["latest"] == "route_deviation"
    assert client.get("/api/v1/run?run=route_deviation").json()["scenario"] == "ROUTE_DEVIATION"
    assert client.get("/api/v1/run?run=../../etc").status_code == 404
    minutes = client.get("/api/v1/analytics/throughput").json()["minutes"]
    assert sum(m["gps"] for m in minutes) > 0
    assert client.get("/api/v1/fleet?step=60").json()["vehicles"]
    network = client.get("/api/v1/network").json()
    assert len(network["nodes"]) == 12 and len(network["warehouses"]) == 5
    detail = client.get("/api/v1/shipments/ship-0001").json()
    assert detail["timeline"] and detail["route_nodes"]
    assert client.get("/api/v1/shipments/nope").status_code == 404

    # the live view replays the run through the real pipeline and must reach its counts
    trace = client.get("/api/v1/stream/trace?limit=3000").json()
    assert trace["total_events"] == run["published"]
    alert = next(r for r in trace["window"] if "alert" in r)
    assert alert["outcome"] == "on_time" and alert["alert"]["rule_id"] == "RD-01"


def test_timeline_is_parameterised(client):
    assert client.get("/api/v1/shipments/ship-0001/timeline").status_code == 200
    assert client.get("/api/v1/shipments/x' OR '1'='1/timeline").status_code == 404


def test_model_endpoints(client):
    report = client.get("/api/v1/ml/report").json()
    assert {"delay", "trip_anomaly", "dwell_anomaly", "demand_forecast"} <= set(report)
    assert len(client.get("/api/v1/ml/forecast").json()["warehouses"]) == 5
    assert client.get("/api/v1/ml/delay-risk?limit=5").json()["shipments"]
    assert "delay_gbt" in client.get("/api/v1/models").json()

