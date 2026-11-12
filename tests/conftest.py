import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pytest

from lil.config import load_config


@pytest.fixture
def cfg():
    config = load_config()
    # Small, fast world for the test suite.
    config["simulation"]["duration_seconds"] = 1800
    config["simulation"]["shipments"] = 24
    config["simulation"]["vehicles"] = 12
    config["simulation"]["sensors"] = 8
    return config


@pytest.fixture(scope="session")
def small_history(tmp_path_factory):
    """Ten simulated days through the full pipeline, shared by the history,
    model and API tests. Everything lives under one temporary root."""
    from lil.runner import run_history
    root = tmp_path_factory.mktemp("lil")
    config = load_config()
    config["storage"]["root"] = str(root / "lake")
    config["ml"]["registry"] = str(root / "models")
    config["ml"]["test_days"] = 3
    config["ml"]["forecast_test_days"] = 2
    result = run_history(days=10, seed=7, cfg=config, run_dir=root / "history")
    return {"root": root, "cfg": config, "result": result, "dir": root / "history"}


@pytest.fixture(scope="session")
def small_ml(small_history):
    from lil.ml.train import train_all
    return train_all(small_history["cfg"], small_history["dir"], online_check=True)
