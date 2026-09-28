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
