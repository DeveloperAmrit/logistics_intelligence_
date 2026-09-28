"""Configuration loading.

Rules:
  1. config/config.yaml is the default.
  2. Any value may be overridden by an environment variable named
     LIL_<SECTION>__<KEY> (double underscore separates nesting).
  3. Configuration is validated once, at start-up, before any service
     produces or consumes data.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "config" / "config.yaml"


def _coerce(raw: str) -> Any:
    low = raw.lower()
    if low in {"true", "false"}:
        return low == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _apply_env(cfg: dict) -> dict:
    for key, raw in os.environ.items():
        if not key.startswith("LIL_"):
            continue
        path = key[4:].lower().split("__")
        node = cfg
        for part in path[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                break
        else:
            node[path[-1]] = _coerce(raw)
    return cfg


class Config(dict):
    """Dict with dotted access: cfg.get_path('cold_chain.max_celsius')."""

    def get_path(self, dotted: str, default: Any = None) -> Any:
        node: Any = self
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    @property
    def lake_root(self) -> Path:
        root = Path(self.get_path("storage.root", "data/lake"))
        return root if root.is_absolute() else REPO_ROOT / root


_REQUIRED = [
    "simulation.seed",
    "kafka.topics.gps",
    "streaming.watermark_seconds",
    "cold_chain.max_celsius",
    "route_deviation.corridor_metres",
    "storage.root",
]


def load_config(path: str | Path | None = None) -> Config:
    path = Path(path) if path else DEFAULT_CONFIG
    with open(path, "r", encoding="utf-8") as handle:
        cfg = Config(_apply_env(yaml.safe_load(handle)))
    missing = [key for key in _REQUIRED if cfg.get_path(key) is None]
    if missing:
        raise ValueError(f"configuration is missing required keys: {missing}")
    if cfg.get_path("cold_chain.min_celsius") >= cfg.get_path("cold_chain.max_celsius"):
        raise ValueError("cold_chain.min_celsius must be below cold_chain.max_celsius")
    return cfg
