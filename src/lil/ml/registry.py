"""A small model registry on disk.

    data/models/<name>/<version>/model.pkl
    data/models/<name>/<version>/meta.json
    data/models/<name>/LATEST

The version is a hash of what produced the model: the feature definition,
the training window, a fingerprint of the Gold files it read and the
parameters. Retraining the same code on the same data gives the same
version; anything else gives a new one. meta.json is what makes a prediction
traceable back to the data behind it.
"""
from __future__ import annotations

import hashlib
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def fingerprint(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(Path(p) for p in paths):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


class ModelRegistry:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, model: Any, meta: dict, tag: str | None = None) -> str:
        identity = {k: meta.get(k) for k in ("feature_spec", "training_window", "data_fingerprint",
                                             "params", "model_class")}
        version = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()[:10]
        target = self.root / name / version
        target.mkdir(parents=True, exist_ok=True)
        with open(target / "model.pkl", "wb") as handle:
            pickle.dump(model, handle)
        meta = {**meta, "name": name, "version": version, "tag": tag,
                "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        (target / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
        if tag:
            (self.root / name / f"TAG_{tag}").write_text(version)
        else:
            (self.root / name / "LATEST").write_text(version)
        return version

    def resolve(self, name: str, version: str | None = None, tag: str | None = None) -> str:
        if version:
            return version
        marker = self.root / name / (f"TAG_{tag}" if tag else "LATEST")
        if not marker.exists():
            raise FileNotFoundError(f"no model '{name}' in {self.root}; run 'make ml' first")
        return marker.read_text().strip()

    def load(self, name: str, version: str | None = None, tag: str | None = None):
        version = self.resolve(name, version, tag)
        target = self.root / name / version
        with open(target / "model.pkl", "rb") as handle:
            model = pickle.load(handle)
        return model, json.loads((target / "meta.json").read_text())

    def catalogue(self) -> dict:
        out = {}
        for folder in sorted(p for p in self.root.iterdir() if p.is_dir()):
            versions = sorted(p.name for p in folder.iterdir() if (p / "meta.json").exists())
            latest = (folder / "LATEST").read_text().strip() if (folder / "LATEST").exists() else None
            out[folder.name] = {"versions": versions, "latest": latest}
        return out
