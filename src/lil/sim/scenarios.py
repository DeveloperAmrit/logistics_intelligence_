"""Fault scenarios.

A scenario changes what the simulated world emits. It never changes the
pipeline, so the pipeline is genuinely being tested rather than told the
answer. Every scenario is deterministic for a given seed.
"""
from __future__ import annotations

from dataclasses import dataclass

NORMAL_DELIVERY = "NORMAL_DELIVERY"
GPS_DROPOUT = "GPS_DROPOUT"
DUPLICATE_EVENT = "DUPLICATE_EVENT"
LATE_EVENT = "LATE_EVENT"
ROUTE_DEVIATION = "ROUTE_DEVIATION"
WAREHOUSE_DWELL_DELAY = "WAREHOUSE_DWELL_DELAY"
COLD_CHAIN_EXCURSION = "COLD_CHAIN_EXCURSION"

ALL_SCENARIOS = [
    NORMAL_DELIVERY,
    GPS_DROPOUT,
    DUPLICATE_EVENT,
    LATE_EVENT,
    ROUTE_DEVIATION,
    WAREHOUSE_DWELL_DELAY,
    COLD_CHAIN_EXCURSION,
]


@dataclass
class ScenarioSettings:
    """Knobs the world reads while it runs. Defaults are the normal case."""

    name: str = NORMAL_DELIVERY
    dropout_seconds: int = 0            # GPS silence, then a burst of stale fixes
    duplicate_rate: float = 0.0         # fraction of events re-published verbatim
    late_rate: float = 0.0              # fraction of events delayed past the watermark
    late_delay_seconds: int = 420   # past watermark + allowed lateness
    deviation_vehicles: int = 0         # vehicles that leave the planned corridor
    deviation_metres: float = 0.0
    dwell_extra_seconds: int = 0        # extra time parked at the warehouse
    excursion_sensors: int = 0          # sensors that breach the temperature limit
    excursion_seconds: int = 0
    excursion_celsius: float = 0.0
    invalid_rate: float = 0.0           # impossible values, for the quarantine path

    @classmethod
    def for_scenario(cls, name: str, invalid_rate: float = 0.0) -> "ScenarioSettings":
        name = name.upper()
        if name not in ALL_SCENARIOS:
            raise ValueError(f"unknown scenario {name}; choose one of {ALL_SCENARIOS}")
        base = cls(name=name, invalid_rate=invalid_rate)
        if name == GPS_DROPOUT:
            base.dropout_seconds = 300
        elif name == DUPLICATE_EVENT:
            base.duplicate_rate = 0.03
        elif name == LATE_EVENT:
            base.late_rate = 0.04
        elif name == ROUTE_DEVIATION:
            base.deviation_vehicles = 3
            base.deviation_metres = 900.0
        elif name == WAREHOUSE_DWELL_DELAY:
            base.dwell_extra_seconds = 900
        elif name == COLD_CHAIN_EXCURSION:
            base.excursion_sensors = 4
            base.excursion_seconds = 1200
            base.excursion_celsius = 10.5
        return base
