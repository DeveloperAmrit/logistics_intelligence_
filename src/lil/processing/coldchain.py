"""Cold-chain excursion detection.

For a 2-8 C shipment the rule is: temperature above the maximum for two
consecutive readings, or sustained for fifteen minutes. One noisy reading
is recorded and ignored, which is the whole point: an alert nobody trusts
is worse than no alert.

The episode is keyed by shipment, and only one alert is raised per episode,
so a replay cannot create a second alert for the same excursion.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from ..envelope import Event


@dataclass
class Episode:
    shipment_id: str
    sensor_id: str
    start_event_time: datetime
    last_event_time: datetime
    readings: int = 1
    max_temperature: float = 0.0
    alerted: bool = False


@dataclass
class ColdChainAlert:
    alert_id: str
    shipment_id: str
    sensor_id: str
    rule_id: str
    severity: str
    start_event_time: datetime
    trigger_event_id: str
    trigger_event_time: datetime
    max_temperature: float
    readings: int
    explanation: str

    def as_dict(self) -> dict:
        data = self.__dict__.copy()
        for key in ("start_event_time", "trigger_event_time"):
            data[key] = data[key].isoformat().replace("+00:00", "Z")
        return data


class ColdChainDetector:
    RULE_ID = "CC-01"

    def __init__(self, cfg):
        self.max_celsius = cfg.get_path("cold_chain.max_celsius")
        self.min_celsius = cfg.get_path("cold_chain.min_celsius")
        self.consecutive = cfg.get_path("cold_chain.consecutive_readings")
        self.sustained = timedelta(seconds=cfg.get_path("cold_chain.sustained_seconds"))
        self.open_episodes: dict[str, Episode] = {}
        self.suppressed_spikes = 0

    def observe(self, event: Event) -> Optional[ColdChainAlert]:
        if event.event_type != "sensor.reading":
            return None
        payload = event.payload or {}
        if payload.get("temperature_class") != "COLD_2_8":
            return None

        temperature = payload.get("temperature_c")
        if temperature is None:
            return None
        key = event.shipment_id or event.sensor_id or event.event_id

        if temperature <= self.max_celsius:
            episode = self.open_episodes.pop(key, None)
            if episode is not None and not episode.alerted:
                # The breach did not persist. Recorded, not alerted.
                self.suppressed_spikes += 1
            return None

        episode = self.open_episodes.get(key)
        if episode is None:
            self.open_episodes[key] = Episode(
                shipment_id=event.shipment_id or "", sensor_id=event.sensor_id or "",
                start_event_time=event.event_time, last_event_time=event.event_time,
                max_temperature=temperature)
            return None

        episode.readings += 1
        episode.last_event_time = event.event_time
        episode.max_temperature = max(episode.max_temperature, temperature)

        if episode.alerted:
            return None

        duration = episode.last_event_time - episode.start_event_time
        if episode.readings >= self.consecutive or duration >= self.sustained:
            episode.alerted = True
            return ColdChainAlert(
                alert_id=f"alert-cc-{episode.shipment_id}-{int(episode.start_event_time.timestamp())}",
                shipment_id=episode.shipment_id, sensor_id=episode.sensor_id,
                rule_id=self.RULE_ID, severity="CRITICAL",
                start_event_time=episode.start_event_time,
                trigger_event_id=event.event_id, trigger_event_time=event.event_time,
                max_temperature=round(episode.max_temperature, 2), readings=episode.readings,
                explanation=(
                    f"temperature above {self.max_celsius} C for {episode.readings} consecutive "
                    f"readings ({duration.total_seconds():.0f} s), peak "
                    f"{episode.max_temperature:.1f} C"),
            )
        return None
