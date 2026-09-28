from datetime import datetime, timedelta, timezone

from lil.envelope import Event
from lil.processing.coldchain import ColdChainDetector
from lil.processing.deviation import RouteDeviationDetector
from lil.sim.network import RoadNetwork, Route

WHEN = datetime(2026, 9, 22, 11, 0, tzinfo=timezone.utc)


def reading(temperature, offset_seconds, shipment="ship-0001"):
    return Event(event_type="sensor.reading", event_time=WHEN + timedelta(seconds=offset_seconds),
                 source="iot-simulator", sensor_id="sen-001", shipment_id=shipment,
                 payload={"temperature_c": temperature, "humidity_pct": 50.0,
                          "temperature_class": "COLD_2_8"})


def test_one_spike_does_not_raise_an_alert(cfg):
    detector = ColdChainDetector(cfg)
    assert detector.observe(reading(5.5, 0)) is None
    assert detector.observe(reading(11.2, 30)) is None      # the spike
    assert detector.observe(reading(5.7, 60)) is None       # back to normal
    assert detector.suppressed_spikes == 1


def test_two_consecutive_breaches_raise_exactly_one_alert(cfg):
    detector = ColdChainDetector(cfg)
    detector.observe(reading(5.5, 0))
    assert detector.observe(reading(9.1, 30)) is None
    alert = detector.observe(reading(9.4, 60))
    assert alert is not None
    assert alert.rule_id == "CC-01"
    assert alert.max_temperature == 9.4
    assert detector.observe(reading(9.9, 90)) is None       # same episode, no second alert


def test_an_ambient_shipment_is_not_judged_by_the_cold_rule(cfg):
    detector = ColdChainDetector(cfg)
    event = reading(15.0, 0)
    event.payload["temperature_class"] = "AMBIENT"
    assert detector.observe(event) is None


def test_the_alert_is_traceable_to_the_event_that_caused_it(cfg):
    detector = ColdChainDetector(cfg)
    detector.observe(reading(9.1, 0))
    trigger = reading(9.6, 30)
    alert = detector.observe(trigger)
    assert alert.trigger_event_id == trigger.event_id
    assert "above" in alert.explanation


def _route() -> Route:
    network = RoadNetwork()
    return Route("route-001", network.shortest_path("basni_industrial", "mandore"),
                 "wh-001", "wh-003")


def gps(lat, lon, offset_seconds):
    return Event(event_type="gps.telemetry", event_time=WHEN + timedelta(seconds=offset_seconds),
                 source="simulator", vehicle_id="veh-001", shipment_id="ship-0001",
                 route_id="route-001",
                 payload={"latitude": lat, "longitude": lon, "speed_kph": 42.0})


def test_a_vehicle_on_its_route_raises_nothing(cfg):
    route = _route()
    detector = RouteDeviationDetector(cfg, {"route-001": route}.get)
    for index, point in enumerate(route.points):
        assert detector.observe(gps(point[0], point[1], index * 30)) is None


def test_one_stray_fix_is_not_a_deviation(cfg):
    route = _route()
    detector = RouteDeviationDetector(cfg, {"route-001": route}.get)
    start = route.points[0]
    assert detector.observe(gps(start[0], start[1], 0)) is None
    assert detector.observe(gps(start[0] + 0.02, start[1], 30)) is None   # outside, but brief
    assert detector.observe(gps(start[0], start[1], 60)) is None          # back on route


def test_a_sustained_departure_raises_one_alert(cfg):
    route = _route()
    detector = RouteDeviationDetector(cfg, {"route-001": route}.get)
    start = route.points[0]
    detector.observe(gps(start[0] + 0.02, start[1], 0))
    alert = detector.observe(gps(start[0] + 0.02, start[1], 120))
    assert alert is not None
    assert alert.deviation_metres > cfg.get_path("route_deviation.corridor_metres")
    assert detector.observe(gps(start[0] + 0.02, start[1], 180)) is None  # no repeat
