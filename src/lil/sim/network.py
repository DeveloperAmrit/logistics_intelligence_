"""A small road network centred on Jodhpur.

The default backend is a built-in graph, so the demo runs with no external
simulator installed. The node coordinates are real WGS84 points around
Jodhpur and the edges are the roads between them, which is enough for
road-valid movement, geofencing and corridor deviation.

A SUMO/TraCI backend can replace this class without touching anything
downstream: the producers only need position, speed, route and time.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass

EARTH_RADIUS_M = 6_371_000.0

# name: (latitude, longitude)
NODES: dict[str, tuple[float, float]] = {
    "clock_tower":     (26.2929, 73.0243),
    "sojati_gate":     (26.2896, 73.0221),
    "paota":           (26.3025, 73.0290),
    "mandore":         (26.3450, 73.0430),
    "ratanada":        (26.2680, 73.0330),
    "shastri_nagar":   (26.2810, 73.0060),
    "basni_industrial": (26.2400, 73.0130),
    "boranada":        (26.2050, 72.9600),
    "airport_gate":    (26.2570, 73.0490),
    "banar_road":      (26.3180, 73.1080),
    "salawas":         (26.1850, 73.0200),
    "jhalamand":       (26.2270, 73.0450),
}

EDGES: list[tuple[str, str]] = [
    ("clock_tower", "sojati_gate"), ("sojati_gate", "paota"), ("paota", "mandore"),
    ("clock_tower", "ratanada"), ("ratanada", "airport_gate"), ("airport_gate", "jhalamand"),
    ("sojati_gate", "shastri_nagar"), ("shastri_nagar", "basni_industrial"),
    ("basni_industrial", "boranada"), ("basni_industrial", "jhalamand"),
    ("jhalamand", "salawas"), ("salawas", "boranada"), ("paota", "banar_road"),
    ("banar_road", "mandore"), ("ratanada", "jhalamand"),
]

WAREHOUSES = {
    "wh-001": "basni_industrial",
    "wh-002": "boranada",
    "wh-003": "mandore",
    "wh-004": "banar_road",
    "wh-005": "salawas",
}


def haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


def point_to_segment_m(point, start, end) -> float:
    """Approximate distance from a point to a segment, in metres.

    Good enough over a city-sized area: latitude and longitude are projected
    onto a local flat plane before the usual point-to-segment formula.
    """
    lat0 = math.radians((start[0] + end[0]) / 2)
    mx = 111_320.0 * math.cos(lat0)
    my = 110_574.0

    px, py = point[1] * mx, point[0] * my
    ax, ay = start[1] * mx, start[0] * my
    bx, by = end[1] * mx, end[0] * my

    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


@dataclass
class Route:
    route_id: str
    nodes: list[str]
    origin_warehouse_id: str
    destination_warehouse_id: str

    @property
    def points(self) -> list[tuple[float, float]]:
        return [NODES[n] for n in self.nodes]

    def length_m(self) -> float:
        pts = self.points
        return sum(haversine_m(pts[i], pts[i + 1]) for i in range(len(pts) - 1))

    def distance_from_corridor_m(self, position: tuple[float, float]) -> float:
        pts = self.points
        return min(
            point_to_segment_m(position, pts[i], pts[i + 1]) for i in range(len(pts) - 1)
        )


class RoadNetwork:
    """Undirected graph with shortest-path routing by great-circle distance."""

    def __init__(self) -> None:
        self.adjacency: dict[str, list[str]] = {n: [] for n in NODES}
        for a, b in EDGES:
            self.adjacency[a].append(b)
            self.adjacency[b].append(a)

    @property
    def edge_count(self) -> int:
        return len(EDGES)

    @property
    def node_count(self) -> int:
        return len(NODES)

    def shortest_path(self, start: str, goal: str) -> list[str]:
        """Dijkstra over metre distances. Keeps vehicles on real roads."""
        import heapq

        queue = [(0.0, start, [start])]
        seen: set[str] = set()
        while queue:
            cost, node, path = heapq.heappop(queue)
            if node == goal:
                return path
            if node in seen:
                continue
            seen.add(node)
            for neighbour in self.adjacency[node]:
                if neighbour in seen:
                    continue
                step = haversine_m(NODES[node], NODES[neighbour])
                heapq.heappush(queue, (cost + step, neighbour, path + [neighbour]))
        raise ValueError(f"no path between {start} and {goal}")

    def build_routes(self, rng: random.Random, count: int) -> list[Route]:
        routes: list[Route] = []
        codes = sorted(WAREHOUSES)
        for index in range(count):
            origin, destination = rng.sample(codes, 2)
            nodes = self.shortest_path(WAREHOUSES[origin], WAREHOUSES[destination])
            routes.append(Route(f"route-{index + 1:03d}", nodes, origin, destination))
        return routes


def interpolate(a: tuple[float, float], b: tuple[float, float], fraction: float) -> tuple[float, float]:
    return (a[0] + (b[0] - a[0]) * fraction, a[1] + (b[1] - a[1]) * fraction)
