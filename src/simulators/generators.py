"""
UrbanPulse — stream generators.

Pure event factories (no Kafka dependency) for the four city streams. They are
*stateful* on purpose: congested junctions stay congested for several cycles,
bunched buses stay close for minutes, and polluted zones stay polluted — so the
Task C detectors (gridlock over 3 cycles, bunching over 5 min, AQI emergencies)
actually fire during a demo instead of needing luck.

Every generator also injects a small fraction of *malformed* events (null AQI,
impossible GPS, out-of-range values) so the Task B dead-letter queue has real
work to reject. Timestamps are epoch milliseconds (int) for clean event-time
watermarking in Flink/Spark.

Each generator exposes `.emit(now_ms)` returning ONE event dict (round-robin
across its entities), so a runner can call it at any target rate.
"""
from __future__ import annotations

import math
import random
from typing import Any

from src.common import config

# ---------------------------------------------------------------------------
# City geography — route/junction/sensor anchor points inside the bbox
# (lat 12.80–13.20, lon 77.40–77.80). Spread deterministically per index.
# ---------------------------------------------------------------------------
_LAT0, _LAT1 = 12.85, 13.15
_LON0, _LON1 = 77.45, 77.75


def _anchor(idx: int, n: int) -> tuple[float, float]:
    """A stable (lat, lon) anchor for entity `idx` of `n`, on a city grid."""
    cols = max(1, int(math.sqrt(n)))
    row, col = divmod(idx, cols)
    lat = _LAT0 + (_LAT1 - _LAT0) * ((row + 0.5) / (cols + 1))
    lon = _LON0 + (_LON1 - _LON0) * ((col + 0.5) / (cols + 1))
    return lat, lon


# ===========================================================================
# 1. Bus GPS  ->  urbanpulse.bus_gps
# ===========================================================================
class BusGpsSimulator:
    """
    Fleet of buses orbiting their route anchor. Injects a persistent *bunching*
    episode (two buses on one route held < 200 m apart) and occasional corrupt
    GPS points for the DLQ.
    """

    def __init__(self, buses_per_route: int = 3, inject_anomalies: bool = True):
        self.inject = inject_anomalies
        self.buses: list[dict[str, Any]] = []
        for r_idx, route_id in enumerate(config.ROUTE_IDS):
            clat, clon = _anchor(r_idx, len(config.ROUTE_IDS))
            for b in range(buses_per_route):
                self.buses.append({
                    "bus_id": f"BUS-{route_id}-{b:02d}",
                    "route_id": route_id,
                    "clat": clat, "clon": clon,
                    "angle": random.uniform(0, 2 * math.pi),
                    "radius": random.uniform(0.010, 0.025),   # ~1–2.5 km orbit
                })
        self._i = 0
        # Bunching episode: pin buses 0 and 1 of route R001 onto the same orbit
        # (identical anchor + radius) and lock B a few metres behind A.
        self._bunch_a = self.buses[0]
        self._bunch_b = self.buses[1]
        self._bunch_b["radius"] = self._bunch_a["radius"]

    def emit(self, now_ms: int) -> dict[str, Any]:
        bus = self.buses[self._i % len(self.buses)]
        self._i += 1

        bus["angle"] = (bus["angle"] + random.uniform(0.02, 0.06)) % (2 * math.pi)

        # Sustained bunching: keep bus B locked onto bus A's position.
        if self.inject and bus is self._bunch_b:
            bus["angle"] = self._bunch_a["angle"] + 0.0004  # ~ few metres offset

        lat = bus["clat"] + bus["radius"] * math.sin(bus["angle"])
        lon = bus["clon"] + bus["radius"] * math.cos(bus["angle"])
        evt = {
            "bus_id": bus["bus_id"],
            "route_id": bus["route_id"],
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "speed_kmh": round(random.uniform(5, 55), 1),
            "occupancy_pct": random.randint(10, 100),
            "timestamp": now_ms,
        }
        # ~1.5% corrupt GPS (impossible coordinates) -> DLQ.
        if self.inject and random.random() < 0.015:
            evt["lat"] = round(random.uniform(20, 40), 6)   # far outside bbox
        return evt


# ===========================================================================
# 2. Traffic signals  ->  urbanpulse.traffic_signals
# ===========================================================================
class TrafficSignalSimulator:
    """
    Junctions per zone. Randomly starts *congestion episodes* where a junction's
    avg_wait_sec ramps above 180 s and stays there for several cycles — exactly
    the pattern the Flink gridlock detector looks for (>180 s, 3 consecutive
    cycles).
    """

    _PHASES = ["GREEN", "YELLOW", "RED"]

    def __init__(self, junctions_per_zone: int = 4, inject_anomalies: bool = True):
        self.inject = inject_anomalies
        self.junctions: list[dict[str, Any]] = []
        for z_idx, zone in enumerate(config.ZONES):
            for j in range(junctions_per_zone):
                self.junctions.append({
                    "junction_id": f"JN-{zone[:3]}-{j:02d}",
                    "zone": zone,
                    "congested_cycles": 0,   # >0 means currently in an episode
                })
        self._i = 0

    def emit(self, now_ms: int) -> dict[str, Any]:
        jn = self.junctions[self._i % len(self.junctions)]
        self._i += 1

        # Start / continue / end a congestion episode.
        if jn["congested_cycles"] > 0:
            jn["congested_cycles"] -= 1
            avg_wait = random.uniform(185, 260)          # sustained gridlock
            vehicles = random.randint(80, 160)
        elif self.inject and random.random() < 0.02:
            jn["congested_cycles"] = random.randint(3, 5)  # trigger 3–5 bad cycles
            avg_wait = random.uniform(185, 220)
            vehicles = random.randint(80, 140)
        else:
            avg_wait = random.uniform(15, 120)           # normal flow
            vehicles = random.randint(5, 60)

        evt = {
            "junction_id": jn["junction_id"],
            "zone": jn["zone"],
            "vehicle_count": vehicles,
            "avg_wait_sec": round(avg_wait, 1),
            "signal_phase": random.choice(self._PHASES),
            "timestamp": now_ms,
        }
        # ~1% null wait -> DLQ.
        if self.inject and random.random() < 0.01:
            evt["avg_wait_sec"] = None
        return evt


# ===========================================================================
# 3. Air quality  ->  urbanpulse.air_quality
# ===========================================================================
class AirQualitySimulator:
    """
    Sensors per zone. Two injected anomalies:
      * pollution episodes: a zone's AQI climbs > 300 (Flink emergency) or holds
        150–250 (Spark rolling-avg health advisory);
      * 5% of readings arrive with null AQI (the assignment's mandated sensor
        fault) — the producer must handle & log these.
    """

    def __init__(self, sensors_per_zone: int = 3, inject_anomalies: bool = True,
                 null_rate: float = 0.05):
        self.inject = inject_anomalies
        self.null_rate = null_rate
        self.sensors: list[dict[str, Any]] = []
        for z_idx, zone in enumerate(config.ZONES):
            base = random.uniform(60, 140)               # baseline AQI per zone
            for s in range(sensors_per_zone):
                self.sensors.append({
                    "sensor_id": f"AQ-{zone[:3]}-{s:02d}",
                    "zone": zone,
                    "base_aqi": base,
                    "episode_cycles": 0,
                    "episode_level": 0.0,
                })
        self._i = 0

    def emit(self, now_ms: int) -> dict[str, Any]:
        s = self.sensors[self._i % len(self.sensors)]
        self._i += 1

        if s["episode_cycles"] > 0:
            s["episode_cycles"] -= 1
            aqi = s["episode_level"] + random.uniform(-15, 15)
        elif self.inject and random.random() < 0.03:
            # Half the episodes are hazardous (>300), half unhealthy (150–250).
            if random.random() < 0.5:
                s["episode_level"] = random.uniform(310, 420)   # emergency
            else:
                s["episode_level"] = random.uniform(160, 240)   # advisory
            s["episode_cycles"] = random.randint(6, 12)
            aqi = s["episode_level"]
        else:
            aqi = s["base_aqi"] + random.uniform(-20, 20)

        aqi = max(5.0, aqi)
        evt = {
            "sensor_id": s["sensor_id"],
            "zone": s["zone"],
            "pm25": round(aqi * random.uniform(0.4, 0.6), 1),
            "pm10": round(aqi * random.uniform(0.7, 0.9), 1),
            "no2": round(random.uniform(10, 80), 1),
            "aqi": round(aqi, 1),
            "timestamp": now_ms,
        }
        # 5% mandated sensor fault: null AQI.
        if self.inject and random.random() < self.null_rate:
            evt["aqi"] = None
        return evt


# ===========================================================================
# 4. Smart meters  ->  urbanpulse.smart_meters
# ===========================================================================
class SmartMeterSimulator:
    """Meters per ward with realistic kWh / voltage / power-factor readings."""

    def __init__(self, meters_per_ward: int = 8, inject_anomalies: bool = True):
        self.inject = inject_anomalies
        self.meters: list[dict[str, Any]] = []
        for ward in config.WARD_IDS:
            for m in range(meters_per_ward):
                self.meters.append({
                    "meter_id": f"MTR-{ward}-{m:03d}",
                    "ward_id": ward,
                    "cum_kwh": random.uniform(1000, 5000),
                })
        self._i = 0

    def emit(self, now_ms: int) -> dict[str, Any]:
        m = self.meters[self._i % len(self.meters)]
        self._i += 1

        delta = random.uniform(0.05, 0.9)   # incremental consumption
        m["cum_kwh"] += delta
        evt = {
            "meter_id": m["meter_id"],
            "ward_id": m["ward_id"],
            "kwh_reading": round(delta, 3),
            "voltage": round(random.uniform(218, 242), 1),
            "power_factor": round(random.uniform(0.85, 0.99), 3),
            "timestamp": now_ms,
        }
        # ~0.5% impossible voltage -> DLQ.
        if self.inject and random.random() < 0.005:
            evt["voltage"] = round(random.uniform(400, 500), 1)
        return evt


# Registry so a generic runner can look a generator up by topic.
SIMULATORS = {
    config.TOPIC_BUS_GPS: BusGpsSimulator,
    config.TOPIC_TRAFFIC_SIGNALS: TrafficSignalSimulator,
    config.TOPIC_AIR_QUALITY: AirQualitySimulator,
    config.TOPIC_SMART_METERS: SmartMeterSimulator,
}
