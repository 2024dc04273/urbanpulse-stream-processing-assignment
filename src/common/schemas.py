"""
UrbanPulse — event schemas, validation rules, and geo helpers.

Shared by the simulators (to emit well-formed events), the DLQ router (to
reject malformed ones), and the Flink detectors (haversine for bus bunching).
Keeping the validation predicates here means the "what is a valid event"
definition lives in exactly one place.
"""
from __future__ import annotations

import math
from typing import Any

# ---------------------------------------------------------------------------
# MetroConnect geographic bounding box (roughly a tier-1 Indian metro).
# GPS points outside this box are treated as impossible / corrupt.
# ---------------------------------------------------------------------------
LAT_MIN, LAT_MAX = 12.80, 13.20
LON_MIN, LON_MAX = 77.40, 77.80


def haversine_metres(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS-84 points, in metres."""
    r = 6_371_000.0  # Earth radius (m)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Validation predicates — each returns an error string if INVALID, else None.
# The DLQ router runs these; the first non-None reason routes the event to the
# dead-letter queue. Field names match the JSON payloads emitted by simulators.
# ---------------------------------------------------------------------------
def validate_air_quality(evt: dict[str, Any]) -> str | None:
    aqi = evt.get("aqi")
    if aqi is None:
        return "NULL_AQI"
    if not isinstance(aqi, (int, float)):
        return "AQI_NOT_NUMERIC"
    if aqi < 0 or aqi > 1000:
        return "AQI_OUT_OF_RANGE"
    for field in ("pm25", "pm10", "no2"):
        v = evt.get(field)
        if v is not None and (not isinstance(v, (int, float)) or v < 0):
            return f"NEGATIVE_{field.upper()}"
    return None


def validate_bus_gps(evt: dict[str, Any]) -> str | None:
    lat, lon = evt.get("lat"), evt.get("lon")
    if lat is None or lon is None:
        return "NULL_COORDINATES"
    if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
        return "IMPOSSIBLE_GPS"
    speed = evt.get("speed_kmh")
    if speed is not None and (speed < 0 or speed > 120):
        return "IMPOSSIBLE_SPEED"
    occ = evt.get("occupancy_pct")
    if occ is not None and (occ < 0 or occ > 100):
        return "OCCUPANCY_OUT_OF_RANGE"
    return None


def validate_traffic_signal(evt: dict[str, Any]) -> str | None:
    wait = evt.get("avg_wait_sec")
    if wait is None:
        return "NULL_WAIT"
    if wait < 0 or wait > 3600:
        return "WAIT_OUT_OF_RANGE"
    if evt.get("vehicle_count", 0) < 0:
        return "NEGATIVE_VEHICLE_COUNT"
    return None


def validate_smart_meter(evt: dict[str, Any]) -> str | None:
    kwh = evt.get("kwh_reading")
    if kwh is None:
        return "NULL_KWH"
    if kwh < 0:
        return "NEGATIVE_KWH"
    pf = evt.get("power_factor")
    if pf is not None and not (0.0 <= pf <= 1.0):
        return "POWER_FACTOR_OUT_OF_RANGE"
    volt = evt.get("voltage")
    if volt is not None and (volt < 150 or volt > 300):
        return "VOLTAGE_OUT_OF_RANGE"
    return None


# Map topic -> validator, so the DLQ router can dispatch generically.
VALIDATORS = {
    "urbanpulse.air_quality": validate_air_quality,
    "urbanpulse.bus_gps": validate_bus_gps,
    "urbanpulse.traffic_signals": validate_traffic_signal,
    "urbanpulse.smart_meters": validate_smart_meter,
}
