"""
UrbanPulse — shared configuration.

Single source of truth for Kafka connection details and topic names, used by
every producer, consumer, simulator, and stream-processing job. Override the
broker list with the URBANPULSE_BOOTSTRAP env var when running inside Docker
(where brokers are reachable as broker1:19092,... instead of localhost:9092).
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Kafka connection
# ---------------------------------------------------------------------------
# From the HOST machine the 3 brokers advertise on localhost:9092/9094/9096.
# From INSIDE the docker network they advertise as broker{1,2,3}:19092.
BOOTSTRAP_SERVERS: str = os.environ.get(
    "URBANPULSE_BOOTSTRAP",
    "localhost:9092,localhost:9094,localhost:9096",
)

# In-cluster bootstrap (used by kafka-ui, Flink, Spark containers)
BOOTSTRAP_SERVERS_INTERNAL: str = "broker1:19092,broker2:19092,broker3:19092"

# ---------------------------------------------------------------------------
# Topics — see create_topics.py for partition counts & retention policies
# ---------------------------------------------------------------------------
TOPIC_BUS_GPS: str = "urbanpulse.bus_gps"
TOPIC_TRAFFIC_SIGNALS: str = "urbanpulse.traffic_signals"
TOPIC_AIR_QUALITY: str = "urbanpulse.air_quality"
TOPIC_SMART_METERS: str = "urbanpulse.smart_meters"
# Static reference data for the Task B Kafka Streams KTable join.  This is a
# compacted topic: a route_id key always resolves to its latest schedule row.
TOPIC_ROUTE_SCHEDULE: str = "urbanpulse.route_schedule"

# Derived / downstream topics
TOPIC_BUS_ENRICHED: str = "urbanpulse.bus_enriched"          # Task B: Streams join output
TOPIC_INCIDENTS: str = "urbanpulse.incidents"               # Task C: Flink alerts
TOPIC_WARD_ENERGY: str = "urbanpulse.ward_energy_summary"   # Task C: Spark ward aggregates
TOPIC_HEALTH_ADVISORIES: str = "urbanpulse.health_advisories"  # Task C: Spark SQL advisories
TOPIC_DLQ: str = "urbanpulse.dlq"                           # Task B: dead-letter queue

# ---------------------------------------------------------------------------
# Reference-data files (static join tables)
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR: str = os.environ.get(
    "URBANPULSE_DATA_DIR",
    os.path.normpath(os.path.join(_HERE, "..", "..", "data")),
)
ROUTE_SCHEDULE_CSV: str = os.path.join(DATA_DIR, "route_schedule.csv")
ZONE_PROFILE_CSV: str = os.path.join(DATA_DIR, "zone_profile.csv")
PARQUET_OUTPUT_DIR: str = os.path.join(DATA_DIR, "parquet", "ward_energy")

# ---------------------------------------------------------------------------
# Domain constants — the city topology (used by simulators & detectors)
# ---------------------------------------------------------------------------
# MetroConnect zones (used across AQI, signals). Kept small & fixed so joins
# and demos are reproducible.
ZONES: list[str] = [
    "Z01-CentralBusiness",
    "Z02-IndustrialEast",
    "Z03-ResidentialNorth",
    "Z04-TechCorridor",
    "Z05-OldCity",
    "Z06-Airport",
]

# Bus routes (subset of the 12k-bus fleet; each route has several buses).
ROUTE_IDS: list[str] = [f"R{n:03d}" for n in range(1, 21)]  # R001..R020

# Wards for smart-meter aggregation.
WARD_IDS: list[str] = [f"W{n:02d}" for n in range(1, 13)]   # W01..W12

# AQI category thresholds (CPCB / Indian National AQI bands).
AQI_UNHEALTHY: int = 150      # health advisory trigger (Task C Spark SQL)
AQI_HAZARDOUS: int = 300      # emergency alert trigger (Task C Flink)

# Traffic gridlock threshold.
GRIDLOCK_WAIT_SECONDS: int = 180
GRIDLOCK_CONSECUTIVE_CYCLES: int = 3

# Bus bunching threshold.
BUNCHING_DISTANCE_METRES: int = 200
BUNCHING_DURATION_SECONDS: int = 300
