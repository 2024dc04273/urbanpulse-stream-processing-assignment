"""
Task B — Kafka topic provisioning.

Creates every UrbanPulse topic with a *justified* partition count and the
mandated per-topic retention policy. Run once after the cluster is up:

    python -m src.task_b_kafka.create_topics            # create
    python -m src.task_b_kafka.create_topics --describe # show resulting configs

Partition-count rationale (each stream's key + throughput drives the count):

  bus_gps (12)          highest-rate stream (~2,400 ev/s); keyed by route_id.
                        12 partitions → ~200 ev/s each and up to 12 parallel
                        consumers, with headroom as the fleet grows.
  traffic_signals (6)   keyed by junction_id. 6 is divisible by the STANDARD
                        priority consumer group size (3) for even assignment,
                        and read whole by the 1-consumer HIGH_PRIORITY group.
  air_quality (3)       low rate (~60 ev/s), keyed by zone. 3 partitions give
                        modest parallelism without over-partitioning a slow topic.
  smart_meters (12)     high rate (~1,100 ev/s), keyed by ward_id (12 wards) →
                        one partition per ward is a clean, balanced mapping.

Retention rationale (the three mandated lifecycles + operational defaults):

  bus_gps         24h    replay bus positions during accident/incident probes.
  air_quality     90d    pollution-trend analysis across seasons.
  smart_meters   365d    regulatory energy audits (annual).
  traffic_signals  7d    operational only; congestion history ages out fast.
  dlq             30d    investigation window for rejected events.
"""
from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from src.common import config

_DAY_MS = 24 * 60 * 60 * 1000

# (topic, partitions, retention_ms, human-readable justification)
TOPIC_SPECS = [
    # --- raw ingestion streams ---
    (config.TOPIC_BUS_GPS,        12, 1 * _DAY_MS,   "24h — accident/incident replay"),
    (config.TOPIC_TRAFFIC_SIGNALS, 6, 7 * _DAY_MS,   "7d — short operational history"),
    (config.TOPIC_AIR_QUALITY,     3, 90 * _DAY_MS,  "90d — pollution trend analysis"),
    (config.TOPIC_SMART_METERS,   12, 365 * _DAY_MS, "365d — regulatory energy audit"),
    # Static CSV reference data is loaded once, keyed by route_id.  Log
    # compaction gives Kafka Streams the latest value per key as a KTable.
    (config.TOPIC_ROUTE_SCHEDULE,  1, -1,             "compacted — current route schedule KTable"),
    # --- derived / downstream topics ---
    (config.TOPIC_BUS_ENRICHED,   12, 1 * _DAY_MS,   "24h — mirrors bus_gps lifecycle"),
    (config.TOPIC_INCIDENTS,       3, 30 * _DAY_MS,  "30d — incident audit trail"),
    (config.TOPIC_WARD_ENERGY,     6, 90 * _DAY_MS,  "90d — ward energy history"),
    (config.TOPIC_HEALTH_ADVISORIES, 3, 30 * _DAY_MS, "30d — advisory audit trail"),
    (config.TOPIC_DLQ,             3, 30 * _DAY_MS,  "30d — rejected-event investigation"),
]

REPLICATION_FACTOR = 3  # matches the 3-broker cluster; survives 1 broker loss.


def create(admin: AdminClient) -> None:
    new_topics = [
        NewTopic(
            topic,
            num_partitions=parts,
            replication_factor=REPLICATION_FACTOR,
            config={
                "retention.ms": str(ret_ms),
                "cleanup.policy": (
                    "compact" if topic == config.TOPIC_ROUTE_SCHEDULE else "delete"
                ),
                # min.insync.replicas=2 with acks=all → no data loss on 1 broker down
                "min.insync.replicas": "2",
            },
        )
        for topic, parts, ret_ms, _ in TOPIC_SPECS
    ]
    futures = admin.create_topics(new_topics, request_timeout=30)
    for topic, fut in futures.items():
        try:
            fut.result()
            spec = next(s for s in TOPIC_SPECS if s[0] == topic)
            print(f"  ✓ created {topic:<32} partitions={spec[1]:<3} "
                  f"retention={spec[3]}")
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc):
                print(f"  • exists  {topic}")
            else:
                print(f"  ✗ FAILED  {topic}: {exc}", file=sys.stderr)


def describe(admin: AdminClient) -> None:
    """Print the live partition count + retention of every UrbanPulse topic."""
    md = admin.list_topics(timeout=10)
    resources = []
    for topic, *_ in TOPIC_SPECS:
        if topic in md.topics:
            resources.append(ConfigResource(ConfigResource.Type.TOPIC, topic))
    if not resources:
        print("No UrbanPulse topics found — run without --describe first.")
        return
    for res, fut in admin.describe_configs(resources).items():
        cfg = fut.result()
        topic = res.name
        parts = len(md.topics[topic].partitions)
        ret = cfg["retention.ms"].value
        ret_days = int(ret) / _DAY_MS if ret and ret != "-1" else "∞"
        print(f"  {topic:<32} partitions={parts:<3} "
              f"retention.ms={ret} (~{ret_days}d) "
              f"min.isr={cfg['min.insync.replicas'].value}")


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse topic provisioning")
    ap.add_argument("--describe", action="store_true",
                    help="print live topic configs instead of creating")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.describe:
        print(f"Describing topics on {args.bootstrap}:")
        describe(admin)
    else:
        print(f"Creating {len(TOPIC_SPECS)} topics on {args.bootstrap} (RF={REPLICATION_FACTOR}):")
        create(admin)


if __name__ == "__main__":
    main()
