"""Load the supplied route-schedule CSV into a compacted Kafka topic.

Each row is keyed by ``route_id`` so the Kafka Streams application can expose
the topic as a true KTable.  Re-running this command is safe: compaction keeps
the current schedule value for every route.

    python -m src.task_b_kafka.load_route_schedule
"""
from __future__ import annotations

import argparse
import csv
import json

from confluent_kafka import Producer

from src.common import config


def main() -> None:
    ap = argparse.ArgumentParser(description="Load route_schedule CSV into Kafka")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--schedule", default=config.ROUTE_SCHEDULE_CSV)
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})
    loaded = 0
    with open(args.schedule, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            route_id = row["route_id"]
            producer.produce(
                config.TOPIC_ROUTE_SCHEDULE,
                key=route_id.encode(),
                value=json.dumps(row).encode(),
            )
            loaded += 1
            producer.poll(0)
    remaining = producer.flush(15)
    if remaining:
        raise RuntimeError(f"{remaining} route-schedule records were not delivered")
    print(f"[route-schedule] loaded {loaded} rows into {config.TOPIC_ROUTE_SCHEDULE} "
          "(key=route_id, cleanup.policy=compact)")


if __name__ == "__main__":
    main()
