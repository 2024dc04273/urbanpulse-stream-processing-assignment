"""
Task B — 5-minute DLQ report.

Reads urbanpulse.dlq from the beginning and produces the error-type distribution
required by the task: a breakdown of rejected events by error_reason and by
source topic, over a bounded window.

    # read the whole DLQ, stop after 5s of no new records, print the report
    python -m src.task_b_kafka.dlq_report

    # strict 5-minute (300s) collection window
    python -m src.task_b_kafka.dlq_report --window 300
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict

from confluent_kafka import Consumer
from tabulate import tabulate

from src.common import config


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse DLQ report")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--window", type=float, default=0,
                    help="fixed collection window in seconds (0 = until idle)")
    ap.add_argument("--idle", type=float, default=5,
                    help="stop after this many seconds with no new DLQ records")
    ap.add_argument("--from-latest", action="store_true",
                    help="collect only DLQ events that arrive after this report starts")
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"urbanpulse-dlq-report-{int(time.time())}",  # fresh read each run
        "auto.offset.reset": "latest" if args.from_latest else "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC_DLQ])

    reasons: Counter[str] = Counter()
    per_topic: dict[str, Counter[str]] = defaultdict(Counter)
    total = 0
    started = time.time()
    last_msg = time.time()
    print(f"[dlq-report] collecting from {config.TOPIC_DLQ} ...")
    try:
        while True:
            if args.window and (time.time() - started) >= args.window:
                break
            if not args.window and (time.time() - last_msg) >= args.idle and total:
                break
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            rec = json.loads(msg.value())
            reason = rec.get("error_reason", "UNKNOWN")
            topic = rec.get("source_topic", "unknown").split(".")[-1]
            reasons[reason] += 1
            per_topic[topic][reason] += 1
            total += 1
            last_msg = time.time()
    finally:
        consumer.close()

    if not total:
        print("[dlq-report] no records in DLQ yet — run the producers + dlq_router first.")
        return

    elapsed = time.time() - started
    print(f"\n===== UrbanPulse DLQ Report  (records={total:,}, "
          f"window≈{elapsed:.0f}s) =====\n")

    # Error-type distribution
    rows = [(r, n, f"{100*n/total:.1f}%") for r, n in reasons.most_common()]
    print("Error-type distribution:")
    print(tabulate(rows, headers=["error_reason", "count", "share"],
                   tablefmt="github"))

    # Errors per source stream
    print("\nRejections by source stream:")
    trows = [(t, sum(c.values()), ", ".join(f"{r}:{n}" for r, n in c.most_common(3)))
             for t, c in sorted(per_topic.items(),
                                key=lambda kv: -sum(kv[1].values()))]
    print(tabulate(trows, headers=["stream", "rejected", "top reasons"],
                   tablefmt="github"))


if __name__ == "__main__":
    main()
