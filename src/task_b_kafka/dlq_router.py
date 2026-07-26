"""
Task B — Dead-Letter Queue router.

Consumes the four raw ingestion topics, validates every event, and routes any
that fail validation to urbanpulse.dlq with an `error_reason` field (plus the
source topic, partition/offset, and the original payload for replay). Valid
events are counted and passed through.

Validation rules live in src/common/schemas.py (one validator per stream) so the
"what is a valid event" definition is shared with the simulators. Categories of
rejection this router catches:

  * MALFORMED_JSON        — payload isn't valid JSON
  * NULL_AQI / NULL_WAIT / NULL_KWH / NULL_COORDINATES — missing critical field
  * AQI_OUT_OF_RANGE, IMPOSSIBLE_GPS, IMPOSSIBLE_SPEED,
    VOLTAGE_OUT_OF_RANGE, POWER_FACTOR_OUT_OF_RANGE, ...  — semantic violations

That is well over the "3+ validation rules" the task requires (see schemas.py).

    python -m src.task_b_kafka.dlq_router
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer, Producer

from src.common import config
from src.common.schemas import VALIDATORS

RAW_TOPICS = [
    config.TOPIC_BUS_GPS,
    config.TOPIC_TRAFFIC_SIGNALS,
    config.TOPIC_AIR_QUALITY,
    config.TOPIC_SMART_METERS,
]


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse DLQ router")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--from-beginning", action="store_true",
                    help="validate existing history too (default: only new events)")
    ap.add_argument("--report-every", type=float, default=10,
                    help="seconds between live distribution prints")
    ap.add_argument("--duration", type=float, default=0,
                    help="stop cleanly after N seconds (0 = until Ctrl-C)")
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "urbanpulse-dlq-router",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(RAW_TOPICS)
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})

    seen = 0
    rejected = 0
    reasons: Counter[str] = Counter()
    by_topic: Counter[str] = Counter()
    last_report = time.time()
    started = time.time()

    print(f"[dlq] validating {', '.join(t.split('.')[-1] for t in RAW_TOPICS)} "
          f"→ routing failures to {config.TOPIC_DLQ}")
    try:
        while True:
            if args.duration and (time.time() - started) >= args.duration:
                break
            msg = consumer.poll(1.0)
            now = time.time()
            if msg is not None and not msg.error():
                seen += 1
                topic = msg.topic()
                raw = msg.value()
                # 1) structural validation
                try:
                    evt = json.loads(raw)
                    reason = VALIDATORS.get(topic, lambda _e: None)(evt)
                except (json.JSONDecodeError, TypeError):
                    evt, reason = None, "MALFORMED_JSON"
                # 2) route failures to the DLQ
                if reason is not None:
                    rejected += 1
                    reasons[reason] += 1
                    by_topic[topic.split(".")[-1]] += 1
                    dlq_record = {
                        "error_reason": reason,
                        "source_topic": topic,
                        "source_partition": msg.partition(),
                        "source_offset": msg.offset(),
                        "detected_at": int(now * 1000),
                        "payload": (evt if evt is not None
                                    else raw.decode("utf-8", "replace")),
                    }
                    producer.produce(
                        config.TOPIC_DLQ,
                        key=(reason.encode()),
                        value=json.dumps(dlq_record).encode(),
                    )
            # periodic live distribution
            if now - last_report >= args.report_every and seen:
                producer.poll(0)
                pct = 100 * rejected / seen if seen else 0
                top = ", ".join(f"{r}={n}" for r, n in reasons.most_common(4))
                print(f"[dlq] seen={seen:,} rejected={rejected:,} ({pct:.1f}%) | {top}")
                last_report = now
    except KeyboardInterrupt:
        print("\n[dlq] interrupted")
    finally:
        producer.flush(10)
        consumer.close()
        print(f"\n[dlq] FINAL: seen={seen:,} rejected={rejected:,}")
        for reason, n in reasons.most_common():
            print(f"        {reason:<28} {n:>6,}")


if __name__ == "__main__":
    main()
