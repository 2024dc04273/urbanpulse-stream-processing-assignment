"""
Task B — Bus GPS producer (ordered-per-route).

Requirement: "use a key of route_id to guarantee ordering of bus positions per
route." Ordering is guaranteed by TWO mechanisms working together:

  1. key = route_id  → Kafka's default partitioner hashes the key, so every
     event for a given route always lands on the SAME partition. Kafka only
     guarantees order *within* a partition, so same-key ⇒ same-partition ⇒
     ordered.
  2. enable.idempotence = True (with acks=all) → the producer preserves
     per-partition order even across retries and with multiple in-flight
     batches. Without this, a retried batch could be re-ordered behind a later
     one. Idempotence also de-duplicates producer retries.

Run:
    python -m src.task_b_kafka.producer_bus_gps --rate 120 --duration 60
    python -m src.task_b_kafka.producer_bus_gps --count 5000
"""
from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer

from src.common import config
from src.simulators.generators import BusGpsSimulator


class DeliveryStats:
    def __init__(self) -> None:
        self.ok = 0
        self.err = 0

    def callback(self, err, msg) -> None:
        if err is not None:
            self.err += 1
            print(f"[bus_gps] DELIVERY FAILED key={msg.key()} : {err}")
        else:
            self.ok += 1


def build_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "urbanpulse-bus-gps-producer",
        # --- ordering + durability ---
        "enable.idempotence": True,   # preserves per-partition order across retries
        "acks": "all",                # wait for all in-sync replicas
        "max.in.flight.requests.per.connection": 5,  # safe with idempotence
        "retries": 10,
        "compression.type": "lz4",
        "linger.ms": 20,
    })


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse bus_gps producer")
    ap.add_argument("--rate", type=float, default=120, help="events/sec")
    ap.add_argument("--duration", type=float, default=0, help="seconds (0=until Ctrl-C)")
    ap.add_argument("--count", type=int, default=0, help="stop after N events (0=unbounded)")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    args = ap.parse_args()

    producer = build_producer(args.bootstrap)
    stats = DeliveryStats()
    gen = BusGpsSimulator()
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    started = time.time()
    sent = 0

    print(f"[bus_gps] producing to {config.TOPIC_BUS_GPS} "
          f"(key=route_id, idempotent, acks=all) @ {args.rate}/s")
    try:
        while True:
            if args.count and sent >= args.count:
                break
            if args.duration and (time.time() - started) >= args.duration:
                break

            evt = gen.emit(int(time.time() * 1000))
            # KEY = route_id → all positions for a route are ordered on one partition.
            key = evt["route_id"].encode()
            producer.produce(
                config.TOPIC_BUS_GPS,
                key=key,
                value=json.dumps(evt).encode(),
                on_delivery=stats.callback,
            )
            sent += 1
            if sent % 500 == 0:
                producer.poll(0)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[bus_gps] interrupted")
    finally:
        producer.flush(15)
        print(f"[bus_gps] sent={sent} delivered={stats.ok} failed={stats.err}")


if __name__ == "__main__":
    main()
