"""
Task B — Air Quality producer (at-least-once + retry + null-AQI handling).

Three graded behaviours:

  1. AT-LEAST-ONCE delivery: acks=all + bounded retries so a transient broker/
     network timeout never silently drops a reading (a missed AQI breach could
     be a public-health failure). Duplicates are acceptable under at-least-once;
     downstream consumers are idempotent on (sensor_id, timestamp).

  2. EXPLICIT RETRY LOGIC for "sensors occasionally timeout": beyond the client's
     internal `retries`, a delivery that ultimately fails is re-queued by the
     application up to MAX_APP_RETRIES times (with the failing event captured via
     the delivery callback). This models an edge sensor whose first publish
     attempt times out and is retried.

  3. NULL-AQI HANDLING: the simulator injects the mandated ~5% null-AQI sensor
     faults. The producer does NOT crash on them — it logs each occurrence, keeps
     a counter, and still forwards the event so the downstream DLQ router
     (dlq_router.py) can route it to urbanpulse.dlq with reason NULL_AQI. Graceful
     handling = observed, counted, logged, never fatal.

Run:
    python -m src.task_b_kafka.producer_air_quality --rate 20 --duration 60
"""
from __future__ import annotations

import argparse
import json
import time
from collections import deque

from confluent_kafka import Producer

from src.common import config
from src.simulators.generators import AirQualitySimulator

MAX_APP_RETRIES = 3


class AirQualityProducer:
    def __init__(self, bootstrap: str) -> None:
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "urbanpulse-air-quality-producer",
            # at-least-once: durable acks + retries, idempotence OFF (a retry may
            # duplicate — acceptable and expected under at-least-once semantics).
            "enable.idempotence": False,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 15000,   # total time incl. retries before failing
            "linger.ms": 20,
            "compression.type": "lz4",
        })
        # events whose delivery failed and must be retried at the app level
        self._retry_queue: deque[tuple[bytes, bytes, int]] = deque()
        self.delivered = 0
        self.failed_permanently = 0
        self.null_aqi_count = 0
        self.retried = 0

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
            return
        # Delivery failed after the client's own retries → application retry.
        attempt = int((msg.headers() or [("attempt", b"0")])[0][1])
        if attempt < MAX_APP_RETRIES:
            self.retried += 1
            self._retry_queue.append((msg.key(), msg.value(), attempt + 1))
            print(f"[air_quality] timeout/err (attempt {attempt}) → re-queue: {err}")
        else:
            self.failed_permanently += 1
            print(f"[air_quality] GIVING UP after {attempt} retries: {err}")

    def _produce(self, key: bytes, value: bytes, attempt: int = 0) -> None:
        while True:
            try:
                self.producer.produce(
                    config.TOPIC_AIR_QUALITY,
                    key=key,
                    value=value,
                    headers=[("attempt", str(attempt).encode())],
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                # local queue full → serve callbacks and retry the enqueue
                self.producer.poll(0.5)

    def _drain_retries(self) -> None:
        while self._retry_queue:
            key, value, attempt = self._retry_queue.popleft()
            self._produce(key, value, attempt)

    def run(self, rate: float, duration: float, count: int) -> None:
        gen = AirQualitySimulator()             # injects ~5% null AQI
        interval = 1.0 / rate if rate > 0 else 0.0
        started = time.time()
        sent = 0
        print(f"[air_quality] producing to {config.TOPIC_AIR_QUALITY} "
              f"(at-least-once, acks=all, app-retry≤{MAX_APP_RETRIES}) @ {rate}/s")
        try:
            while True:
                if count and sent >= count:
                    break
                if duration and (time.time() - started) >= duration:
                    break

                evt = gen.emit(int(time.time() * 1000))

                # --- graceful null-AQI handling: log + count, do NOT drop ---
                if evt.get("aqi") is None:
                    self.null_aqi_count += 1
                    if self.null_aqi_count <= 20 or self.null_aqi_count % 25 == 0:
                        print(f"[air_quality] ⚠ null AQI from {evt['sensor_id']} "
                              f"(zone={evt['zone']}) — forwarding for DLQ "
                              f"[total nulls={self.null_aqi_count}]")

                key = evt["zone"].encode()      # co-locate a zone's sensors
                self._produce(key, json.dumps(evt).encode())
                sent += 1

                if sent % 200 == 0:
                    self.producer.poll(0)
                    self._drain_retries()
                if interval:
                    time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[air_quality] interrupted")
        finally:
            self.producer.flush(15)
            self._drain_retries()
            self.producer.flush(15)
            null_pct = (100 * self.null_aqi_count / sent) if sent else 0
            print(f"[air_quality] sent={sent} delivered={self.delivered} "
                  f"app-retries={self.retried} perm-failed={self.failed_permanently}")
            print(f"[air_quality] null-AQI handled gracefully: "
                  f"{self.null_aqi_count} ({null_pct:.1f}%)")


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse air_quality producer")
    ap.add_argument("--rate", type=float, default=20, help="events/sec")
    ap.add_argument("--duration", type=float, default=0, help="seconds (0=until Ctrl-C)")
    ap.add_argument("--count", type=int, default=0, help="stop after N events")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    args = ap.parse_args()
    AirQualityProducer(args.bootstrap).run(args.rate, args.duration, args.count)


if __name__ == "__main__":
    main()
