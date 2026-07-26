"""
UrbanPulse — multi-stream simulator runner.

Feeds any subset of the four city streams into Kafka at a target rate. This is
the convenience driver used for the end-to-end demo / video:

    # start ALL four streams (background) — the whole platform gets data
    python -m src.simulators.run_simulator --all

    # just one stream, faster, for 60 seconds
    python -m src.simulators.run_simulator --topic urbanpulse.smart_meters --rate 200 --duration 60

Task B's *graded* producers (producer_bus_gps.py, producer_air_quality.py)
implement the specific delivery semantics being assessed; this runner is the
general-purpose feeder for traffic_signals / smart_meters and for whole-platform
demos. Message keys follow the partitioning strategy documented in create_topics.py.
"""
from __future__ import annotations

import argparse
import json
import signal
import threading
import time

from confluent_kafka import Producer

from src.common import config
from src.simulators.generators import SIMULATORS

# Partition key per stream (see create_topics.py for the rationale).
KEY_FIELD = {
    config.TOPIC_BUS_GPS: "route_id",        # order buses per route
    config.TOPIC_TRAFFIC_SIGNALS: "junction_id",
    config.TOPIC_AIR_QUALITY: "zone",        # co-locate a zone's sensors
    config.TOPIC_SMART_METERS: "ward_id",    # co-locate a ward's meters
}

_stop = threading.Event()


def _now_ms() -> int:
    return int(time.time() * 1000)


def stream_worker(topic: str, rate: float, duration: float, bootstrap: str) -> None:
    """Produce one stream at `rate` events/sec until `duration` (0 = forever)."""
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": f"sim-{topic}",
        "linger.ms": 20,
        "compression.type": "lz4",
    })
    gen = SIMULATORS[topic]()
    key_field = KEY_FIELD[topic]
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    started = time.time()

    while not _stop.is_set():
        if duration and (time.time() - started) >= duration:
            break
        evt = gen.emit(_now_ms())
        key = str(evt.get(key_field, "")).encode()
        producer.produce(topic, key=key, value=json.dumps(evt).encode())
        sent += 1
        if sent % 1000 == 0:
            producer.poll(0)                      # serve delivery callbacks
        if interval:
            time.sleep(interval)

    producer.flush(10)
    print(f"[{topic}] produced {sent} events")


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse stream simulator")
    ap.add_argument("--topic", help="single topic to produce")
    ap.add_argument("--all", action="store_true", help="produce all four streams")
    ap.add_argument("--rate", type=float, default=0,
                    help="events/sec per stream (default: per-stream demo rates)")
    ap.add_argument("--duration", type=float, default=0,
                    help="seconds to run (0 = until Ctrl-C)")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    args = ap.parse_args()

    # Demo-scaled default rates (real targets are ~40x higher — see the spec).
    default_rates = {
        config.TOPIC_BUS_GPS: 120,
        config.TOPIC_TRAFFIC_SIGNALS: 40,
        config.TOPIC_AIR_QUALITY: 20,
        config.TOPIC_SMART_METERS: 80,
    }

    if args.all:
        topics = list(SIMULATORS.keys())
    elif args.topic:
        topics = [args.topic]
    else:
        ap.error("specify --all or --topic <name>")

    signal.signal(signal.SIGINT, lambda *_: _stop.set())
    signal.signal(signal.SIGTERM, lambda *_: _stop.set())

    threads = []
    for t in topics:
        rate = args.rate or default_rates[t]
        th = threading.Thread(target=stream_worker,
                              args=(t, rate, args.duration, args.bootstrap),
                              daemon=True)
        th.start()
        threads.append(th)
        print(f"[start] {t} @ {rate} events/sec")

    try:
        for th in threads:
            while th.is_alive():
                th.join(0.5)
    except KeyboardInterrupt:
        _stop.set()
    print("simulator stopped")


if __name__ == "__main__":
    main()
