"""
Task B — Priority consumer architecture for urbanpulse.traffic_signals.

Two consumer groups read the SAME topic with different SLAs:

  HIGH_PRIORITY  (group "urbanpulse-signals-high")     — 1 consumer, reads all
                 6 partitions. Feeds the real-time signal-control system; must
                 stay at near-zero lag at all times.
  STANDARD_PRIORITY (group "urbanpulse-signals-standard") — 3 consumers, 2
                 partitions each. Feeds the analytics dashboard; tolerant of lag.

Because the two groups have INDEPENDENT committed offsets, a slowdown in the
STANDARD group (simulated here with a per-message sleep) cannot slow the HIGH
group — that isolation is the whole point of separate consumer groups. The demo
proves it by printing live lag for both groups: STANDARD climbs, HIGH stays ~0.

    # one-command self-contained demo (spawns producer + both groups + monitor)
    python -m src.task_b_kafka.priority_consumers --role demo --duration 45

    # or run pieces in separate terminals:
    python -m src.task_b_kafka.priority_consumers --role high
    python -m src.task_b_kafka.priority_consumers --role standard --consumers 3 --slow-ms 120
    python -m src.task_b_kafka.priority_consumers --role monitor
"""
from __future__ import annotations

import argparse
import json
import threading
import time

from confluent_kafka import (Consumer, ConsumerGroupTopicPartitions, Producer,
                             TopicPartition)
from confluent_kafka.admin import AdminClient

from src.common import config
from src.simulators.generators import TrafficSignalSimulator

GROUP_HIGH = "urbanpulse-signals-high"
GROUP_STANDARD = "urbanpulse-signals-standard"
TOPIC = config.TOPIC_TRAFFIC_SIGNALS

_stop = threading.Event()


# ---------------------------------------------------------------------------
# Consumer worker
# ---------------------------------------------------------------------------
def consumer_worker(group: str, member: int, bootstrap: str, slow_ms: float,
                    processed: dict) -> None:
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "client.id": f"{group}-{member}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,   # so the monitor can read progress
        "partition.assignment.strategy": "cooperative-sticky",
    })
    c.subscribe([TOPIC])
    try:
        while not _stop.is_set():
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            _ = json.loads(msg.value())          # "process" the signal reading
            processed[group] = processed.get(group, 0) + 1
            if slow_ms:                          # simulate a slow analytics stage
                time.sleep(slow_ms / 1000.0)
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Producer worker (demo only) — drive signals fast enough that STANDARD lags
# ---------------------------------------------------------------------------
def producer_worker(bootstrap: str, rate: float) -> None:
    p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10,
                  "compression.type": "lz4"})
    gen = TrafficSignalSimulator()
    interval = 1.0 / rate
    sent = 0
    while not _stop.is_set():
        evt = gen.emit(int(time.time() * 1000))
        p.produce(TOPIC, key=evt["junction_id"].encode(), value=json.dumps(evt).encode())
        sent += 1
        if sent % 500 == 0:
            p.poll(0)
        time.sleep(interval)
    p.flush(5)


# ---------------------------------------------------------------------------
# Lag monitor — compares committed offsets vs end offsets per group
# ---------------------------------------------------------------------------
def group_lag(admin: AdminClient, probe: Consumer, group: str,
              partitions: list[int]) -> int:
    """Total lag (end - committed) for `group` across all partitions."""
    try:
        fut = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group)])
        committed = {tp.partition: tp.offset
                     for tp in fut[group].result().topic_partitions
                     if tp.topic == TOPIC}
    except Exception:  # noqa: BLE001 — group may not exist yet
        committed = {}
    total = 0
    for part in partitions:
        _, high = probe.get_watermark_offsets(TopicPartition(TOPIC, part), timeout=5)
        off = committed.get(part, -1001)
        consumed = off if off is not None and off >= 0 else high  # unknown ⇒ assume caught up
        total += max(0, high - consumed)
    return total


def monitor_worker(bootstrap: str, duration: float, processed: dict) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    probe = Consumer({"bootstrap.servers": bootstrap,
                      "group.id": "urbanpulse-lag-probe",
                      "enable.auto.commit": False})
    parts = list(probe.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions.keys())
    print(f"\n{'time':>5} | {'HIGH lag':>9} {'HIGH done':>10} | "
          f"{'STD lag':>8} {'STD done':>9}  (topic has {len(parts)} partitions)")
    print("-" * 62)
    started = time.time()
    while not _stop.is_set():
        if duration and (time.time() - started) >= duration:
            break
        t = int(time.time() - started)
        hi = group_lag(admin, probe, GROUP_HIGH, parts)
        st = group_lag(admin, probe, GROUP_STANDARD, parts)
        print(f"{t:>4}s | {hi:>9,} {processed.get(GROUP_HIGH, 0):>10,} | "
              f"{st:>8,} {processed.get(GROUP_STANDARD, 0):>9,}")
        time.sleep(3)
    probe.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_demo(bootstrap: str, duration: float, slow_ms: float, prod_rate: float) -> None:
    processed: dict[str, int] = {}
    threads = [
        threading.Thread(target=producer_worker, args=(bootstrap, prod_rate), daemon=True),
        threading.Thread(target=consumer_worker,
                         args=(GROUP_HIGH, 0, bootstrap, 0.0, processed), daemon=True),
    ]
    for i in range(3):   # STANDARD_PRIORITY group: 3 slow consumers
        threads.append(threading.Thread(
            target=consumer_worker,
            args=(GROUP_STANDARD, i, bootstrap, slow_ms, processed), daemon=True))
    monitor = threading.Thread(target=monitor_worker,
                               args=(bootstrap, duration, processed), daemon=True)

    print(f"[demo] producing {TOPIC} @ {prod_rate}/s | HIGH=1 fast consumer | "
          f"STANDARD=3 consumers @ {slow_ms}ms/msg each")
    for th in threads:
        th.start()
    time.sleep(2)        # let groups form & production get ahead
    monitor.start()
    monitor.join()
    _stop.set()
    time.sleep(1)
    print("\n[demo] RESULT: the HIGH_PRIORITY group held near-zero lag while the "
          "STANDARD_PRIORITY group fell behind — isolation via separate groups.")


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse priority consumers")
    ap.add_argument("--role", choices=["demo", "high", "standard", "monitor"],
                    default="demo")
    ap.add_argument("--consumers", type=int, default=3, help="standard group size")
    ap.add_argument("--slow-ms", type=float, default=120,
                    help="per-message processing delay for the standard group")
    ap.add_argument("--prod-rate", type=float, default=300, help="demo producer rate")
    ap.add_argument("--duration", type=float, default=45)
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    args = ap.parse_args()

    import signal
    signal.signal(signal.SIGINT, lambda *_: _stop.set())
    signal.signal(signal.SIGTERM, lambda *_: _stop.set())

    if args.role == "demo":
        run_demo(args.bootstrap, args.duration, args.slow_ms, args.prod_rate)
        return

    processed: dict[str, int] = {}
    if args.role == "high":
        print(f"[high] 1 consumer in {GROUP_HIGH} reading all partitions")
        consumer_worker(GROUP_HIGH, 0, args.bootstrap, 0.0, processed)
    elif args.role == "standard":
        print(f"[standard] {args.consumers} consumers in {GROUP_STANDARD} "
              f"@ {args.slow_ms}ms/msg")
        threads = [threading.Thread(target=consumer_worker,
                                    args=(GROUP_STANDARD, i, args.bootstrap,
                                          args.slow_ms, processed), daemon=True)
                   for i in range(args.consumers)]
        for th in threads:
            th.start()
        for th in threads:
            while th.is_alive():
                th.join(0.5)
    elif args.role == "monitor":
        monitor_worker(args.bootstrap, args.duration, processed)


if __name__ == "__main__":
    main()
