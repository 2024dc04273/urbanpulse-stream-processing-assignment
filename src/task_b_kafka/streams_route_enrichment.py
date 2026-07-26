"""
Task B — Real-time route enrichment (KStream ⋈ KTable join).

Joins the live bus_gps KStream with a static route_schedule KTable to produce an
enriched stream carrying scheduled_arrival_time, route_name and terminal beside
each GPS position — the foundation for the real-time ETA service.

  bus_gps (KStream, keyed by route_id)
        ⋈  route_schedule (KTable, keyed by route_id, loaded from CSV)
        →  urbanpulse.bus_enriched (KStream)

In the canonical JVM stack this is a Kafka Streams `KStream.join(KTable)`:

    KTable<String,Route> routes = builder.table("route_schedule");
    builder.stream("urbanpulse.bus_gps")
           .join(routes, (gps, route) -> enrich(gps, route))
           .to("urbanpulse.bus_enriched");

The Python-first implementation below has identical *semantics* — a stream-table
join keyed on route_id — with the KTable materialised in memory from the CSV
(the table is small and changes rarely, exactly the KTable use-case). Every
consumed GPS event is enriched by a table lookup and re-emitted; a GPS event
whose route_id is absent from the table is emitted with a null-join marker
(mirroring a left join) rather than dropped.

    python -m src.task_b_kafka.streams_route_enrichment
"""
from __future__ import annotations

import argparse
import csv
import json
import time

from confluent_kafka import Consumer, Producer

from src.common import config


def load_route_ktable(path: str) -> dict[str, dict]:
    """Materialise the route_schedule KTable: route_id -> schedule row."""
    table: dict[str, dict] = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            table[row["route_id"]] = {
                "route_name": row["route_name"],
                "terminal": row["terminal"],
                "scheduled_arrival_time": row["scheduled_arrival_time"],
                "headway_min": int(row["headway_min"]),
            }
    return table


def enrich(gps: dict, route: dict | None) -> dict:
    """Stream-table join projection."""
    out = dict(gps)
    if route is None:
        out.update(route_name=None, terminal=None,
                   scheduled_arrival_time=None, join_status="NO_ROUTE_MATCH")
    else:
        out.update(route_name=route["route_name"],
                   terminal=route["terminal"],
                   scheduled_arrival_time=route["scheduled_arrival_time"],
                   join_status="MATCHED")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse route enrichment stream")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--schedule", default=config.ROUTE_SCHEDULE_CSV)
    ap.add_argument("--report-every", type=int, default=2000)
    ap.add_argument("--from-beginning", action="store_true",
                    help="enrich existing history (default: only new events)")
    ap.add_argument("--duration", type=float, default=0,
                    help="stop cleanly after N seconds (0 = until Ctrl-C)")
    args = ap.parse_args()

    ktable = load_route_ktable(args.schedule)
    print(f"[enrichment] loaded route_schedule KTable: {len(ktable)} routes")

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "urbanpulse-route-enrichment",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.TOPIC_BUS_GPS])
    producer = Producer({"bootstrap.servers": args.bootstrap,
                         "linger.ms": 20, "compression.type": "lz4"})

    enriched = matched = unmatched = 0
    print(f"[enrichment] {config.TOPIC_BUS_GPS} ⋈ route_schedule "
          f"→ {config.TOPIC_BUS_ENRICHED}")
    started = time.time()
    try:
        while True:
            if args.duration and (time.time() - started) >= args.duration:
                break
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            gps = json.loads(msg.value())
            route = ktable.get(gps.get("route_id"))
            out = enrich(gps, route)
            matched += route is not None
            unmatched += route is None
            producer.produce(
                config.TOPIC_BUS_ENRICHED,
                key=(gps.get("route_id") or "").encode(),
                value=json.dumps(out).encode(),
            )
            enriched += 1
            if enriched % args.report_every == 0:
                producer.poll(0)
                print(f"[enrichment] enriched={enriched:,} "
                      f"matched={matched:,} unmatched={unmatched:,}  "
                      f"e.g. {out['route_name']} → {out['terminal']} "
                      f"(sched {out['scheduled_arrival_time']})")
    except KeyboardInterrupt:
        print("\n[enrichment] interrupted")
    finally:
        producer.flush(10)
        consumer.close()
        print(f"[enrichment] total enriched={enriched:,} "
              f"matched={matched:,} unmatched={unmatched:,}")


if __name__ == "__main__":
    main()
