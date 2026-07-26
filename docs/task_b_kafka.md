# Task B — Apache Kafka: Multi-Source Urban Data Ingestion
**UrbanPulse** · DSE ZG556 Stream Processing & Analytics · Marks: 20 (M2)

> All results shown below were produced by running the code in this repository
> against the 3-broker KRaft cluster in `docker-compose.yml`. Commands to
> reproduce each result are given inline. Source lives in `src/task_b_kafka/`.

---

## B.0 Component Map

| # | Requirement | Module | Evidence (§) |
|---|---|---|---|
| 1 | 3-broker cluster + retention policies | `docker-compose.yml`, `create_topics.py` | B.1 |
| 2 | Producers — at-least-once, route-key ordering, null handling | `producer_bus_gps.py`, `producer_air_quality.py` | B.2 |
| 3 | Priority consumer architecture (HIGH zero-lag) | `priority_consumers.py` | B.3 |
| 4 | Kafka Streams route enrichment (KTable join) | `load_route_schedule.py` + `task_b_kafka_streams/` | B.4 |
| 5 | Dead-letter queue + 5-min report | `dlq_router.py`, `dlq_report.py` | B.5 |

---

## B.1 Cluster Setup & Retention Policies (4 marks)

**Cluster.** Three brokers in **KRaft mode** (no ZooKeeper), each acting as both
broker and controller, forming a 3-voter metadata quorum. Replication factor is
**3** with `min.insync.replicas = 2`, so the cluster survives the loss of one
broker with no data loss when producers use `acks=all`.

```
$ docker exec urbanpulse-broker1 kafka-metadata-quorum --bootstrap-server broker1:19092 describe --status
ClusterId:  MkU3OEVBNTcwNTJENDM2Qk   LeaderId: 2   CurrentVoters: [1,2,3]   MaxFollowerLag: 0
```

**Topics, partitions & retention** (`python -m src.task_b_kafka.create_topics`):

| Topic | Partitions | Retention | Justification |
|---|---|---|---|
| `urbanpulse.bus_gps` | **12** | **24 h** | Highest rate (~2,400 ev/s); keyed by `route_id`. 12 partitions ≈ 200 ev/s each and up to 12 parallel consumers. **24 h** = replay bus positions during accident/incident investigations. |
| `urbanpulse.traffic_signals` | **6** | 7 d | Keyed by `junction_id`. **6** is divisible by the STANDARD consumer-group size (3) for even assignment and read whole by the 1-consumer HIGH group. Signal history ages out fast → 7 d. |
| `urbanpulse.air_quality` | **3** | **90 d** | Low rate (~60 ev/s), keyed by `zone`. 3 partitions give modest parallelism without over-partitioning. **90 d** = seasonal pollution-trend analysis. |
| `urbanpulse.smart_meters` | **12** | **365 d** | High rate (~1,100 ev/s), keyed by `ward_id` (12 wards) → one partition per ward. **365 d** = annual regulatory energy audit. |

*Three distinct retention values (24 h / 90 d / 365 d), each tied to a concrete
operational or regulatory driver, exactly as required.* Verified live:

```
$ python -m src.task_b_kafka.create_topics --describe
urbanpulse.bus_gps       partitions=12  retention.ms=86400000    (~1d)   min.isr=2
urbanpulse.air_quality   partitions=3   retention.ms=7776000000  (~90d)  min.isr=2
urbanpulse.smart_meters  partitions=12  retention.ms=31536000000 (~365d) min.isr=2
```

---

## B.2 Producers (5 marks)

### Bus GPS — ordered per route (`producer_bus_gps.py`)
Ordering of positions **per route** is guaranteed by two mechanisms together:

1. **`key = route_id`** → the default partitioner sends every event for a route
   to the *same* partition, and Kafka guarantees order within a partition.
2. **`enable.idempotence = True` + `acks=all`** → per-partition order is
   preserved even across retries and with up to 5 in-flight batches, and
   producer retries are de-duplicated.

```
$ python -m src.task_b_kafka.producer_bus_gps --count 2000
[bus_gps] producing to urbanpulse.bus_gps (key=route_id, idempotent, acks=all)
[bus_gps] sent=2000 delivered=2000 failed=0
```

### Air Quality — at-least-once + retry + null handling (`producer_air_quality.py`)
- **At-least-once:** `acks=all` + client `retries=5`; `enable.idempotence=False`
  so a retried send may duplicate — acceptable under at-least-once (a missed AQI
  breach is worse than a duplicate).
- **Explicit retry logic** (sensors "occasionally timeout"): a delivery that
  fails after the client's own retries is re-queued by the application up to
  `MAX_APP_RETRIES=3` times, tracked via a message header `attempt`.
- **Null-AQI handling:** the simulator injects the mandated ~5% null-AQI sensor
  faults. The producer logs and counts each one and forwards it (never crashes),
  so the downstream DLQ can classify it. Graceful = observed, counted, non-fatal.

```
$ python -m src.task_b_kafka.producer_air_quality --count 2000
[air_quality] ⚠ null AQI from AQ-Z01-00 (zone=Z01-CentralBusiness) — forwarding for DLQ [total nulls=25]
[air_quality] sent=2000 delivered=2000 app-retries=0 perm-failed=0
[air_quality] null-AQI handled gracefully: 94 (4.7%)   ← ≈ the injected 5%
```

---

## B.3 Priority Consumer Architecture (6 marks)

Two consumer groups read the **same** `traffic_signals` topic with independent
committed offsets:

| Group | Members | Partitions each | Role |
|---|---|---|---|
| `urbanpulse-signals-high` (**HIGH_PRIORITY**) | **1** | all 6 | real-time signal control — must stay ~0 lag |
| `urbanpulse-signals-standard` (**STANDARD_PRIORITY**) | **3** | 2 | analytics dashboard — lag-tolerant |

Because the groups have **independent offsets**, a deliberate slowdown in the
STANDARD group (150 ms/msg) cannot slow HIGH. The self-contained demo produces
signals at 300 ev/s and prints live lag for both groups:

```
$ python -m src.task_b_kafka.priority_consumers --role demo --duration 30 --slow-ms 150 --prod-rate 300

 time |  HIGH lag  HIGH done |  STD lag  STD done   (topic has 6 partitions)
--------------------------------------------------------------
   0s |         0        167 |      156        15
   9s |         7      2,199 |    2,007       195
  18s |         8      4,235 |    3,864       373
  27s |        13      6,244 |    5,695       552
```

**Result:** HIGH_PRIORITY lag stays in single digits (near-zero) and keeps
draining all partitions, while STANDARD_PRIORITY lag climbs past **5,600**
because its 3 slow consumers process only ~20 ev/s combined against 300 ev/s of
production. The critical signal-control path is fully isolated from analytics
back-pressure — exactly the guarantee the city needs for 90-second adaptive
signal control.

---

## B.4 Kafka Streams Route Enrichment (3 marks)

This is implemented as an **actual Apache Kafka Streams** application in Java
(`src/task_b_kafka_streams/RouteEnrichmentApp.java`), rather than a Python
look-up loop. The supplied `route_schedule.csv` is first loaded by
`load_route_schedule.py` into `urbanpulse.route_schedule`, a **compacted topic**
keyed by `route_id`. Kafka Streams materialises that topic as a local KTable and
performs this left join:

```
bus_gps (KStream, keyed route_id) LEFT JOIN route_schedule (compacted KTable)
      → urbanpulse.bus_enriched   (+ route_name, terminal, scheduled_arrival_time)
```

The service uses `exactly_once_v2` processing. A GPS event with an unknown
`route_id` is emitted with `join_status = NO_ROUTE_MATCH` rather than dropped,
making the exception visible for data-quality follow-up.

```
$ docker compose exec app python -m src.task_b_kafka.load_route_schedule
[route-schedule] loaded 20 rows into urbanpulse.route_schedule (key=route_id, cleanup.policy=compact)
$ docker compose --profile streams up -d --build route-enrichment
[kafka-streams] KStream(bus_gps) LEFT JOIN KTable(route_schedule) → urbanpulse.bus_enriched
```
Sample enriched record on `urbanpulse.bus_enriched`:
```json
{ "bus_id": "BUS-R002-00", "route_id": "R002", "lat": 12.869167, "lon": 77.541086,
  "speed_kmh": 42.4, "occupancy_pct": 26, "timestamp": 1783647584927,
  "route_name": "Shivajinagar - Whitefield", "terminal": "Whitefield TTMC",
  "scheduled_arrival_time": "08:20", "join_status": "MATCHED" }
```
This enriched stream (position + schedule) is the foundation for the real-time
ETA service.

---

## B.5 Dead-Letter Queue (2 marks)

`dlq_router.py` consumes all four raw streams, validates each event against the
per-stream rules in `src/common/schemas.py`, and routes failures to
`urbanpulse.dlq` with an `error_reason`, the source topic/partition/offset, and
the original payload (for replay). **Well over 3 validation rules** are enforced —
e.g. `NULL_AQI`, `AQI_OUT_OF_RANGE`, `IMPOSSIBLE_GPS`, `IMPOSSIBLE_SPEED`,
`NULL_WAIT`, `VOLTAGE_OUT_OF_RANGE`, `POWER_FACTOR_OUT_OF_RANGE`, `NULL_KWH`,
`MALFORMED_JSON`.

```
$ python -m src.task_b_kafka.dlq_router --from-beginning --duration 25
[dlq] FINAL: seen=14,012 rejected=244
```

**5-minute DLQ report** (`dlq_report.py`) — error-type distribution:

| error_reason | count | share |
|---|---|---|
| NULL_AQI | 107 | 43.9% |
| NULL_WAIT | 84 | 34.4% |
| IMPOSSIBLE_GPS | 50 | 20.5% |
| VOLTAGE_OUT_OF_RANGE | 3 | 1.2% |

| stream | rejected | top reasons |
|---|---|---|
| air_quality | 107 | NULL_AQI:107 |
| traffic_signals | 84 | NULL_WAIT:84 |
| bus_gps | 50 | IMPOSSIBLE_GPS:50 |
| smart_meters | 3 | VOLTAGE_OUT_OF_RANGE:3 |

The rejection rate (~1.7% overall) matches the anomaly rates the simulators
inject (5% null AQI, ~1.5% impossible GPS, ~1% null wait, ~0.5% bad voltage),
confirming the validation and routing are correct end-to-end.

---

## B.6 How to Run (all of Task B)

```bash
docker compose up -d                                    # 3 brokers + Kafka-UI + app
docker compose exec app python -m src.task_b_kafka.create_topics
# producers
docker compose exec app python -m src.task_b_kafka.producer_bus_gps --rate 120 --duration 60
docker compose exec app python -m src.task_b_kafka.producer_air_quality --rate 20 --duration 60
# priority consumers (self-contained demo)
docker compose exec app python -m src.task_b_kafka.priority_consumers --role demo
# enrichment join
docker compose exec app python -m src.task_b_kafka.load_route_schedule
docker compose --profile streams up -d --build route-enrichment
# DLQ
docker compose exec app python -m src.task_b_kafka.dlq_router --from-beginning --duration 60
docker compose exec app python -m src.task_b_kafka.dlq_report
```
Kafka-UI at **http://localhost:8080** shows all topics, partitions, consumer-group
lag, and messages live — useful for the video walkthrough.

*End of Task B.*
