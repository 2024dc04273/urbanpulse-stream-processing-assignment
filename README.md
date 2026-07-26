# UrbanPulse — Real-Time Urban Operations Intelligence Platform

**DSE ZG556 / CC ZG556 — Stream Processing & Analytics · Situated Learning Assignment**
Domain 3: Smart Cities & Urban Infrastructure · 75 marks

UrbanPulse is a unified streaming-intelligence platform for the fictional tier-1
city **MetroConnect**. It ingests four live city streams (bus GPS, traffic
signals, air quality, smart meters), detects operational incidents in real time,
and produces ward-level analytics for city administrators — on a fully
open-source, self-hostable stack.

This repository is a **complete, runnable implementation** of all three graded
tasks, driven entirely by Docker Compose.

---

## What's here

| Task | Focus | Deliverable | Where |
|---|---|---|---|
| **A** | Architecture — Lambda vs Kappa, diagram, storage choices, govt checklist | Report | [`docs/task_a_architecture.md`](docs/task_a_architecture.md) |
| **B** | Kafka — cluster, producers, priority consumers, streams join, DLQ | Code + report | [`src/task_b_kafka/`](src/task_b_kafka), [`docs/task_b_kafka.md`](docs/task_b_kafka.md) |
| **C** | Flink incident detection + Spark ward analytics | Code + report | [`src/task_c_flink_spark/`](src/task_c_flink_spark), [`docs/task_c_flink_spark.md`](docs/task_c_flink_spark.md) |

---

## Architecture at a glance

```
 4 city streams        Kafka (3-broker KRaft)          Processing                 Serving
┌──────────────┐      ┌─────────────────────┐   ┌───────────────────────┐   ┌──────────────┐
│ bus_gps      │─────▶│ urbanpulse.bus_gps  │──▶│ Flink incident detect │──▶│ incidents    │
│ traffic_sig  │─────▶│ urbanpulse.traffic… │──▶│  (AQI/gridlock/bunch)  │   │ topic        │
│ air_quality  │─────▶│ urbanpulse.air_qual…│──▶│ Spark ward energy 15m  │──▶│ ward_energy  │
│ smart_meters │─────▶│ urbanpulse.smart_me…│──▶│ Spark AQI advisory SQL │──▶│ health_advis │
└──────────────┘      └─────────────────────┘   └───────────────────────┘   └──────────────┘
                              │  invalid events ─▶ urbanpulse.dlq  (validated + reported)
```
See [`docs/task_a_architecture.md`](docs/task_a_architecture.md) for the full
labelled diagram, storage-technology choices, and the Lambda-vs-Kappa analysis.

---

## Prerequisites

- **Docker Desktop** (Compose v2). That's it — every runtime (Kafka, the Python
  toolbox, Flink, Spark) is containerised, so nothing needs to be installed on the
  host and it works regardless of host OS/Python/CPU (arm64 or amd64).

---

## Quick start

```bash
# 1. Start the 3-broker Kafka cluster + Kafka-UI + Python toolbox
docker compose up -d

# 2. Create all topics (justified partitions + retention policies)
docker compose exec app python -m src.task_b_kafka.create_topics

# 3. Start feeding all four simulated streams (background)
docker compose exec -d app python -m src.simulators.run_simulator --all
```

Open **Kafka-UI at http://localhost:8080** to watch topics, partitions,
consumer-group lag, and messages live.

### Task B — Kafka ingestion
```bash
docker compose exec app python -m src.task_b_kafka.producer_bus_gps --rate 120 --duration 60
docker compose exec app python -m src.task_b_kafka.producer_air_quality --rate 20 --duration 60
docker compose exec app python -m src.task_b_kafka.priority_consumers --role demo          # HIGH vs STANDARD lag
# Actual Kafka Streams KStream-KTable join (route schedule is a compacted topic)
docker compose exec app python -m src.task_b_kafka.load_route_schedule
docker compose --profile streams up -d --build route-enrichment
docker compose exec app python -m src.task_b_kafka.dlq_router --from-beginning --duration 60
docker compose exec app python -m src.task_b_kafka.dlq_report
```

### Task C — Flink + Spark
```bash
# Flink incident detection (native image; local-mode MiniCluster is simplest)
docker compose --profile flink up -d
docker compose exec -e URBANPULSE_BUNCHING_SECONDS=45 flink-jobmanager \
    python /opt/job/src/task_c_flink_spark/flink_incident_detection.py

# Spark ward energy (Kafka + partitioned Parquet dual sink)
docker compose --profile spark up -d
docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/job/src/task_c_flink_spark/spark_ward_energy.py --duration 180

# Spark health advisories (streaming SQL)
docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/job/src/task_c_flink_spark/spark_health_advisory.py --duration 180
```

> **Demo tip:** the graded window sizes (15-min energy window, 10-min AQI window,
> 5-min bunching) take a while in real time. For a fast demo, shorten them:
> `--window "30 seconds"`, `--window "1 minute"`, `URBANPULSE_BUNCHING_SECONDS=30`.

### Shut down
```bash
docker compose --profile flink --profile spark down          # stop
docker compose --profile flink --profile spark down -v       # stop + wipe volumes
```

---

## Project layout

```
spa/
├── docker-compose.yml            # 3-broker Kafka + UI + app + flink + spark
├── infra/                        # Dockerfiles (app / flink / spark)
├── data/
│   ├── route_schedule.csv        # KTable for Task B enrichment join
│   ├── zone_profile.csv          # static table for Task C AQI advisory join
│   └── parquet/ward_energy/      # Task C partitioned Parquet output (generated)
├── src/
│   ├── common/                   # shared config, schemas, validation, geo
│   ├── simulators/               # 4 stateful stream generators + runner
│   ├── task_b_kafka/             # topics, producers, priority consumers, streams, DLQ
│   └── task_c_flink_spark/       # Flink incident detection + 2 Spark jobs
└── docs/                         # Task A / B / C reports (+ combined submission PDF)
```

---

## Notes on the stack

- **Kafka:** 3 brokers in **KRaft mode** (no ZooKeeper), RF=3, `min.insync.replicas=2`.
- **Python-first:** producers/consumers/DLQ use `confluent-kafka`; stream
  processing uses **PySpark** and **PyFlink**.
- **Apple Silicon:** the Flink image is built **natively for arm64** (PyFlink's
  `pemja` bridge is compiled from source) — an amd64 image under emulation
  segfaults the JVM, so native is the supported path. Spark is JVM-based and runs
  natively everywhere.

See [`docs/SUBMISSION.md`](docs/SUBMISSION.md) for packaging and the video
walkthrough outline.
