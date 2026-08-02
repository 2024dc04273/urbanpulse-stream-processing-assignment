# UrbanPulse — Reproducible Runbook

Run these commands from the repository root. They work on macOS, Linux, and
Windows with Docker Desktop / Docker Compose v2 installed.

## Run the full demonstration automatically

```bash
bash scripts/run_demo.sh
```

This runs the sequence below with short demo windows and saves the terminal
evidence in `logs/demo-<timestamp>/`. For the assessed five-minute DLQ
collection, run `URBANPULSE_DEMO_DURATION=300 bash scripts/run_demo.sh`.

## 1. Start the platform and create topics

```bash
docker compose up -d
docker compose exec app python -m src.task_b_kafka.create_topics
docker compose exec app python -m src.task_b_kafka.create_topics --describe
```

Open Kafka UI at <http://localhost:8080>. Confirm all three brokers are healthy
before continuing.

## 2. Start the Kafka Streams enrichment service

Load the schedule **before** generating bus GPS data, then start the service.

```bash
docker compose exec app python -m src.task_b_kafka.load_route_schedule
docker compose --profile streams up -d --build route-enrichment
```

## 3. Start DLQ validation and live source data

Run each command in a separate terminal. The first two run for five minutes;
the third gives the required five-minute DLQ distribution report.

```bash
docker compose exec app python -m src.task_b_kafka.dlq_router --from-beginning --duration 300
```

```bash
docker compose exec app python -m src.simulators.run_simulator --all --rate 200 --duration 300
```

```bash
docker compose exec app python -m src.task_b_kafka.dlq_report --from-latest --window 300
```

## 4. Demonstrate the Task B requirements

```bash
docker compose exec app python -m src.task_b_kafka.producer_bus_gps --rate 120 --duration 60
docker compose exec app python -m src.task_b_kafka.producer_air_quality --rate 20 --duration 60
docker compose exec app python -m src.task_b_kafka.priority_consumers --role demo --duration 45
```

Inspect a few joined records:

```bash
docker compose exec broker1 kafka-console-consumer \
  --bootstrap-server broker1:19092 \
  --topic urbanpulse.bus_enriched \
  --from-beginning --timeout-ms 5000 --max-messages 10
```

## 5. Run Flink incident detection

Keep the source-data terminal from step 3 running. The following uses a
30-second bunching threshold only for a fast demonstration; the committed code
defaults to the required five minutes.

```bash
docker compose --profile flink up -d
docker compose exec -e URBANPULSE_BUNCHING_SECONDS=30 flink-jobmanager \
  flink run -d -py /opt/job/src/task_c_flink_spark/flink_incident_detection.py
docker compose exec flink-jobmanager flink list
```

View alert messages:

```bash
docker compose exec broker1 kafka-console-consumer \
  --bootstrap-server broker1:19092 \
  --topic urbanpulse.incidents \
  --from-beginning --timeout-ms 5000 --max-messages 30
```

## 6. Run Spark ward-energy analytics

Use a fresh checkpoint location for every independent demo run. The 15-second
window and 5-second watermark below are only for a quick demonstration; the
code defaults remain the required 15 minutes and 45 minutes.

```bash
docker compose --profile spark up -d
docker compose exec spark spark-submit \
  --conf "spark.jars.ivy=/tmp/ivy/ward_energy_demo_01" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/job/src/task_c_flink_spark/spark_ward_energy.py \
  --window "15 seconds" --watermark "5 seconds" \
  --checkpoint /tmp/ck/ward_energy_demo_01 --duration 90
```

The job writes each completed batch to both `urbanpulse.ward_energy_summary`
and `data/parquet/ward_energy/ward_id=<ward>/date=<date>/`.

## 7. Run Spark AQI health advisories

Again, use a new checkpoint path for a new demo.

```bash
docker compose exec spark spark-submit \
  --conf "spark.jars.ivy=/tmp/ivy/health_advisory_demo_01" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/job/src/task_c_flink_spark/spark_health_advisory.py \
  --window "30 seconds" --slide "5 seconds" --watermark "5 seconds" \
  --checkpoint /tmp/ck/health_advisory_demo_01 --duration 90
```

The production defaults are a 10-minute rolling window updated every minute,
with Update mode publishing only enriched advisories above AQI 150.

## 8. Stop the platform

```bash
docker compose --profile streams --profile flink --profile spark down
```

To also remove Kafka volumes and generated runtime state:

```bash
docker compose --profile streams --profile flink --profile spark down -v
```
