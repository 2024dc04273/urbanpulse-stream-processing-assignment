### 1. Opening — project overview

  [Screen: Repository open in VS Code/Finder. Show README.md and project folders.]

  Say:

  “Hello everyone. This is our UrbanPulse project for the Smart Cities and Urban Infrastructure domain.

  UrbanPulse is a real-time urban operations intelligence platform for a fictional city called MetroConnect. It ingests four city data streams: bus GPS locations, traffic-signal readings,
  air-quality readings, and smart-meter data.

  The platform uses Apache Kafka for ingestion and streaming transport, Kafka Streams for route enrichment, Apache Flink for real-time incident detection, and Spark Structured Streaming
  for city analytics.

  The repository also contains the combined report, the implementation for all three tasks, Docker configuration, and a one-command reproducible demonstration script.”

  [Point to these folders.]

  - src/task_b_kafka — Kafka producers, consumers, DLQ, and route enrichment support
  - src/task_c_flink_spark — Flink and Spark jobs
  - docs — report, runbook, and submission material
  - scripts/run_demo.sh — automated end-to-end reproduction

  ———

  ### 2. Prerequisites and one-command reproduction

  [Screen: Docker Desktop running.]

  Say:

  “Before running the project, Docker Desktop must be open and running. No host Kafka, Spark, or Flink installation is needed because all services are containerised.”

  [Screen: Terminal in repository root.]

  git pull origin main
  bash scripts/run_demo.sh

  Say:

  “First, we pull the latest version from the main branch. Then we run the one-command demo script.

  This script starts the three-broker Kafka cluster, creates all required Kafka topics, loads the route schedule, starts Kafka Streams, Flink, and Spark, produces live simulated city
  data, runs the dead-letter queue validation flow, runs the priority consumer demonstration, and saves evidence logs automatically.”

  [Optional: show the command in the script.]

  sed -n '1,220p' scripts/run_demo.sh

  Say:

  “The normal demonstration uses short windows so the video can complete quickly. For the assignment’s required five-minute DLQ collection, we use the following command.”

  URBANPULSE_DEMO_DURATION=300 bash scripts/run_demo.sh

  Say:

  “This performs the same workflow but collects fresh DLQ evidence for five minutes.”

  ———

  ### 3. Kafka cluster and topics

  [Screen: Terminal output showing topic descriptions.]

  Say:

  “The platform runs Kafka as a three-broker KRaft cluster. KRaft means Kafka runs without ZooKeeper. The replication factor is three, and the minimum in-sync replica setting is two,
  which allows the cluster to tolerate one broker failure while maintaining durable writes.”

  [Screen: Kafka UI — http://localhost:8080.]

  Say:

  “In Kafka UI, we can see the active Kafka cluster and the topics used by the platform.”

  [Show topic list.]

  Say:

  “The four raw ingestion topics are:

  - urbanpulse.bus_gps
  - urbanpulse.traffic_signals
  - urbanpulse.air_quality
  - urbanpulse.smart_meters

  The platform also creates derived topics:

  - urbanpulse.route_schedule for compacted route reference data
  - urbanpulse.bus_enriched for enriched bus GPS messages
  - urbanpulse.incidents for Flink incident alerts
  - urbanpulse.ward_energy_summary for Spark energy aggregates
  - urbanpulse.health_advisories for AQI health warnings
  - urbanpulse.dlq for invalid records”

  Emphasize:

  “The topic partition counts and retention periods are different because the streams have different throughput, ordering, operational, and regulatory requirements.”

  ———

  ### 4. Task B — producers and route enrichment

  [Screen: Kafka UI, open urbanpulse.bus_gps or urbanpulse.air_quality.]

  Say:

  “The project contains two assessed Kafka producers.

  The bus GPS producer uses the route ID as the Kafka key. This means all GPS records from the same route go to the same partition, preserving their order. It uses idempotence and
  acknowledgements from all in-sync replicas for reliable delivery.

  The air-quality producer implements at-least-once delivery. It can retry messages, and it handles null AQI values gracefully rather than crashing. Those invalid values become visible to
  downstream validation.”

  [Screen: Terminal / show route schedule step in script.]

  Say:

  “Before starting route enrichment, the script loads the route schedule into a compacted Kafka topic. The route ID is the key, so Kafka Streams can materialise this as a KTable.”

  [Screen: Kafka UI, topic urbanpulse.bus_enriched.]

  Say:

  “The route-enrichment service performs a KStream-KTable left join. Each live bus GPS record is enriched with route name, terminal, and scheduled arrival time.”

  [Open one enriched JSON message.]

  Say:

  “This is an enriched message. We can see the original bus ID and route ID, plus the added route name, terminal, scheduled arrival time, and the join status. This demonstrates a real
  Kafka Streams join rather than a static lookup in Python.”

  ———

  ### 5. Task B — DLQ validation and report

  [Screen: Terminal.]

  RUN_LOG="$(ls -td logs/demo-* | head -n 1)"
  cat "$RUN_LOG/dlq-report.log"

  Say:

  “The platform validates every raw event through a dead-letter queue router. Invalid data is not silently ignored. Instead, the router attaches an error reason, source topic, partition,
  offset, timestamp, and original payload, then writes the invalid event to urbanpulse.dlq.”

  [Show the generated DLQ report.]

  Say:

  “The report shows the distribution of invalid records collected during this run. The evidence includes multiple validation categories.”

  [Point to output.]

  Say:

  “In this run, we can see:

  - NULL_AQI for air-quality readings without a valid AQI value
  - NULL_WAIT for traffic-signal readings without wait time
  - IMPOSSIBLE_GPS for invalid bus positions
  - VOLTAGE_OUT_OF_RANGE for invalid smart-meter readings

  The report also groups these rejections by source stream. This makes data quality observable and auditable.”

  Emphasize:

  “The script uses a fresh-only report mode. That means the five-minute report contains records arriving during the current demonstration, instead of mixing in historical DLQ data.”

  ———

  ### 6. Task B — priority consumer architecture

  [Screen: Terminal.]

  cat "$RUN_LOG/priority-consumers.log"

  Say:

  “This is the priority-consumer demonstration for traffic signals.

  The same Kafka topic is consumed by two separate consumer groups.

  The high-priority group has one fast consumer and is intended for real-time traffic signal control. The standard-priority group has slower consumers and represents an analytics
  dashboard.”

  [Point to HIGH lag and STD lag columns.]

  Say:

  “The important result is that high-priority lag stays near zero or in single digits, while standard-priority lag grows. This proves the critical traffic-control path is isolated from
  the slower analytics path.

  Because consumer groups have independent offsets, a slow dashboard cannot block a high-priority real-time control system.”

  ———

  ### 7. Task C — Flink real-time incident detection

  [Screen: Flink dashboard — http://localhost:8081.]

  Say:

  “Now we move to Task C. This is the Flink dashboard. The running job is called urbanpulse-incident-detection.”

  [Show job status is RUNNING.]

  Say:

  “The Flink job consumes live events and detects three incident patterns using keyed state, event time, watermarks, and checkpoints.”

  [Optional terminal.]

  docker compose exec -T flink-jobmanager flink list

  Say:

  “The first detector identifies AQI emergencies. When AQI exceeds 300, it emits a hazardous alert. It also applies a per-sensor cooldown to avoid repeated alert spam.

  The second detector identifies traffic gridlock. It tracks each junction and emits an alert after three consecutive cycles with average wait time above 180 seconds.

  The third detector identifies bus bunching. It tracks buses by route. If two buses remain within 200 metres for more than five minutes, the job emits a bunching incident.”

  [Explain demo setting.]

  Say:

  “For a short demonstration, the script uses a 30-second bunching threshold. The committed default remains the required five-minute threshold.”

  [Screen: Kafka UI topic urbanpulse.incidents, or terminal.]

  docker compose exec broker1 kafka-console-consumer \
    --bootstrap-server broker1:19092 \
    --topic urbanpulse.incidents \
    --from-beginning --timeout-ms 5000 --max-messages 10

  Say:

  “Here we can see incident messages written to the incidents topic. They include the incident type, affected route or zone, event timestamp, and useful operational details.”

  ———

  ### 8. Task C — Spark ward energy analytics

  [Screen: Terminal.]

  cat "$RUN_LOG/spark-ward-energy.log"

  Say:

  “The first Spark Structured Streaming job reads smart-meter events and computes ward-level energy analytics.

  For each ward and time window, it calculates total kilowatt-hour consumption, average power factor, and peak voltage.”

  [Point to columns in output table.]

  Say:

  “The job uses event-time processing and watermarks, so late but valid data can still be included in the correct window. Invalid meter values, such as impossible voltage or power-factor
  values, are filtered from the analytical output.”

  [Explain dual sink.]

  Say:

  “Each completed window is written to two outputs from the same checkpointed streaming query.

  First, it publishes a JSON summary to the Kafka topic urbanpulse.ward_energy_summary.

  Second, it writes partitioned Parquet files under data/parquet/ward_energy, partitioned by ward ID and date. This supports historical reporting and analytical queries.”

  Emphasize:

  “The script uses short demonstration windows. The actual committed defaults are a 15-minute tumbling window with a 45-minute watermark, as required.”

  ———

  ### 9. Task C — Spark AQI health advisories

  [Screen: Terminal.]

  cat "$RUN_LOG/spark-health-advisory.log"

  Say:

  “The second Spark job performs streaming SQL for AQI health advisories.

  It computes a rolling average AQI by zone, joins the stream with the static zone profile table, and filters for unhealthy conditions where the rolling AQI is above 150.”

  [Point to fields: zone, zone name, population, schools, AQI, advisory level.]

  Say:

  “The zone profile enriches each advisory with the zone name, population, and number of schools. This means city officers receive an actionable advisory instead of only a raw AQI number.

  For example, they can see which zone is unhealthy, how many people may be exposed, and how many schools may be affected.”

  Emphasize:

  “The job uses Update mode, so it republishes the advisory as the rolling AQI changes. The fast demo uses a 30-second rolling window and five-second update interval. The committed
  production defaults are a ten-minute rolling window updated every minute.”

  ———

  ### 10. Architecture and report

  [Screen: Open docs/UrbanPulse_Report.pdf.]

  Say:

  “The combined report documents the complete solution.”

  [Show architecture diagram.]

  Say:

  “The architecture uses a lean Lambda design.

  The speed layer uses Flink and Spark Structured Streaming for low-latency operational actions such as incident detection, AQI advisories, and ward-level energy insight.

  The batch layer uses immutable Parquet data for reproducible and auditable government reporting.

  The report also compares Lambda and Kappa architectures, explains storage technology choices, includes a government smart-city readiness checklist, documents the Kafka implementation,
  and explains why Flink and Spark are used for different workloads.”

  ———

  ### 11. Closing

  [Screen: Terminal, README, or final report.]

  Say:

  “To reproduce the full project, run:

  git pull origin main
  bash scripts/run_demo.sh

  For the required five-minute DLQ collection, run:

  URBANPULSE_DEMO_DURATION=300 bash scripts/run_demo.sh

  All generated evidence is saved under logs/demo-<timestamp>/, and the detailed manual instructions are available in docs/RUNBOOK.md.

  Thank you.”
