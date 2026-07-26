# Task C — Flink Real-Time Incident Detection & Spark Urban Analytics Engine
**UrbanPulse** · DSE ZG556 Stream Processing & Analytics · Marks: 35 (M3)

> Code in `src/task_c_flink_spark/`. The Flink and Spark runtimes are provided as
> Docker images (`infra/flink.Dockerfile`, `infra/spark.Dockerfile`) wired into
> `docker-compose.yml` under the `flink` and `spark` profiles. Verified outputs
> are shown inline; reproduce with the commands in §C.5.

---

## C.0 Component Map

| # | Requirement | Module | Marks |
|---|---|---|---|
| I | Flink — 3 incident patterns, keyed state + event-time watermarks | `flink_incident_detection.py` | 10 |
| II.a | Spark — ward energy 15-min tumbling window, 45-min watermark, Kafka+Parquet | `spark_ward_energy.py` | 20 |
| II.b | Spark — 10-min rolling-avg AQI SQL, zone_profile join, Update mode | `spark_health_advisory.py` | (within II) |
| III | Flink vs Spark — use-case mapping across 4 dimensions | this doc §C.4 | 5 |

---

## C.1 Part I — Apache Flink Incident Detection (10 marks)

A single Flink DataStream job (`flink_incident_detection.py`) runs three
detectors, each keyed and using event-time watermarks (5 s bounded
out-of-orderness on the events' `timestamp` field). Checkpointing every 30 s
makes the keyed state recoverable. All alerts are unioned into one stream and
written to `urbanpulse.incidents`.

| Pattern | Key | State | Trigger |
|---|---|---|---|
| **(a) AQI Emergency** | `sensor_id` | `ValueState<Long>` last-alert time | `aqi > 300`; 2-min event-time cooldown suppresses duplicates |
| **(b) Traffic Gridlock** | `junction_id` | `ValueState<Int>` consecutive-cycle counter | `avg_wait_sec > 180` for **3 consecutive** cycles → alert(junction_id, zone), then reset |
| **(c) Bus Bunching** | `route_id` | `ValueState<String>` containing positions, pair-first-seen times and alert flags | two buses `< 200 m` apart for **> 5 min**; a checkpointed **event-time timer** emits alert(bus_a, bus_b) at the threshold |

**Why keyed state + event time here:** each detector's logic is intrinsically
per-entity and temporal. Gridlock's "3 consecutive cycles" is a per-junction
counter; bunching's "within 200 m for 5 minutes" is a per-route, per-pair
event-time timer registered through `ctx.timer_service()`. The timer is
checkpointed with the state and fires once the watermark passes the threshold,
so an alert does not depend on another event from that same pair arriving after
five minutes. Event-time watermarks keep the calculation correct despite
slightly out-of-order readings.

Events with null critical fields are dropped at parse time (they are handled by
the Task B DLQ), so the detectors operate on clean data.

**Verified output** — all three detectors fire and alerts are written to
`urbanpulse.incidents` (bunching window shortened to 20 s for the demo via
`URBANPULSE_BUNCHING_SECONDS`; the code default is the full 5 minutes):

```
=== incident types (one 70s demo run) ===
  11  AQI_EMERGENCY
  44  TRAFFIC_GRIDLOCK
   1  BUS_BUNCHING

=== one sample of each (JSON on urbanpulse.incidents) ===
{"incident_type":"AQI_EMERGENCY","severity":"HAZARDOUS","sensor_id":"AQ-Z02-02",
 "zone":"Z02-IndustrialEast","aqi":395.8,"detail":"AQI 396 > 300 at AQ-Z02-02 (Z02-IndustrialEast)"}
{"incident_type":"TRAFFIC_GRIDLOCK","junction_id":"JN-Z05-01","zone":"Z05-OldCity",
 "avg_wait_sec":220.8,"consecutive_cycles":3,"detail":"JN-Z05-01 (Z05-OldCity) avg_wait 221s for 3 consecutive cycles"}
{"incident_type":"BUS_BUNCHING","route_id":"R001","bus_a":"BUS-R001-00","bus_b":"BUS-R001-01",
 "distance_m":44.3,"held_seconds":20.0,"detail":"BUS-R001-00 & BUS-R001-01 on R001 within 44m for >20s"}

$ kafka-get-offsets --topic urbanpulse.incidents   →  264 incidents persisted
```

> **Keyed-state note (engineering detail).** The bunching detector keeps its
> per-route state (`positions`, per-pair proximity timers, alerted flags) in a
> single `ValueState<String>` JSON blob keyed by `route_id`, with 5-minute
> position pruning to bound the state. This proved more robust in PyFlink than a
> non-STRING-valued `MapState`. The gridlock and AQI detectors use
> `ValueState<Int>`/`ValueState<Long>` respectively.

---

## C.2 Part II — Spark Structured Streaming: Ward Energy (20 marks)

`spark_ward_energy.py` consumes `urbanpulse.smart_meters` and computes, per
`ward_id` per **15-minute tumbling window** with a **45-minute late-data
watermark**:

```python
parsed.withWatermark("event_time", "45 minutes")
      .groupBy(window("event_time", "15 minutes"), "ward_id")
      .agg(sum("kwh_reading")   .alias("total_kwh_consumed"),
           avg("power_factor")  .alias("avg_power_factor"),
           max("voltage")       .alias("peak_voltage"))
```

**Dual sink** (both required, two independent streaming queries on the same
aggregated DataFrame):

1. **Kafka** → `urbanpulse.ward_energy_summary` (one JSON row per closed window).
2. **Parquet** → `data/parquet/ward_energy`, **partitioned by `ward_id` and
   `date`** for historical trend analysis.

Append output mode is used: a windowed aggregation with a watermark emits each
window's final result once the watermark passes the window end — and append is
also the only mode the Parquet file sink supports. The 45-minute watermark means
a meter reading up to 45 minutes late is still counted in its correct window.

**Verified output** (demo window shortened to 30 s / 10 s watermark so windows
close quickly; the graded defaults are 15 min / 45 min). All 12 wards emit once
per closed window; injected sensor-fault voltages are filtered out so
`peak_voltage` stays in the real 218–242 V band:

```
| ward_id | window_start        | window_end          | total_kwh_consumed | avg_power_factor | peak_voltage |
|---------|---------------------|---------------------|-------------------:|-----------------:|-------------:|
| W01     | 2026-07-10 05:03:00 | 2026-07-10 05:03:30 |            17.709  |          0.9252  |       241.7  |
| W02     | 2026-07-10 05:03:00 | 2026-07-10 05:03:30 |            21.558  |          0.9228  |       241.9  |
| W03     | 2026-07-10 05:03:00 | 2026-07-10 05:03:30 |            19.775  |          0.9286  |       241.6  |
| …       |                     |                     |                    |                  |              |
| W12     | 2026-07-10 05:03:00 | 2026-07-10 05:03:30 |            13.772  |          0.9092  |       241.9  |
```
The same rows are simultaneously published as JSON to
`urbanpulse.ward_energy_summary` and appended to the Parquet lake below.

Partitioned Parquet written to disk (partitioned by `ward_id` then `date`):
```
data/parquet/ward_energy/
├── ward_id=W01/date=2026-07-10/part-00000-….snappy.parquet
├── ward_id=W02/date=2026-07-10/part-00002-….snappy.parquet
├── … (one directory per ward)                 → 36 parquet files across 12 wards
└── _spark_metadata/
```

### C.2.1 Streaming SQL — AQI Health Advisories (part of Part II)

`spark_health_advisory.py` is a **Streaming SQL query** on `urbanpulse.air_quality`:

- (a) a **10-minute rolling average AQI per zone**, implemented as a sliding
  `window(event_time, '10 minutes', '1 minute')`,
- (b) **joined with the static `zone_profile` table** (zone name, population,
  number of schools) — a stream-static join,
- (c) **filtered for `rolling_avg_aqi > 150`** (Unhealthy) and written to
  `urbanpulse.health_advisories` in **Update** output mode.

The logic is expressed as an actual SQL string over temp views (see the module),
so a ward officer receives "Unhealthy air over 620,000 people and 110 schools in
Residential North", not a bare number.

**Verified output** (demo rolling window shortened to 1 minute and recomputed
every 10 seconds) — enriched
advisories on `urbanpulse.health_advisories`, Update mode re-emitting as each
zone's rolling average evolves (note `readings` climbing 221 → 231 for the same
window):
```
| zone                 | zone_name             | population | num_schools | rolling_avg_aqi | readings | advisory_level |
|----------------------|-----------------------|-----------:|------------:|----------------:|---------:|----------------|
| Z06-Airport          | Airport & Aerotropolis|    190000  |         27  |          170.8  |     221  | UNHEALTHY      |
| Z03-ResidentialNorth | Residential North     |    620000  |        110  |          159.8  |     229  | UNHEALTHY      |
| Z06-Airport          | Airport & Aerotropolis|    190000  |         27  |          170.1  |     223  | UNHEALTHY      |
| Z03-ResidentialNorth | Residential North     |    620000  |        110  |          159.6  |     231  | UNHEALTHY      |
```
Only zones whose 10-minute rolling average exceeds 150 appear, each enriched with
the population and school count a ward officer needs to gauge exposure.

---

## C.4 Part III — Flink vs Spark for UrbanPulse (5 marks)

Mapping the two UrbanPulse use cases to the right engine, across the four
required dimensions.

| Dimension | **Bus Bunching → Apache Flink** | **Ward Energy → Apache Spark** |
|---|---|---|
| **State size** | Per-route keyed state holding each bus's latest position **plus a timer per bus-pair**, updated on **every** event. Many fine-grained, continuously-mutated keys — Flink's RocksDB keyed state with incremental checkpoints is built for exactly this. | A windowed aggregation keyed by ward (**only 12 wards**) over 15-min windows — small, bounded state. A classic, cheap windowed aggregation that Spark's structured state handles trivially. |
| **Latency requirement** | Bunching is a **live operational incident** — buses must be un-bunched quickly, so alerts need **event-at-a-time, sub-second** latency. Flink processes each event as it arrives. | Ward energy feeds **councillor dashboards refreshed every 15 minutes** — latency-relaxed. Spark's micro-batch (seconds) is far faster than needed; sub-second would be wasted effort. |
| **Recovery time objective** | Incident detection must resume fast after failure without losing the in-flight proximity timers. Flink's **incremental RocksDB checkpoints + exactly-once** restore large keyed state quickly → low RTO for a critical path. | A 15-min reporting job tolerates a slightly higher RTO; Spark checkpoints to reliable storage and simply reprocesses the current micro-batch on restart — perfectly adequate for non-critical analytics. |
| **Operational complexity** | The "within 200 m for > 5 min" rule is a per-pair, event-time state machine — **natural in a Flink `KeyedProcessFunction` with timers**, awkward in Spark (needs `flatMapGroupsWithState`). Flink is the simpler fit *for this logic*. | Windowed SUM/AVG/MAX + a **dual sink including partitioned Parquet** for a data lake is idiomatic, concise Spark SQL. Doing partitioned Parquet analytics output in Flink is more work. Spark is the simpler fit *for this logic*. |

**Conclusion.** UrbanPulse deliberately uses **both** engines, each where it is
strongest — which is exactly the *lean Lambda* architecture chosen in Task A:
**Flink for low-latency, fine-grained, event-time incident detection** (AQI,
gridlock, bunching) on the speed layer, and **Spark for windowed, SQL-friendly,
data-lake-producing analytical aggregation** (ward energy, health advisories).
Forcing either use case onto the other engine would increase either latency
(Spark for bunching) or code complexity (Flink for partitioned Parquet reports).

---

## C.5 How to Run (all of Task C)

```bash
# cluster + data
docker compose up -d
docker compose exec app python -m src.task_b_kafka.create_topics
docker compose exec -d app python -m src.simulators.run_simulator --all

# --- Flink incident detection (runs on the arm64-native Flink image) ---
docker compose --profile flink up -d
docker compose exec -e URBANPULSE_BUNCHING_SECONDS=45 flink-jobmanager \
    flink run -py /opt/job/src/task_c_flink_spark/flink_incident_detection.py
#   ...or local mode:  docker compose exec flink-jobmanager \
#       python /opt/job/src/task_c_flink_spark/flink_incident_detection.py

# --- Spark ward energy (dual sink) ---
docker compose --profile spark up -d
docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/job/src/task_c_flink_spark/spark_ward_energy.py --duration 180

# --- Spark health advisories (streaming SQL) ---
docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/job/src/task_c_flink_spark/spark_health_advisory.py \
    --window "1 minute" --slide "10 seconds" --watermark "20 seconds" --duration 180
```
Watch results live in Kafka-UI (`http://localhost:8080`) on the
`urbanpulse.incidents`, `urbanpulse.ward_energy_summary`, and
`urbanpulse.health_advisories` topics, and the Flink dashboard at
`http://localhost:8081`.

*End of Task C.*
