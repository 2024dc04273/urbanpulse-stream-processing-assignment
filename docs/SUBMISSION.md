# UrbanPulse — Submission Guide & Video Walkthrough Outline

Per the assignment, the final submission is a single `.zip` containing: the source
code (one git repo), a combined PDF report (Tasks A + B + C), and a video
walkthrough. This document explains how to assemble all three.

---

## 1. Deliverable checklist

| Deliverable | Assignment requirement | In this repo |
|---|---|---|
| Task A | PDF reporting all three points | `docs/task_a_architecture.md` |
| Task B | Report + code (git repo link) | `docs/task_b_kafka.md` + `src/task_b_kafka/` |
| Task C | Report + code (git repo link) | `docs/task_c_flink_spark.md` + `src/task_c_flink_spark/` |
| Combined PDF | All of A, B, C | build from the three docs (see §2) |
| Video | End-to-end working demo, voice-over | outline in §4 |
| Zip | All deliverables | see §3 |

---

## 2. The combined report (PDF + Word .docx)

The final upload report is **`docs/UrbanPulse_Report.pdf`**; the matching editable
source is **`docs/UrbanPulse_Report.docx`**. Both contain a title page and all of
Tasks A, B and C, with the architecture diagram and task page breaks.

**Regenerate it** (after editing any `docs/task_*.md`):
```bash
python3 scripts/build_docx.py        # → docs/UrbanPulse_Report.docx
python3 scripts/build_pdf.py         # → docs/UrbanPulse_Report.pdf
```
`build_docx.py` renders the Task A Mermaid diagram to `docs/diagrams/architecture.png`
with headless Chrome, then converts the concatenated Markdown to .docx with
**pandoc** (`brew install pandoc`). The Markdown source of the combined report is
also kept at `docs/UrbanPulse_Combined_Report.md`.

> The report already embeds the *verified run outputs* (lag tables, DLQ
> distribution, ward-energy windows, health advisories, incident alerts), so it is
> evidence-complete on its own. Re-run both builders after changing task reports.

---

## 3. Packaging the zip

```bash
# from the repo root — exclude generated/runtime artifacts
git archive --format=zip -o UrbanPulse_Submission.zip HEAD
# (or, if not committing) a clean copy without volumes/caches:
zip -r UrbanPulse_Submission.zip . \
    -x '*.git*' -x 'data/parquet/*' -x '*/__pycache__/*' -x '*.log'
```
The committed repository contains the combined report PDF and Word source. Add
the recorded video file to the zip before final upload to eLearn.

---

## 4. Video walkthrough script (~9–11 min, with voice-over)

Record a terminal beside Kafka-UI (`http://localhost:8080`) and narrate the
following. Use the shortened demonstration windows shown below; the code and
report retain the required 15-minute / 45-minute production defaults.

1. **0:00–0:45 — Objective and architecture.** Show page 1 of
   `UrbanPulse_Report.pdf`, then the architecture diagram. Say: “UrbanPulse
   combines Kafka, Flink and Spark for MetroConnect. I chose lean Lambda because
   statutory weekly/monthly reports need immutable, reproducible batch data as
   well as real-time response.”
2. **0:45–1:30 — Kafka cluster.** Run `docker compose up -d`, open Kafka-UI and
   show the three brokers. Say: “This is a three-broker KRaft cluster with
   replication factor three and minimum in-sync replicas two, so one broker can
   fail without losing acknowledged data.”
3. **1:30–2:10 — Topics and retention.** Run
   `docker compose exec app python -m src.task_b_kafka.create_topics`, then show
   `urbanpulse.bus_gps`, `urbanpulse.air_quality` and
   `urbanpulse.smart_meters` in Kafka-UI. Say why their retentions are 24 hours,
   90 days and 365 days respectively.
4. **2:10–3:05 — Producer guarantees.** Run the bus and air-quality producers.
   Point out that `route_id` is the bus-message key and that the bus producer is
   idempotent. Point out a logged null AQI in the air producer and explain that
   it is accepted at-least-once, logged, then handled by the DLQ.
5. **3:05–3:50 — Priority consumers.** Run
   `docker compose exec app python -m src.task_b_kafka.priority_consumers --role demo`.
   Keep the lag table visible. Say: “The high-priority signal-control group has
   its own offsets and stays near zero while the deliberately slowed standard
   analytics group accumulates lag.”
6. **3:50–4:40 — Actual Kafka Streams enrichment.** Run
   `docker compose exec app python -m src.task_b_kafka.load_route_schedule` then
   `docker compose --profile streams up -d --build route-enrichment`. In Kafka-UI,
   show `urbanpulse.route_schedule` and `urbanpulse.bus_enriched`. Say: “The
   compacted route-schedule topic becomes a Kafka Streams KTable. The application
   left-joins it to the keyed bus GPS KStream and adds route name, terminal and
   scheduled arrival time.”
7. **4:40–5:20 — DLQ.** Start `dlq_router` before the feeder, then run it for a
   minute and execute `dlq_report`. Show multiple validation reasons. Say: “Bad
   records are preserved with their source topic, partition, offset and a
   machine-readable error reason instead of being silently dropped.”
8. **5:20–6:45 — Flink incidents.** Start the all-stream simulator, bring up the
   Flink profile and run the Flink job with
   `URBANPULSE_BUNCHING_SECONDS=45`. Show all three types in
   `urbanpulse.incidents`. Say: “AQI uses sensor-keyed state, gridlock counts
   three consecutive junction cycles, and bunching uses route-keyed state plus a
   checkpointed event-time timer. Watermarks make the timing correct for slightly
   out-of-order events.”
9. **6:45–8:00 — Spark ward energy.** Start the Spark profile and run the ward
   job with `--window "30 seconds" --watermark "10 seconds" --duration 120` for
   the recording. Show console output, the Kafka topic and the Parquet path
   partitioned by `ward_id/date`. Say: “In production these are 15-minute
   tumbling windows with a 45-minute late-data watermark.”
10. **8:00–9:00 — Spark AQI advisory.** Run the advisory job with
    `--window "1 minute" --slide "10 seconds" --watermark "20 seconds" --duration 120`.
    Show enriched `UNHEALTHY` messages. Say: “This is a true sliding ten-minute
    production average, joined to static zone population and school data, and
    written in Update mode.”
11. **9:00–10:00 — Close.** Return to the report comparison table. Say: “Flink
    fits per-event, stateful incident detection and the fast recovery it needs;
    Spark fits the SQL-centric, windowed ward aggregates and Parquet history.
    The source, report PDF and reproducible Docker commands are in one Git repo.”

**Recording tip:** the repo includes `mcp`-free plain scripts, so you can run each
step in a terminal split next to the Kafka-UI browser window for a clean capture.
