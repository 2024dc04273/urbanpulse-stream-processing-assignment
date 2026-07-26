"""
Task C · Part II — Spark Structured Streaming: ward energy analytics.

Consumes urbanpulse.smart_meters and computes, per ward_id per 15-minute
TUMBLING window (with a 45-minute late-data watermark):

    total_kwh_consumed = sum(kwh_reading)
    avg_power_factor   = avg(power_factor)
    peak_voltage       = max(voltage)

Dual sink (both required):
    1. Kafka  → urbanpulse.ward_energy_summary   (one JSON row per closed window)
    2. Parquet → data/parquet/ward_energy, partitioned by (ward_id, date)
                 for historical trend analysis.

Append output mode is used because a windowed aggregation with a watermark emits
each window's final result once the watermark passes the window end — which is
also the only mode the Parquet file sink supports.

Submit (spark profile up):
    docker compose exec spark spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        /opt/job/src/task_c_flink_spark/spark_ward_energy.py --duration 180

Demo tip: the 15-min window needs 15 min of (event-time) data. For a quick demo
shorten it:  --window "1 minute" --watermark "30 seconds"
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, from_json, max as smax, round as sround,
                                   struct, sum as ssum, to_date, to_json, window)
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

from src.common import config

BOOTSTRAP = os.environ.get("URBANPULSE_BOOTSTRAP", config.BOOTSTRAP_SERVERS_INTERNAL)

METER_SCHEMA = StructType([
    StructField("meter_id", StringType()),
    StructField("ward_id", StringType()),
    StructField("kwh_reading", DoubleType()),
    StructField("voltage", DoubleType()),
    StructField("power_factor", DoubleType()),
    StructField("timestamp", LongType()),        # epoch millis
])


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse Spark ward energy")
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--window", default=os.environ.get("URBANPULSE_WINDOW", "15 minutes"))
    ap.add_argument("--watermark", default=os.environ.get("URBANPULSE_WATERMARK", "45 minutes"))
    ap.add_argument("--parquet", default=config.PARQUET_OUTPUT_DIR)
    ap.add_argument("--checkpoint", default="/tmp/ck/ward_energy")
    ap.add_argument("--duration", type=float, default=0, help="seconds then stop (0=forever)")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("urbanpulse-ward-energy")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", config.TOPIC_SMART_METERS)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.select(from_json(col("value").cast("string"), METER_SCHEMA).alias("d"))
              .select("d.*")
              # Sanity filter: drop injected sensor faults so councillor
              # aggregates aren't polluted (the DLQ router captures them
              # separately for investigation).
              .filter(col("kwh_reading").isNotNull() & (col("kwh_reading") >= 0))
              .filter((col("voltage") >= 150) & (col("voltage") <= 300))
              .filter((col("power_factor") >= 0) & (col("power_factor") <= 1))
              .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")))

    agg = (parsed
           .withWatermark("event_time", args.watermark)
           .groupBy(window(col("event_time"), args.window), col("ward_id"))
           .agg(sround(ssum("kwh_reading"), 3).alias("total_kwh_consumed"),
                sround(avg("power_factor"), 4).alias("avg_power_factor"),
                sround(smax("voltage"), 1).alias("peak_voltage")))

    out = agg.select(
        col("ward_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage"),
    )

    print(f"[ward-energy] window={args.window} watermark={args.watermark} "
          f"→ Kafka:{config.TOPIC_WARD_ENERGY} + Parquet:{args.parquet}")

    # --- Sink 1: Kafka (append) ---
    q_kafka = (out.select(col("ward_id").alias("key"), to_json(struct("*")).alias("value"))
               .writeStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("topic", config.TOPIC_WARD_ENERGY)
               .option("checkpointLocation", args.checkpoint + "/kafka")
               .outputMode("append").start())

    # --- Sink 2: Parquet partitioned by ward_id + date (append) ---
    q_parquet = (out.withColumn("date", to_date("window_start"))
                 .writeStream.format("parquet")
                 .option("path", args.parquet)
                 .option("checkpointLocation", args.checkpoint + "/parquet")
                 .partitionBy("ward_id", "date")
                 .outputMode("append").start())

    # --- Sink 3: console (demo visibility) ---
    q_console = (out.writeStream.format("console")
                 .option("truncate", "false").option("numRows", "20")
                 .outputMode("append").start())

    if args.duration:
        spark.streams.awaitAnyTermination(args.duration)
        for q in (q_kafka, q_parquet, q_console):
            q.stop()
        print("[ward-energy] stopped after duration")
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
