"""
Task C · Part II — Spark Structured Streaming SQL: AQI health advisories.

A Streaming SQL query on urbanpulse.air_quality that:
  (a) computes a 10-minute rolling average AQI per zone (updated each minute),
  (b) joins the result with a static zone_profile table (zone name, population,
      number of schools) to produce an enriched advisory,
  (c) filters for rolling_avg_aqi > 150 (Unhealthy) and writes to
      urbanpulse.health_advisories,
using **Update** output mode (so a zone's advisory is re-emitted as its rolling
average changes within the window).

The core logic is expressed as an actual SQL string (spark.sql over temp views),
satisfying the "Streaming SQL query" requirement. A stream-static join enriches
each windowed zone average with population/school context so a ward officer sees
"Unhealthy air over 62,000 people and 110 schools", not just a number.

Submit (spark profile up):
    docker compose exec spark spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        /opt/job/src/task_c_flink_spark/spark_health_advisory.py --duration 180

Demo tip: shorten the rolling window to see output fast:  --window "2 minutes"
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

from src.common import config

BOOTSTRAP = os.environ.get("URBANPULSE_BOOTSTRAP", config.BOOTSTRAP_SERVERS_INTERNAL)

AQ_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone", StringType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("aqi", DoubleType()),
    StructField("timestamp", LongType()),
])


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanPulse Spark health advisories")
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--window", default=os.environ.get("URBANPULSE_AQI_WINDOW", "10 minutes"))
    ap.add_argument("--slide", default=os.environ.get("URBANPULSE_AQI_SLIDE", "1 minute"),
                    help="sliding-window trigger interval; must be no longer than --window")
    ap.add_argument("--watermark", default=os.environ.get("URBANPULSE_AQI_WATERMARK", "5 minutes"))
    ap.add_argument("--zone-profile", default=config.ZONE_PROFILE_CSV)
    ap.add_argument("--checkpoint", default="/tmp/ck/health_advisory")
    ap.add_argument("--duration", type=float, default=0)
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("urbanpulse-health-advisory")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Static join table: zone_profile (zone, zone_name, population, num_schools)
    zone_profile = spark.read.option("header", True).csv(args.zone_profile)
    zone_profile.createOrReplaceTempView("zone_profile")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", config.TOPIC_AIR_QUALITY)
           .option("startingOffsets", "latest")
           .load())

    # Parse + event time, drop null-AQI (handled by the Task B DLQ), register view.
    (raw.select(from_json(col("value").cast("string"), AQ_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("aqi").isNotNull())
        .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
        .withWatermark("event_time", args.watermark)
        .createOrReplaceTempView("air_quality"))

    # (a) Rolling = a 10-minute *sliding* window, recomputed every minute.
    # A one-argument window() would be tumbling, not the required rolling average.
    # (b) Join zone_profile, (c) filter for the unhealthy threshold.
    advisories = spark.sql(f"""
        WITH rolled AS (
            SELECT window(event_time, '{args.window}', '{args.slide}') AS w,
                   zone,
                   round(avg(aqi), 1) AS rolling_avg_aqi,
                   count(*)          AS readings
            FROM air_quality
            GROUP BY window(event_time, '{args.window}', '{args.slide}'), zone
        )
        SELECT r.zone,
               z.zone_name,
               CAST(z.population AS INT)  AS population,
               CAST(z.num_schools AS INT) AS num_schools,
               r.w.start AS window_start,
               r.w.end   AS window_end,
               r.rolling_avg_aqi,
               r.readings,
               'UNHEALTHY' AS advisory_level
        FROM rolled r
        JOIN zone_profile z ON r.zone = z.zone
        WHERE r.rolling_avg_aqi > {config.AQI_UNHEALTHY}
    """)

    print(f"[health-advisory] rolling avg AQI window={args.window} slide={args.slide} > "
          f"{config.AQI_UNHEALTHY} → {config.TOPIC_HEALTH_ADVISORIES} (Update mode)")

    q_kafka = (advisories
               .select(col("zone").alias("key"), to_json(struct("*")).alias("value"))
               .writeStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("topic", config.TOPIC_HEALTH_ADVISORIES)
               .option("checkpointLocation", args.checkpoint + "/kafka")
               .outputMode("update").start())

    q_console = (advisories.writeStream.format("console")
                 .option("truncate", "false").option("numRows", "20")
                 .outputMode("update").start())

    if args.duration:
        spark.streams.awaitAnyTermination(args.duration)
        for q in (q_kafka, q_console):
            q.stop()
        print("[health-advisory] stopped after duration")
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
