"""
Task C · Part I — Apache Flink incident detection.

A single Flink DataStream job with three keyed-state, event-time detectors, all
writing alerts to the urbanpulse.incidents Kafka topic:

  (a) AQI Emergency  — key = sensor_id. Any reading with AQI > 300 (Hazardous)
      raises an alert; a per-sensor 2-minute event-time cooldown (ValueState)
      suppresses duplicate spam. Emitted well within the 2-minute SLA.

  (b) Traffic Gridlock — key = junction_id. A ValueState counter tracks
      CONSECUTIVE signal cycles with avg_wait_sec > 180; on the 3rd consecutive
      cycle a gridlock alert (junction_id, zone) is emitted, then the counter
      resets.

  (c) Bus Bunching — key = route_id. ValueState holds each bus's latest position
      and the event-time at which each bus PAIR first came within 200 m.  An
      event-time timer fires at the five-minute threshold, so the alert is not
      dependent on a further position event arriving after that point.

Event time comes from each event's `timestamp` field (epoch millis) with a
5-second bounded-out-of-orderness watermark. Checkpointing (30 s) makes the
keyed state recoverable.

Submit (from the repo root, cluster running with --profile flink):
    docker compose exec flink-jobmanager \
        flink run -py /opt/job/src/task_c_flink_spark/flink_incident_detection.py

Env knobs (for a quick demo, shorten the bunching window):
    URBANPULSE_BUNCHING_SECONDS=45
"""
from __future__ import annotations

import json
import os

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema,
                                                 KafkaSink, KafkaSource)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from src.common import config
from src.common.schemas import (haversine_metres, validate_air_quality,
                                validate_bus_gps, validate_traffic_signal)

BOOTSTRAP = os.environ.get("URBANPULSE_BOOTSTRAP", config.BOOTSTRAP_SERVERS_INTERNAL)
BUNCHING_SECONDS = int(os.environ.get("URBANPULSE_BUNCHING_SECONDS",
                                      config.BUNCHING_DURATION_SECONDS))

# Row layouts (positional access downstream).
AQI_ROW = Types.ROW_NAMED(["sensor_id", "zone", "aqi", "ts"],
                          [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])
SIG_ROW = Types.ROW_NAMED(["junction_id", "zone", "avg_wait", "ts"],
                          [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])
BUS_ROW = Types.ROW_NAMED(["bus_id", "route_id", "lat", "lon", "ts"],
                          [Types.STRING(), Types.STRING(), Types.DOUBLE(),
                           Types.DOUBLE(), Types.LONG()])


class TsAssigner(TimestampAssigner):
    """Event time = the `ts` field (last column) in epoch millis."""
    def __init__(self, idx: int):
        self.idx = idx

    def extract_timestamp(self, value, record_timestamp):
        return int(value[self.idx])


def _ws(idx: int) -> WatermarkStrategy:
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(TsAssigner(idx)))


# ---------------------------------------------------------------------------
# Parsers: JSON string -> typed Row (skip events missing critical fields; those
# are handled by the Task B DLQ, so the detectors see only clean data).
# ---------------------------------------------------------------------------
def parse_aqi(js: str):
    try:
        e = json.loads(js)
        if validate_air_quality(e) is not None:      # drop DLQ-bound events
            return []
        return [Row(e["sensor_id"], e["zone"], float(e["aqi"]), int(e["timestamp"]))]
    except Exception:  # noqa: BLE001
        return []


def parse_sig(js: str):
    try:
        e = json.loads(js)
        if validate_traffic_signal(e) is not None:
            return []
        return [Row(e["junction_id"], e["zone"], float(e["avg_wait_sec"]),
                    int(e["timestamp"]))]
    except Exception:  # noqa: BLE001
        return []


def parse_bus(js: str):
    try:
        e = json.loads(js)
        # Drop impossible GPS / speed etc. so corrupt points can't reset the
        # bunching proximity timer (those are captured by the Task B DLQ).
        if validate_bus_gps(e) is not None:
            return []
        return [Row(e["bus_id"], e["route_id"], float(e["lat"]), float(e["lon"]),
                    int(e["timestamp"]))]
    except Exception:  # noqa: BLE001
        return []


# ===========================================================================
# (a) AQI Emergency — keyed by sensor_id
# ===========================================================================
class AqiEmergency(KeyedProcessFunction):
    COOLDOWN_MS = 120_000  # 2 minutes

    def open(self, ctx):
        self.last_alert = ctx.get_state(
            ValueStateDescriptor("last_alert_ts", Types.LONG()))

    def process_element(self, value, ctx):
        sensor_id, zone, aqi, ts = value[0], value[1], value[2], value[3]
        if aqi > config.AQI_HAZARDOUS:
            last = self.last_alert.value()
            if last is None or (ts - last) > self.COOLDOWN_MS:
                self.last_alert.update(ts)
                yield json.dumps({
                    "incident_type": "AQI_EMERGENCY",
                    "severity": "HAZARDOUS",
                    "sensor_id": sensor_id, "zone": zone,
                    "aqi": round(aqi, 1), "event_ts": ts,
                    "detail": f"AQI {aqi:.0f} > 300 at {sensor_id} ({zone})",
                })


# ===========================================================================
# (b) Traffic Gridlock — keyed by junction_id
# ===========================================================================
class Gridlock(KeyedProcessFunction):
    def open(self, ctx):
        self.streak = ctx.get_state(
            ValueStateDescriptor("consecutive_cycles", Types.INT()))

    def process_element(self, value, ctx):
        junction_id, zone, avg_wait, ts = value[0], value[1], value[2], value[3]
        c = self.streak.value() or 0
        if avg_wait > config.GRIDLOCK_WAIT_SECONDS:
            c += 1
            if c >= config.GRIDLOCK_CONSECUTIVE_CYCLES:
                self.streak.update(0)     # reset after firing
                yield json.dumps({
                    "incident_type": "TRAFFIC_GRIDLOCK",
                    "junction_id": junction_id, "zone": zone,
                    "avg_wait_sec": round(avg_wait, 1),
                    "consecutive_cycles": c, "event_ts": ts,
                    "detail": f"{junction_id} ({zone}) avg_wait {avg_wait:.0f}s "
                              f"for {c} consecutive cycles",
                })
            else:
                self.streak.update(c)
        else:
            self.streak.update(0)         # streak broken


# ===========================================================================
# (c) Bus Bunching — keyed by route_id
# ===========================================================================
class BusBunching(KeyedProcessFunction):
    # Per-route state is kept as a single JSON blob in ONE ValueState. PyFlink's
    # ValueState round-trips reliably (see the gridlock counter); a MapState with
    # non-STRING or pipe-keyed entries did not persist across events in this
    # runtime, so we avoid it here.
    #   blob = {"pos": {bus_id: [lat, lon, ts]},
    #           "since": {pair: first-proximity-ts},
    #           "alerted": {pair: 1}}
    STALE_MS = 300_000   # drop positions not seen for 5 min (bounds state)

    def open(self, ctx):
        self.state = ctx.get_state(
            ValueStateDescriptor("bunch_state", Types.STRING()))

    def process_element(self, value, ctx):
        bus_id, route_id, lat, lon, ts = (value[0], value[1], value[2],
                                          value[3], value[4])
        st = json.loads(self.state.value() or "{}")
        pos = st.get("pos", {})
        since = st.get("since", {})
        alerted = st.get("alerted", {})

        pos[bus_id] = [lat, lon, ts]
        pos = {b: p for b, p in pos.items() if ts - p[2] <= self.STALE_MS}
        active_buses = set(pos)
        # A stale position cannot keep a proximity timer or alert flag alive.
        for pair in list(since):
            a, b = pair.split("|", 1)
            if a not in active_buses or b not in active_buses:
                since.pop(pair, None)
                alerted.pop(pair, None)

        for other_id, p in list(pos.items()):
            if other_id == bus_id:
                continue
            dist = haversine_metres(lat, lon, p[0], p[1])
            pair = "|".join(sorted((bus_id, other_id)))
            if dist < config.BUNCHING_DISTANCE_METRES:
                if pair not in since:
                    since[pair] = ts
                    # An event-time timer is checkpointed with keyed state.  It
                    # fires once the watermark moves past the threshold, even
                    # when the pair itself has not sent a later position update.
                    ctx.timer_service().register_event_time_timer(
                        ts + BUNCHING_SECONDS * 1000
                    )
            else:                                            # pair drifted apart
                since.pop(pair, None)
                alerted.pop(pair, None)

        self.state.update(json.dumps({"pos": pos, "since": since, "alerted": alerted}))

    def on_timer(self, timestamp, ctx):
        """Emit exactly once when an active pair reaches the event-time threshold."""
        st = json.loads(self.state.value() or "{}")
        pos = st.get("pos", {})
        since = st.get("since", {})
        alerted = st.get("alerted", {})
        route_id = ctx.get_current_key()

        for pair, first_seen in list(since.items()):
            if pair in alerted or first_seen + BUNCHING_SECONDS * 1000 != timestamp:
                continue
            bus_a, bus_b = pair.split("|", 1)
            a, b = pos.get(bus_a), pos.get(bus_b)
            if a is None or b is None:
                continue
            distance = haversine_metres(a[0], a[1], b[0], b[1])
            if distance < config.BUNCHING_DISTANCE_METRES:
                alerted[pair] = 1
                yield json.dumps({
                    "incident_type": "BUS_BUNCHING",
                    "route_id": route_id, "bus_a": bus_a, "bus_b": bus_b,
                    "distance_m": round(distance, 1),
                    "held_seconds": BUNCHING_SECONDS,
                    "event_ts": timestamp,
                    "detail": f"{bus_a} & {bus_b} on {route_id} within "
                              f"{distance:.0f}m for >{BUNCHING_SECONDS}s",
                })

        self.state.update(json.dumps({"pos": pos, "since": since, "alerted": alerted}))


# ===========================================================================
# Wiring
# ===========================================================================
def kafka_source(topic: str, group: str) -> KafkaSource:
    return (KafkaSource.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_topics(topic)
            .set_group_id(group)
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())


def build_sink() -> KafkaSink:
    return (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(config.TOPIC_INCIDENTS)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    # The Kafka connector is installed in /opt/flink/lib by infra/flink.Dockerfile,
    # which puts it on the JobManager and TaskManager classpaths.  Do not call
    # ``add_jars`` here: PyFlink 1.20 serialises that value as a Python list in
    # ``pipeline.jars`` and Flink then rejects the literal "['file:/...']" as a
    # malformed URL during submission.
    env.enable_checkpointing(30_000)   # recoverable keyed state every 30s

    # (a) AQI
    aqi = (env.from_source(kafka_source(config.TOPIC_AIR_QUALITY, "flink-aqi"),
                           WatermarkStrategy.no_watermarks(), "aqi-src")
           .flat_map(parse_aqi, output_type=AQI_ROW)
           .assign_timestamps_and_watermarks(_ws(3))
           .key_by(lambda r: r[0], key_type=Types.STRING())
           .process(AqiEmergency(), output_type=Types.STRING()))

    # (b) Gridlock
    grid = (env.from_source(kafka_source(config.TOPIC_TRAFFIC_SIGNALS, "flink-signals"),
                            WatermarkStrategy.no_watermarks(), "sig-src")
            .flat_map(parse_sig, output_type=SIG_ROW)
            .assign_timestamps_and_watermarks(_ws(3))
            .key_by(lambda r: r[0], key_type=Types.STRING())
            .process(Gridlock(), output_type=Types.STRING()))

    # (c) Bunching
    bunch = (env.from_source(kafka_source(config.TOPIC_BUS_GPS, "flink-bus"),
                             WatermarkStrategy.no_watermarks(), "bus-src")
             .flat_map(parse_bus, output_type=BUS_ROW)
             .assign_timestamps_and_watermarks(_ws(4))
             .key_by(lambda r: r[1], key_type=Types.STRING())
             .process(BusBunching(), output_type=Types.STRING()))

    incidents = aqi.union(grid, bunch)
    incidents.print()                    # also echo to the TaskManager log
    incidents.sink_to(build_sink())

    env.execute("urbanpulse-incident-detection")


if __name__ == "__main__":
    main()
