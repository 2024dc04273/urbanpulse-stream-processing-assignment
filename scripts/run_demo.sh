#!/usr/bin/env bash
# UrbanPulse one-command end-to-end demo.
#
# Usage from the repository root:
#   bash scripts/run_demo.sh
#
# Optional environment variables:
#   URBANPULSE_DEMO_DURATION=300  # seconds; 300 is the assessed five-minute run
#   URBANPULSE_SIM_RATE=200       # events/sec for each simulated source
#   URBANPULSE_BUNCHING_SECONDS=30 # short threshold for a fast Flink demo

set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

demo_seconds="${URBANPULSE_DEMO_DURATION:-120}"
sim_rate="${URBANPULSE_SIM_RATE:-200}"
bunching_seconds="${URBANPULSE_BUNCHING_SECONDS:-30}"
run_id="$(date +%Y%m%d-%H%M%S)"
log_dir="logs/demo-${run_id}"
mkdir -p "$log_dir"

if ! [[ "$demo_seconds" =~ ^[0-9]+$ ]] || (( demo_seconds < 30 )); then
  echo "URBANPULSE_DEMO_DURATION must be a whole number of at least 30 seconds." >&2
  exit 2
fi
if ! [[ "$sim_rate" =~ ^[0-9]+$ ]] || (( sim_rate < 1 )); then
  echo "URBANPULSE_SIM_RATE must be a positive whole number." >&2
  exit 2
fi

background_pids=()
cleanup() {
  local process_id
  for process_id in "${background_pids[@]:-}"; do
    kill "$process_id" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

wait_for_command() {
  local description="$1"
  shift
  local attempt
  for attempt in $(seq 1 30); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${description}." >&2
  return 1
}

start_background() {
  local name="$1"
  shift
  echo "Starting ${name}..."
  "$@" >"${log_dir}/${name}.log" 2>&1 &
  background_pids+=("$!")
}

wait_for_background() {
  local name="$1"
  local process_id="$2"
  if ! wait "$process_id"; then
    echo "${name} failed. See ${log_dir}/${name}.log" >&2
    return 1
  fi
}

echo "==> Starting the Kafka platform"
docker compose up -d
wait_for_command "the Python toolbox and Kafka brokers" \
  docker compose exec -T app python -m src.task_b_kafka.create_topics

echo "==> Creating/describing Kafka topics"
docker compose exec -T app python -m src.task_b_kafka.create_topics --describe \
  | tee "${log_dir}/topics.log"

echo "==> Loading route schedule before starting the KStream-KTable join"
docker compose exec -T app python -m src.task_b_kafka.load_route_schedule \
  | tee "${log_dir}/route-schedule.log"

echo "==> Starting Kafka Streams, Flink, and Spark services"
docker compose --progress quiet --profile streams --profile flink --profile spark up -d --build
wait_for_command "the Flink JobManager" docker compose exec -T flink-jobmanager flink list

# Do not use grep -q here: it can close the pipe before the Flink CLI has
# finished writing, which makes pipefail treat a healthy running job as absent.
if docker compose exec -T flink-jobmanager flink list | grep "RUNNING" >/dev/null; then
  echo "==> A Flink job is already running; reusing it."
else
  echo "==> Submitting the Flink incident-detection job"
  docker compose exec -T -e "URBANPULSE_BUNCHING_SECONDS=${bunching_seconds}" \
    flink-jobmanager flink run -d -py \
    /opt/job/src/task_c_flink_spark/flink_incident_detection.py \
    | tee "${log_dir}/flink-submit.log"
fi

echo "==> Collecting ${demo_seconds}s of fresh DLQ events and four live source streams"
start_background dlq-router \
  docker compose exec -T app python -m src.task_b_kafka.dlq_router --duration "$demo_seconds"
start_background dlq-report \
  docker compose exec -T app python -m src.task_b_kafka.dlq_report \
  --from-latest --window "$demo_seconds"

# Let both fresh consumer groups receive their partition assignments before
# source events arrive; otherwise a very short demonstration can start before
# the report has established its "latest" offsets.
sleep 3
start_background simulator \
  docker compose exec -T app python -m src.simulators.run_simulator --all \
  --rate "$sim_rate" --duration "$demo_seconds"

echo "==> Running the priority-consumer lag demonstration"
start_background priority-consumers \
  docker compose exec -T app python -m src.task_b_kafka.priority_consumers \
  --role demo --duration 45

echo "==> Running both bounded Spark analytics jobs"
start_background spark-ward-energy \
  docker compose exec -T spark spark-submit \
  --conf "spark.jars.ivy=/tmp/ivy/ward_energy_${run_id}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/job/src/task_c_flink_spark/spark_ward_energy.py \
  --window "15 seconds" --watermark "5 seconds" \
  --checkpoint "/tmp/ck/ward_energy_${run_id}" --duration "$demo_seconds"
start_background spark-health-advisory \
  docker compose exec -T spark spark-submit \
  --conf "spark.jars.ivy=/tmp/ivy/health_advisory_${run_id}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/job/src/task_c_flink_spark/spark_health_advisory.py \
  --window "30 seconds" --slide "5 seconds" --watermark "5 seconds" \
  --checkpoint "/tmp/ck/health_advisory_${run_id}" --duration "$demo_seconds"

# The simulator and DLQ report deliberately run for the requested collection
# period. Spark is bounded to the same period; all output is kept in log_dir.
for process_id in "${background_pids[@]}"; do
  wait_for_background "background task" "$process_id"
done
background_pids=()

echo
echo "==> Verification"
docker compose exec -T flink-jobmanager flink list | tee "${log_dir}/flink-list.log"
docker compose exec -T broker1 kafka-console-consumer \
  --bootstrap-server broker1:19092 --topic urbanpulse.incidents \
  --from-beginning --timeout-ms 3000 --max-messages 10 \
  | tee "${log_dir}/incidents-sample.log" || true

echo
echo "UrbanPulse demo completed. Evidence logs: ${log_dir}"
echo "Kafka UI: http://localhost:8080  |  Flink UI: http://localhost:8081"
echo "Docker services and the Flink incident-detection job remain running."
