# UrbanPulse — convenience targets. Run `make help` for the list.
# Everything runs in containers, so Docker Desktop is the only prerequisite.

.DEFAULT_GOAL := help
COMPOSE := docker compose
EXEC_APP := $(COMPOSE) exec app python -m
SPARK_SUBMIT := $(COMPOSE) exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

.PHONY: help up down clean topics feed \
        b-producers b-priority b-enrich b-dlq \
        c-flink c-ward c-advisory

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

## ---- platform ----
up:  ## Start Kafka cluster + UI + app toolbox
	$(COMPOSE) up -d

down:  ## Stop everything (keep data)
	$(COMPOSE) --profile flink --profile spark down

clean:  ## Stop everything and wipe volumes + parquet
	$(COMPOSE) --profile flink --profile spark down -v
	rm -rf data/parquet/ward_energy

topics:  ## Create all Kafka topics (partitions + retention)
	$(COMPOSE) exec app python -m src.task_b_kafka.create_topics

feed:  ## Start feeding all 4 simulated streams (background)
	$(COMPOSE) exec -d app python -m src.simulators.run_simulator --all
	@echo "Feeding all streams. Kafka-UI: http://localhost:8080"

## ---- Task B ----
b-producers:  ## Run bus_gps + air_quality producers (60s each)
	$(EXEC_APP) src.task_b_kafka.producer_bus_gps --rate 120 --duration 60
	$(EXEC_APP) src.task_b_kafka.producer_air_quality --rate 20 --duration 60

b-priority:  ## Priority-consumer demo (HIGH vs STANDARD lag)
	$(EXEC_APP) src.task_b_kafka.priority_consumers --role demo

b-enrich:  ## Load the route KTable and run the Kafka Streams enrichment join
	$(EXEC_APP) src.task_b_kafka.load_route_schedule
	$(COMPOSE) --profile streams up -d --build route-enrichment

b-dlq:  ## Run DLQ router (60s) then the 5-min report
	$(EXEC_APP) src.task_b_kafka.dlq_router --from-beginning --duration 60
	$(EXEC_APP) src.task_b_kafka.dlq_report

## ---- Task C ----
c-flink:  ## Flink incident detection (local mode; short bunching window)
	$(COMPOSE) --profile flink up -d
	$(COMPOSE) exec -e URBANPULSE_BUNCHING_SECONDS=45 flink-jobmanager \
		python /opt/job/src/task_c_flink_spark/flink_incident_detection.py

c-ward:  ## Spark ward energy (Kafka + partitioned Parquet)
	$(COMPOSE) --profile spark up -d
	$(SPARK_SUBMIT) /opt/job/src/task_c_flink_spark/spark_ward_energy.py --duration 180

c-advisory:  ## Spark health advisories (streaming SQL)
	$(COMPOSE) --profile spark up -d
	$(SPARK_SUBMIT) /opt/job/src/task_c_flink_spark/spark_health_advisory.py --duration 180
