# UrbanPulse "toolbox" image — runs simulators, producers, consumers, DLQ.
# Pinned to Python 3.11 so confluent-kafka wheels resolve cleanly regardless of
# the host Python version.
FROM python:3.11-slim

WORKDIR /app

# Only the light-weight client deps go in this image (Spark/Flink run in their
# own containers). Keeps the toolbox small and fast to build.
RUN pip install --no-cache-dir \
        confluent-kafka==2.5.3 \
        pandas==2.2.2 \
        tabulate==0.9.0

# Source is bind-mounted at runtime (see docker-compose.yml) so code edits are
# picked up without rebuilding. PYTHONPATH=/app lets `-m src.*` resolve.
ENV PYTHONPATH=/app \
    URBANPULSE_BOOTSTRAP=broker1:19092,broker2:19092,broker3:19092 \
    PYTHONUNBUFFERED=1

CMD ["bash"]
