# UrbanPulse Spark image — PySpark 3.5.1 on a JRE.
# Spark is JVM-based, so this runs natively on arm64 (no emulation needed). The
# spark-sql-kafka connector JAR is fetched at submit time via --packages.
# Pinned to bookworm: Debian trixie dropped openjdk-17, and Spark 3.5 supports
# Java 11/17 (not 21). Bookworm ships openjdk-17-jre-headless.
FROM python:3.11-slim-bookworm

# Java 17 (Spark 3.5 supports Java 11/17) + procps for spark scripts.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 \
    PYTHONPATH=/opt/job \
    URBANPULSE_BOOTSTRAP=broker1:19092,broker2:19092,broker3:19092 \
    PYTHONUNBUFFERED=1
# JAVA_HOME arch suffix differs amd64/arm64; resolve a stable symlink too.
RUN ln -s "$(dirname $(dirname $(readlink -f $(which java))))" /opt/java-home || true
ENV JAVA_HOME=/opt/java-home

WORKDIR /opt/job
CMD ["sleep", "infinity"]
