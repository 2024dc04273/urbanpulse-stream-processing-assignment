# UrbanPulse Flink image — Flink 1.20 + PyFlink + Kafka connector.
# Used for the jobmanager, the taskmanager, and job submission, so the Python
# UDFs (KeyedProcessFunctions) run identically on the cluster.
#
# Built NATIVELY (arm64 on Apple Silicon): PyFlink's `pemja` bridge has no arm64
# wheel, and an amd64 image under QEMU segfaults the JVM. So we compile pemja
# from source here — that needs a full JDK (for jni.h) + a C toolchain — and the
# resulting image runs on the host's native architecture, stably.
FROM flink:1.20.0-scala_2.12-java11

# Toolchain to compile pemja: JDK (jni.h), Python headers, gcc.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev python-is-python3 \
        openjdk-11-jdk-headless build-essential && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME → the JDK just for the build (jni.h); Flink's own JRE is used at
# runtime via the image's default JAVA_HOME. Arch suffix resolved dynamically.
RUN JDK_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    echo "Building pemja against JAVA_HOME=$JDK_HOME" && \
    JAVA_HOME="$JDK_HOME" pip3 install --no-cache-dir apache-flink==1.20.0

# Kafka SQL connector (fat jar) → picked up from /opt/flink/lib on the classpath.
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

ENV FLINK_KAFKA_JAR=/opt/flink/lib/flink-sql-connector-kafka.jar \
    PYTHONPATH=/opt/job
