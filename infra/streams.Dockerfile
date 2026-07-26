# Task B Kafka Streams route-enrichment application.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY src/task_b_kafka_streams/pom.xml ./pom.xml
RUN mvn --batch-mode -q dependency:go-offline
COPY src/task_b_kafka_streams/src ./src
RUN mvn --batch-mode -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/urbanpulse-route-enrichment-1.0.0-jar-with-dependencies.jar /app/route-enrichment.jar
ENTRYPOINT ["java", "-jar", "/app/route-enrichment.jar"]
