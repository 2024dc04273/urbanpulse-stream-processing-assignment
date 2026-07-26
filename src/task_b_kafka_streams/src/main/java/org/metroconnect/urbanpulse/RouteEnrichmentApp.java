package org.metroconnect.urbanpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Task B's actual Kafka Streams KStream-KTable join.
 *
 * <p>The compacted urbanpulse.route_schedule topic is materialized as a KTable
 * keyed by route_id.  The bus GPS producer uses the same key, so every live
 * record is enriched with its latest schedule value before being sent to
 * urbanpulse.bus_enriched.</p>
 */
public final class RouteEnrichmentApp {
  private static final String BUS_GPS = "urbanpulse.bus_gps";
  private static final String ROUTE_SCHEDULE = "urbanpulse.route_schedule";
  private static final String BUS_ENRICHED = "urbanpulse.bus_enriched";
  private static final ObjectMapper JSON = new ObjectMapper();

  private RouteEnrichmentApp() {}

  public static void main(String[] args) {
    String bootstrap = System.getenv().getOrDefault(
        "URBANPULSE_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096");

    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "urbanpulse-route-enrichment-v1");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    StreamsBuilder builder = new StreamsBuilder();
    KTable<String, String> schedules = builder.table(
        ROUTE_SCHEDULE,
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as("urbanpulse-route-schedule-store"));
    KStream<String, String> busPositions = builder.stream(
        BUS_GPS, Consumed.with(Serdes.String(), Serdes.String()));

    busPositions
        .leftJoin(schedules, RouteEnrichmentApp::enrich)
        .to(BUS_ENRICHED, Produced.with(Serdes.String(), Serdes.String()));

    Topology topology = builder.build();
    System.out.println("[kafka-streams] KStream(bus_gps) LEFT JOIN KTable(route_schedule) "
        + "-> " + BUS_ENRICHED);
    KafkaStreams streams = new KafkaStreams(topology, properties);
    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> streams.close(Duration.ofSeconds(15)), "route-enrichment-shutdown"));
    streams.start();
  }

  private static String enrich(String gpsJson, String scheduleJson) {
    try {
      JsonNode gps = JSON.readTree(gpsJson);
      ObjectNode result = gps.deepCopy();
      if (scheduleJson == null) {
        result.putNull("route_name");
        result.putNull("terminal");
        result.putNull("scheduled_arrival_time");
        result.put("join_status", "NO_ROUTE_MATCH");
      } else {
        JsonNode schedule = JSON.readTree(scheduleJson);
        copyScheduleField(schedule, result, "route_name");
        copyScheduleField(schedule, result, "terminal");
        copyScheduleField(schedule, result, "scheduled_arrival_time");
        result.put("join_status", "MATCHED");
      }
      return JSON.writeValueAsString(result);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Cannot enrich route record", ex);
    }
  }

  private static void copyScheduleField(JsonNode source, ObjectNode target, String field) {
    JsonNode value = source.get(field);
    if (value == null || value.isNull()) {
      target.putNull(field);
    } else {
      target.set(field, value);
    }
  }
}
