package sandbox.kafka.requestreply.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import sandbox.kafka.admin.TopicConfig;

/** Container for Kafka configuration properties. */
@Data
public class KafkaProperties {

  /* comma-separate list of Kafka broker URLs */
  private String brokers;

  /* schema registry URL */
  private String schemaRegistry;

  /* topic configurations to be managed by this application */
  private Topics topics;

  @Data
  public static class Topics {

    /* persistent data topic */
    private TopicConfig data;

    /* ephemeral response topic */
    private TopicConfig response;
  }

  /**
   * Get Kafka admin client configuration properties.
   *
   * @return configuration properties
   */
  public Map<String, Object> getAdminProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("client.id", "admin");
    properties.put("bootstrap.servers", brokers);
    return properties;
  }

  /**
   * Get Kafka consumer configuration properties.
   *
   * @param clientId consumer client id
   * @return configuration properties
   */
  public Map<String, Object> getConsumerProperties(String clientId) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("client.id", clientId);
    properties.put("bootstrap.servers", brokers);
    properties.put("key.deserializer", StringDeserializer.class);
    properties.put("value.deserializer", KafkaAvroDeserializer.class);
    properties.put("schema.registry.url", schemaRegistry);
    properties.put("schema.reflection", true);
    properties.put("auto.offset.reset", "earliest");
    return properties;
  }

  /**
   * Get Kafka producer configuration properties.
   *
   * @param clientId producer client id
   * @return configuration properties
   */
  public Map<String, Object> getProducerProperties(String clientId) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("client.id", clientId);
    properties.put("bootstrap.servers", brokers);
    properties.put("key.serializer", StringSerializer.class);
    properties.put("value.serializer", KafkaAvroSerializer.class);
    properties.put("value.subject.name.strategy", RecordNameStrategy.class);
    properties.put("schema.registry.url", schemaRegistry);
    properties.put("schema.reflection", true);
    return properties;
  }
}
