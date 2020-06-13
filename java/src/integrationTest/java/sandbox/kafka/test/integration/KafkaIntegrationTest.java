package sandbox.kafka.test.integration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import sandbox.kafka.consumer.Consumer;
import sandbox.kafka.consumer.ConsumerConfig;
import sandbox.kafka.producer.Producer;
import sandbox.kafka.producer.ProducerConfig;

/**
 * Base class for integration tests that sets up a containerized test environment in Docker.
 *
 * <p><a href="https://www.testcontainers.org/">Testcontainers</a> is used to manage Docker
 * containers.
 */
public abstract class KafkaIntegrationTest {

  private static final String CONFLUENT_VERSION = "5.5.0";

  // send container output to slf4j logger
  private static final Logger logger = LoggerFactory.getLogger(KafkaIntegrationTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);

  // Docker network for inter-container communication
  private static final Network network;

  // Kafka container
  private static final KafkaContainer kafka;

  // Schema Registry container
  private static final GenericContainer<?> schemaRegistry;
  private static final String schemaRegistryUrl;

  // setup Docker containers as singletons for reuse across integration tests
  static {
    logger.info("Setting up integration test environment");

    // create Docker network
    network = Network.newNetwork();

    // create Kafka container
    logger.info("Creating Kafka container");
    kafka =
        new KafkaContainer(CONFLUENT_VERSION)
            .withNetwork(network)
            .withNetworkAliases("kafka")
            // configure two listeners:
            //  BROKER for internal connections (within Docker network)
            //  PLAINTEXT for external connections (host)
            .withEnv("KAFKA_LISTENERS", "BROKER://kafka:9092,PLAINTEXT://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://kafka:9092,PLAINTEXT://0.0.0.0:9093")
            // disable "proactive support" metrics collected by Confluent, it just adds noise
            .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
            .withLogConsumer(logConsumer);

    kafka.start();

    // create Schema Registry container
    logger.info("Creating Schema Registry container");
    schemaRegistry =
        new GenericContainer<>(
                String.format("confluentinc/cp-schema-registry:%s", CONFLUENT_VERSION))
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .withExposedPorts(8081)
            .waitingFor(Wait.forHttp("/"))
            .withLogConsumer(logConsumer);

    schemaRegistry.start();

    schemaRegistryUrl =
        String.format(
            "http://%s:%s",
            schemaRegistry.getContainerIpAddress(), schemaRegistry.getMappedPort(8081));
  }

  /**
   * Create Kafka producer using Avro serialization.
   *
   * @param topic Kafka topic
   * @param schemaReflection if true, use Avro schema reflection
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createAvroProducer(String topic, boolean schemaReflection) {
    ProducerConfig config = defaultProducerConfig(topic);
    config.setValueSerializer(KafkaAvroSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("schema.reflection", schemaReflection);
    return new Producer<>(config);
  }

  /**
   * Create Kafka producer using JSON serialization.
   *
   * @param topic Kafka topic
   * @param type Kafka message value type
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createJsonProducer(String topic, Class<T> type) {
    ProducerConfig config = defaultProducerConfig(topic);
    config.setValueSerializer(KafkaJsonSchemaSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("json.value.type", type);
    return new Producer<>(config);
  }

  /**
   * Create Kafka producer using Protocol Buffers serialization.
   *
   * @param topic Kafka topic
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createProtoProducer(String topic) {
    ProducerConfig config = defaultProducerConfig(topic);
    config.setValueSerializer(KafkaProtobufSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    return new Producer<>(config);
  }

  private static ProducerConfig defaultProducerConfig(String topic) {
    ProducerConfig config = new ProducerConfig();
    config.setBrokers(kafka.getBootstrapServers());
    config.setTopic(topic);
    config.setKeySerializer(ByteArraySerializer.class);
    config.setValueSerializer(ByteArraySerializer.class);
    return config;
  }

  /**
   * Create Kafka consumer using Avro deserialization.
   *
   * @param topic Kafka topic
   * @param schemaReflection if true, use Avro schema reflection
   * @param <T> Kafka message value type
   * @return Kafka consumer
   */
  static <T> Consumer<byte[], T> createAvroConsumer(String topic, boolean schemaReflection) {
    ConsumerConfig config = defaultConsumerConfig(topic);
    config.setValueDeserializer(KafkaAvroDeserializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("schema.reflection", schemaReflection);
    config.addProperty("specific.avro.reader", !schemaReflection);
    return new Consumer<>(config);
  }

  /**
   * Create Kafka consumer using JSON deserialization.
   *
   * @param topic Kafka topic
   * @param type Kafka message value type
   * @param <T> Kafka message value type
   * @return Kafka consumer
   */
  static <T> Consumer<byte[], T> createJsonConsumer(String topic, Class<T> type) {
    ConsumerConfig config = defaultConsumerConfig(topic);
    config.setValueDeserializer(KafkaJsonSchemaDeserializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("json.value.type", type);
    return new Consumer<>(config);
  }

  /**
   * Create Kafka consumer using Protocol Buffers deserialization.
   *
   * @param topic Kafka topic
   * @param type Kafka message value type
   * @param <T> Kafka message value type
   * @return Kafka consumer
   */
  static <T> Consumer<byte[], T> createProtoConsumer(String topic, Class<T> type) {
    ConsumerConfig config = defaultConsumerConfig(topic);
    config.setValueDeserializer(KafkaProtobufDeserializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("specific.protobuf.value.type", type);
    return new Consumer<>(config);
  }

  private static ConsumerConfig defaultConsumerConfig(String topic) {
    ConsumerConfig config = new ConsumerConfig();
    config.setBrokers(kafka.getBootstrapServers());
    config.setGroup("test-group");
    config.setTopic(topic);
    config.setKeyDeserializer(ByteArrayDeserializer.class);
    config.setValueDeserializer(ByteArrayDeserializer.class);
    config.addProperty("auto.offset.reset", "earliest");
    return config;
  }
}
