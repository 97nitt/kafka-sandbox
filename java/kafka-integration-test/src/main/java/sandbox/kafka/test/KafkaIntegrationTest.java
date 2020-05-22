package sandbox.kafka.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Base class for integration tests that sets up required Kafka infrastructure using Testcontainers.
 *
 */
public class KafkaIntegrationTest {

  private static final String CONFLUENT_VERSION = "5.5.0";

  // send container output to slf4j logger
  private static final Logger logger = LoggerFactory.getLogger(KafkaIntegrationTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);

  // Docker network for inter-container communication
  private static final Network network;

  // Kafka container
  protected static final KafkaContainer kafka;

  // Schema Registry container
  protected static final GenericContainer<?> schemaRegistry;
  protected static final String schemaRegistryUrl;

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
}
