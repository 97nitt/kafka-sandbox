package sandbox.kafka.test.integration;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import sandbox.kafka.serde.avro.AvroDeserializer;
import sandbox.kafka.serde.avro.AvroSerializer;
import sandbox.kafka.test.models.Thingy;

/**
 * Base class for integration tests that sets up a containerized test environment in Docker.
 *
 * <a href="https://www.testcontainers.org/">Testcontainers</a> is used to manage Docker containers.
 */
public abstract class KafkaIntegrationTest {

    protected static final Logger logger = LoggerFactory.getLogger(KafkaIntegrationTest.class);

    private static final String CONFLUENT_VERSION = "5.5.0";

    // send container output to slf4j logger
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
        kafka = new KafkaContainer(CONFLUENT_VERSION)
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
        schemaRegistry = new GenericContainer<>(String.format("confluentinc/cp-schema-registry:%s", CONFLUENT_VERSION))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withExposedPorts(8081)
                .waitingFor(Wait.forHttp("/"))
                .withLogConsumer(logConsumer);

        schemaRegistry.start();

        schemaRegistryUrl = String.format("http://%s:%s",
                schemaRegistry.getContainerIpAddress(),
                schemaRegistry.getMappedPort(8081));
    }

    static Producer<byte[], Thingy> createAvroProducer(String topic) {
    	ProducerConfig config = defaultProducerConfig(topic);
    	config.setValueSerializer(AvroSerializer.class);
    	config.addProperty("schema.registry.url", schemaRegistryUrl);
    	return new Producer<>(config);
	}

    static Producer<String, String> createStringProducer(String topic) {
		ProducerConfig config = defaultProducerConfig(topic);
		config.setKeySerializer(StringSerializer.class);
		config.setValueSerializer(StringSerializer.class);
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

	static Consumer<byte[], Thingy> createAvroConsumer(String topic) {
    	ConsumerConfig config = defaultConsumerConfig(topic);
    	config.setValueDeserializer(AvroDeserializer.class);
    	config.addProperty("schema.registry.url", schemaRegistryUrl);
    	config.addProperty("value.deserializer.type", Thingy.class);
    	return new Consumer<>(config);
	}

	static Consumer<String, String> createStringConsumer(String topic) {
		ConsumerConfig config = defaultConsumerConfig(topic);
		config.setKeyDeserializer(StringDeserializer.class);
		config.setValueDeserializer(StringDeserializer.class);
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
