package sandbox.kafka.test.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import sandbox.kafka.consumer.Consumer;
import sandbox.kafka.consumer.ConsumerConfig;
import sandbox.kafka.producer.Message;
import sandbox.kafka.producer.Producer;
import sandbox.kafka.producer.ProducerConfig;
import sandbox.kafka.test.KafkaIntegrationTest;
import sandbox.kafka.test.models.AvroThingy;
import sandbox.kafka.test.models.Thingy;
import sandbox.kafka.test.models.ThingyProto;

/**
 * These integration tests verify that we can send and receive a message through Kafka using
 * different schema-based serializers.
 */
public class SchemaSerdeTests extends KafkaIntegrationTest {

  @Test
  public void avroUsingReflection() {
    Thingy input = new Thingy("test", "ing");
    String topic = "test-avro-reflection";

    Producer<byte[], Thingy> producer = createAvroProducer(true);
    Consumer<byte[], Thingy> consumer = createAvroConsumer(topic, true);

    BiConsumer<Thingy, Thingy> assertions =
        (i, o) -> {
          assertEquals(i.getFoo(), o.getFoo());
          assertEquals(i.getBar(), o.getBar());
        };

    executeTest(input, topic, producer, consumer, assertions);
  }

  @Test
  public void avroUsingSpecificData() {
    AvroThingy input = AvroThingy.newBuilder().setFoo("test").setBar("ing").build();

    String topic = "test-avro-specific";

    Producer<byte[], AvroThingy> producer = createAvroProducer(false);
    Consumer<byte[], AvroThingy> consumer = createAvroConsumer(topic, false);

    BiConsumer<AvroThingy, AvroThingy> assertions =
        (i, o) -> {
          assertEquals(i.getFoo(), o.getFoo());
          assertEquals(i.getBar(), o.getBar());
        };

    executeTest(input, topic, producer, consumer, assertions);
  }

  @Test
  public void json() {
    Thingy input = new Thingy("test", "ing");
    String topic = "test-json";

    Producer<byte[], Thingy> producer = createJsonProducer(Thingy.class);
    Consumer<byte[], Thingy> consumer = createJsonConsumer(topic, Thingy.class);

    BiConsumer<Thingy, Thingy> assertions =
        (i, o) -> {
          assertEquals(i.getFoo(), o.getFoo());
          assertEquals(i.getBar(), o.getBar());
        };

    executeTest(input, topic, producer, consumer, assertions);
  }

  @Test
  public void proto() {
    ThingyProto.Thingy input = ThingyProto.Thingy.newBuilder().setFoo("test").setBar("ing").build();
    String topic = "test-proto";

    Producer<byte[], ThingyProto.Thingy> producer = createProtoProducer();
    Consumer<byte[], ThingyProto.Thingy> consumer =
        createProtoConsumer(topic, ThingyProto.Thingy.class);

    BiConsumer<ThingyProto.Thingy, ThingyProto.Thingy> assertions =
        (i, o) -> {
          assertEquals(i.getFoo(), o.getFoo());
          assertEquals(i.getBar(), o.getBar());
        };

    executeTest(input, topic, producer, consumer, assertions);
  }

  private <I, O> void executeTest(
      I input,
      String topic,
      Producer<byte[], I> producer,
      Consumer<byte[], O> consumer,
      BiConsumer<I, O> assertions) {

    // create test message
    String contextId = UUID.randomUUID().toString();
    Message<byte[], I> message = new Message<>(topic, input);
    message.addHeader("context-id", contextId);

    // send message
    AtomicReference<RecordMetadata> metadata = new AtomicReference<>();
    AtomicReference<Exception> exception = new AtomicReference<>();
    producer.send(
        message,
        (meta, ex) -> {
          metadata.set(meta);
          exception.set(ex);
        });

    // close Kafka producer
    producer.close();

    // verify successful receipt
    assertNull(exception.get());
    assertNotNull(metadata.get());
    assertEquals(topic, metadata.get().topic());
    assertEquals(0, metadata.get().partition());
    assertEquals(0, metadata.get().offset());
    assertTrue(metadata.get().hasTimestamp());

    // poll once for messages
    ConsumerRecords<byte[], O> poll = consumer.poll();

    // close Kafka consumer
    consumer.close();

    // verify results
    assertNotNull(poll);
    assertEquals(1, poll.count());

    ConsumerRecord<byte[], O> record = poll.iterator().next();
    assertNotNull(record);
    assertEquals(topic, record.topic());
    assertEquals(0, record.partition());
    assertEquals(0, record.offset());
    assertTrue(record.timestamp() > 0);
    assertNull(record.key());
    assertEquals(1, record.headers().toArray().length);
    assertEquals(contextId, new String(record.headers().lastHeader("context-id").value()));

    O output = record.value();
    assertions.accept(input, output);
  }

  /**
   * Create Kafka producer using Avro serialization.
   *
   * @param schemaReflection if true, use Avro schema reflection
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createAvroProducer(boolean schemaReflection) {
    ProducerConfig config = defaultProducerConfig();
    config.setValueSerializer(KafkaAvroSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("schema.reflection", schemaReflection);
    return new Producer<>(config);
  }

  /**
   * Create Kafka producer using JSON serialization.
   *
   * @param type Kafka message value type
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createJsonProducer(Class<T> type) {
    ProducerConfig config = defaultProducerConfig();
    config.setValueSerializer(KafkaJsonSchemaSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    config.addProperty("json.value.type", type);
    return new Producer<>(config);
  }

  /**
   * Create Kafka producer using Protocol Buffers serialization.
   *
   * @param <T> Kafka message value type
   * @return Kafka producer
   */
  static <T> Producer<byte[], T> createProtoProducer() {
    ProducerConfig config = defaultProducerConfig();
    config.setValueSerializer(KafkaProtobufSerializer.class);
    config.addProperty("schema.registry.url", schemaRegistryUrl);
    return new Producer<>(config);
  }

  private static ProducerConfig defaultProducerConfig() {
    ProducerConfig config = new ProducerConfig();
    config.setBrokers(kafka.getBootstrapServers());
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
