package sandbox.kafka.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import sandbox.kafka.consumer.Consumer;
import sandbox.kafka.producer.Message;
import sandbox.kafka.producer.Producer;
import sandbox.kafka.test.models.Thingy;

/**
 * These integration tests verify that we can send and receive a message through Kafka using
 * different schema-based serializers.
 */
public class SchemaSerdeTests extends KafkaIntegrationTest {

  @Test
  public void avro() {
    String topic = "test-avro";

    Producer<byte[], Thingy> producer = createAvroProducer(topic);
    Consumer<byte[], Thingy> consumer = createAvroConsumer(topic);

    executeTest(topic, producer, consumer);
  }

  @Test
  public void json() {
    String topic = "test-json";

    Producer<byte[], Thingy> producer = createJsonProducer(topic);
    Consumer<byte[], Thingy> consumer = createJsonConsumer(topic);

    executeTest(topic, producer, consumer);
  }

  private void executeTest(
      String topic, Producer<byte[], Thingy> producer, Consumer<byte[], Thingy> consumer) {

    // create test message
    Thingy thingy = new Thingy("test", "ing");
    String contextId = UUID.randomUUID().toString();

    Message<byte[], Thingy> message = new Message<>(thingy);
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
    ConsumerRecords<byte[], Thingy> poll = consumer.poll();

    // close Kafka consumer
    consumer.close();

    // verify results
    assertNotNull(poll);
    assertEquals(1, poll.count());

    ConsumerRecord<byte[], Thingy> record = poll.iterator().next();
    assertNotNull(record);
    assertEquals(topic, record.topic());
    assertEquals(0, record.partition());
    assertEquals(0, record.offset());
    assertTrue(record.timestamp() > 0);
    assertNull(record.key());
    assertEquals(thingy.getFoo(), record.value().getFoo());
    assertEquals(thingy.getBar(), record.value().getBar());
    assertEquals(1, record.headers().toArray().length);
    assertEquals(contextId, new String(record.headers().lastHeader("context-id").value()));
  }
}
