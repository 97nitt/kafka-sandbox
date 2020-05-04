package sandbox.kafka.test.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import sandbox.kafka.consumer.Consumer;
import sandbox.kafka.producer.Message;
import sandbox.kafka.producer.Producer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This integration test verifies that we can send and receive a message through Kafka
 * using String serialization.
 *
 */
public class StringSerdeTest extends KafkaIntegrationTest {

    private static final String TOPIC = "test";

    @Test
    public void testProducerAndConsumer() {
        // create Kafka producer
        Producer<String, String> producer = createProducer(
        		TOPIC,
				StringSerializer.class,
				StringSerializer.class);

        // create test message
		String key = "key";
		String value = "test";
		String contextId = UUID.randomUUID().toString();

		Message<String, String> message = new Message<>(key, value);
		message.addHeader("context-id", contextId);

        // send message
		AtomicReference<RecordMetadata> metadata = new AtomicReference<>();
		AtomicReference<Exception> exception = new AtomicReference<>();
        producer.send(message, (meta, ex) -> {
        	metadata.set(meta);
        	exception.set(ex);
		});

        // close Kafka producer
        producer.close();

		// verify successful receipt
		assertNull(exception.get());
		assertNotNull(metadata.get());
		assertEquals(TOPIC, metadata.get().topic());
		assertEquals(0, metadata.get().partition());
		assertEquals(0, metadata.get().offset());
		assertTrue(metadata.get().hasTimestamp());

        // create Kafka consumer
        Consumer<String, String> consumer = createConsumer(
        		TOPIC,
				StringDeserializer.class,
				StringDeserializer.class);

        // poll once for messages
        ConsumerRecords<String, String> poll = consumer.poll();

        // close Kafka consumer
        consumer.close();

        // verify results
        assertNotNull(poll);
        assertEquals(1, poll.count());

		ConsumerRecord<String, String> record = poll.iterator().next();
        assertNotNull(record);
        assertEquals(TOPIC, record.topic());
        assertEquals(0, record.partition());
        assertEquals(0, record.offset());
        assertTrue(record.timestamp() > 0);
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(1, record.headers().toArray().length);
        assertEquals(contextId, new String(record.headers().lastHeader("context-id").value()));
    }
}
