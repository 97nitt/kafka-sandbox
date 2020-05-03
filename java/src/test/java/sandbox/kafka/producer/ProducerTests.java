package sandbox.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class ProducerTests {

	private static final Callback callback = (meta, e) -> {};

	@Test
	public void sendValue() {
		// given
		String topic = "topic";
		String value = "value";

		KafkaProducer<String, String> client = mock(KafkaProducer.class);
		ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

		// when
		Producer<String, String> producer = new Producer<>(client, topic);
		producer.send(value);
		producer.close();

		// then
		verify(client).send(recordCaptor.capture(), callbackCaptor.capture());
		verify(client).close();

		ProducerRecord<String, String> record = recordCaptor.getValue();
		assertEquals(topic, record.topic());
		assertNull(record.partition());
		assertNull(record.timestamp());
		assertNull(record.key());
		assertEquals(value, record.value());

		assertNotSame(callback, callbackCaptor.getValue());
	}


	@Test
	public void sendValueAndCallback() {
		// given
		String topic = "topic";
		String value = "value";

		KafkaProducer<String, String> client = mock(KafkaProducer.class);
		ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

		// when
		Producer<String, String> producer = new Producer<>(client, topic);
		producer.send(value, callback);
		producer.close();

		// then
		verify(client).send(recordCaptor.capture(), callbackCaptor.capture());
		verify(client).close();

		ProducerRecord<String, String> record = recordCaptor.getValue();
		assertEquals(topic, record.topic());
		assertNull(record.partition());
		assertNull(record.timestamp());
		assertNull(record.key());
		assertEquals(value, record.value());

		assertSame(callback, callbackCaptor.getValue());
	}

	@Test
	public void sendKeyValue() {
		// given
		String topic = "topic";
		String key = "key";
		String value = "value";

		KafkaProducer<String, String> client = mock(KafkaProducer.class);
		ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

		// when
		Producer<String, String> producer = new Producer<>(client, topic);
		producer.send(key, value);
		producer.close();

		// then
		verify(client).send(recordCaptor.capture(), callbackCaptor.capture());
		verify(client).close();

		ProducerRecord<String, String> record = recordCaptor.getValue();
		assertEquals(topic, record.topic());
		assertNull(record.partition());
		assertNull(record.timestamp());
		assertEquals(key, record.key());
		assertEquals(value, record.value());

		assertNotSame(callback, callbackCaptor.getValue());
	}

	@Test
	public void sendKeyValueAndCallback() {
		// given
		String topic = "topic";
		String key = "key";
		String value = "value";

		KafkaProducer<String, String> client = mock(KafkaProducer.class);
		ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
		ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

		// when
		Producer<String, String> producer = new Producer<>(client, topic);
		producer.send(key, value, callback);
		producer.close();

		// then
		verify(client).send(recordCaptor.capture(), callbackCaptor.capture());
		verify(client).close();

		ProducerRecord<String, String> record = recordCaptor.getValue();
		assertEquals(topic, record.topic());
		assertNull(record.partition());
		assertNull(record.timestamp());
		assertEquals(key, record.key());
		assertEquals(value, record.value());

		assertSame(callback, callbackCaptor.getValue());
	}
}
