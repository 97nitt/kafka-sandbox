package sandbox.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class ProducerTests {

  @Test
  public void send() {
    // given
    String topic = "topic";
    String key = "key";
    String value = "value";
    String contextId = UUID.randomUUID().toString();

    Message<String, String> message = new Message<>(key, value);
    message.addHeader("context-id", contextId);

    KafkaProducer<String, String> client = mock(KafkaProducer.class);
    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

    // when
    Producer<String, String> producer = new Producer<>(client, topic);
    producer.send(message);
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
    assertEquals(1, record.headers().toArray().length);
    assertEquals(contextId, new String(record.headers().lastHeader("context-id").value()));
  }

  @Test
  public void sendWithCallback() {
    // given
    String topic = "topic";
    String key = "key";
    String value = "value";
    String contextId = UUID.randomUUID().toString();

    Message<String, String> message = new Message<>(key, value);
    message.addHeader("context-id", contextId);

    Callback callback = (meta, ex) -> {};

    KafkaProducer<String, String> client = mock(KafkaProducer.class);
    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

    // when
    Producer<String, String> producer = new Producer<>(client, topic);
    producer.send(message, callback);
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
    assertEquals(1, record.headers().toArray().length);
    assertEquals(contextId, new String(record.headers().lastHeader("context-id").value()));

    assertSame(callback, callbackCaptor.getValue());
  }
}
