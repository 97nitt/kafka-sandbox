package sandbox.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class ConsumerTests {

  @Test
  public void poll() {
    // given
    String topic = "topic";
    long timeout = 1000;
    ArgumentCaptor<Duration> timeoutCaptor = ArgumentCaptor.forClass(Duration.class);
    ArgumentCaptor<Collection<String>> topicCaptor = ArgumentCaptor.forClass(Collection.class);

    KafkaConsumer<byte[], byte[]> client = mock(KafkaConsumer.class);
    ConsumerRecords<byte[], byte[]> expected = mock(ConsumerRecords.class);
    when(client.poll(any(Duration.class))).thenReturn(expected);

    // when
    Consumer<byte[], byte[]> consumer = new Consumer<>(client, topic, timeout);
    ConsumerRecords<byte[], byte[]> actual = consumer.poll();
    consumer.close();

    // then
    assertSame(expected, actual);
    verify(client).subscribe(topicCaptor.capture());
    verify(client).poll(timeoutCaptor.capture());
    verify(client).close();

    assertEquals(timeout, timeoutCaptor.getValue().toMillis());
    assertEquals(1, topicCaptor.getValue().size());
    assertTrue(topicCaptor.getValue().contains(topic));
  }
}
