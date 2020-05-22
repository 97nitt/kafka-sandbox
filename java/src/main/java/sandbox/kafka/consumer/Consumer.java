package sandbox.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * This class wraps a {@link KafkaConsumer}, but unlike that client, is limited to receiving
 * messages from a single Kafka topic.
 *
 * @param <K> message key type
 * @param <V> message value type
 */
public class Consumer<K, V> {

  private final KafkaConsumer<K, V> consumer;
  private final Duration timeout;

  /**
   * Constructor.
   *
   * @param config consumer configuration
   */
  public Consumer(ConsumerConfig config) {
    this(
        new KafkaConsumer<>(config.getConsumerProperties()),
        config.getTopic(),
        config.getPollTimeout());
  }

  /**
   * Package-private constructor, for unit testing purposes.
   *
   * @param consumer Kafka consumer client
   * @param topic Kafka topic
   * @param timeout consumer poll timeout (in milliseconds)
   */
  Consumer(KafkaConsumer<K, V> consumer, String topic, long timeout) {
    this.consumer = consumer;
    this.consumer.subscribe(Collections.singleton(topic));
    this.timeout = Duration.ofMillis(timeout);
  }

  /**
   * Poll for messages.
   *
   * <p>This method will return when the maximum number of messages configured in the consumer are
   * available (max.poll.records), or the poll timeout has been reached.
   *
   * @return received messages
   */
  public ConsumerRecords<K, V> poll() {
    return consumer.poll(timeout);
  }

  /** Close this Kafka consumer. */
  public void close() {
    consumer.close();
  }
}
