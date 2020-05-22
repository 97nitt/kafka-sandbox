package sandbox.kafka.producer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka producer.
 *
 * <p>This class wraps a {@link KafkaProducer}, but unlike that client, is limited to sending
 * messages to a single Kafka topic.
 *
 * @param <K> message key type
 * @param <V> message value type
 */
public class Producer<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  private static final Callback defaultCallback =
      (meta, e) -> {
        if (e == null) {
          logger.debug(
              "Successfully sent message: topic={}, partition={}, offset={}",
              meta.topic(),
              meta.partition(),
              meta.offset());
        } else {
          logger.error(
              "Failed to send message: topic={}, partition={}", meta.topic(), meta.partition(), e);
        }
      };

  private final KafkaProducer<K, V> producer;
  private final String topic;

  /**
   * Constructor.
   *
   * @param config Kafka producer configuration
   */
  public Producer(ProducerConfig config) {
    this(new KafkaProducer<>(config.getProducerProperties()), config.getTopic());
  }

  /**
   * Package-private constructor, for unit testing purposes.
   *
   * @param producer Kafka producer client
   * @param topic Kafka topic
   */
  Producer(KafkaProducer<K, V> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  /**
   * Send message.
   *
   * @param message Kafka message
   */
  public void send(Message<K, V> message) {
    send(message, null);
  }

  /**
   * Send message.
   *
   * <p>An optional asynchronous callback can be provided that will receive a {@link RecordMetadata}
   * when the message message is acknowledged by the broker. If an error occurs, the callback will
   * receive an {@link Exception} and a {@link RecordMetadata} populated only with the attempted
   * topic and partition.
   *
   * @param message Kafka message
   * @param callback optional asynchronous callback
   */
  public void send(Message<K, V> message, Callback callback) {
    ProducerRecord<K, V> record =
        new ProducerRecord<>(
            topic,
            null,
            message.getKey(),
            message.getValue(),
            convertHeaders(message.getHeaders()));

    producer.send(record, callback == null ? defaultCallback : callback);
  }

  private Iterable<Header> convertHeaders(Map<String, String> headers) {
    return headers
        .entrySet()
        .stream()
        .map(h -> createHeader(h.getKey(), h.getValue()))
        .collect(Collectors.toList());
  }

  private Header createHeader(String key, String value) {
    byte[] bytes = null;
    if (value != null) {
      bytes = value.getBytes(StandardCharsets.UTF_8);
    }
    return new RecordHeader(key, bytes);
  }

  /** Close this Kafka producer. */
  public void close() {
    logger.info("Closing producer");
    producer.close();
  }
}
