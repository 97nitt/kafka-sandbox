package sandbox.kafka.producer;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

/**
 * Kafka message.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Getter
public class Message<K, V> {

  /* Kafka topic */
  private final String topic;

  /* Kafka topic partition */
  private final Integer partition;

  /* message key */
  private final K key;

  /* message value */
  private final V value;

  /* message headers */
  private final Map<String, String> headers;

  /**
   * Constructor.
   *
   * @param topic Kafka topic
   * @param value message value
   */
  public Message(String topic, V value) {
    this(topic, null, null, value);
  }

  /**
   * Constructor.
   *
   * @param topic Kafka topic
   * @param key Kafka message key
   * @param value Kafka message value
   */
  public Message(String topic, K key, V value) {
    this(topic, null, key, value);
  }

  /**
   * Constructor.
   *
   * @param topic Kafka topic
   * @param partition Kafka topic partition (optional)
   * @param key Kafka message key (optional)
   * @param value Kafka message value
   */
  public Message(String topic, Integer partition, K key, V value) {
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.value = value;
    this.headers = new HashMap<>();
  }

  /**
   * Add message header.
   *
   * @param name header name
   * @param value header value
   */
  public void addHeader(String name, String value) {
    headers.put(name, value);
  }

  /**
   * Get message header.
   *
   * @param name header name
   * @return header value, null if header does not exist
   */
  public String getHeader(String name) {
    return headers.get(name);
  }
}
