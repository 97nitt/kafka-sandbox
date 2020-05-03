package sandbox.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka producer.
 *
 * This class wraps a {@link KafkaProducer}, but unlike that client, is limited to sending messages
 * to a single Kafka topic.
 *
 * @param <K> message key type
 * @param <V> message value type
 */
public class Producer<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(Producer.class);
	private static final Callback defaultCallback = (meta, e) -> {
		if (e == null) {
			logger.debug("Successfully sent message: topic={}, partition={}, offset={}",
					meta.topic(),
					meta.partition(),
					meta.offset());
		} else {
			logger.error("Failed to send message: topic={}, partition={}",
					meta.topic(),
					meta.partition(),
					e);
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
	 * @param value message value
	 */
	public void send(V value) {
		send(null, value);
	}

	/**
	 * Send message.
	 *
	 * @param key message key
	 * @param value message value
	 */
	public void send(K key, V value) {
		send(key, value, null);
	}

	/**
	 * Send message.
	 *
	 * An optional asynchronous callback can be provided that will receive a {@link RecordMetadata} when the message
	 * message is acknowledged by the broker. If an error occurs, the callback will receive an {@link Exception} and
	 * a {@link RecordMetadata} populated only with the attempted topic and partition.
	 *
	 * @param value message value
	 * @param callback optional asynchronous callback
	 */
	public void send(V value, Callback callback) {
		send(null, value, callback);
	}

	/**
	 * Send message.
	 *
	 * An optional asynchronous callback can be provided that will receive a {@link RecordMetadata} when the message
	 * message is acknowledged by the broker. If an error occurs, the callback will receive an {@link Exception} and
	 * a {@link RecordMetadata} populated only with the attempted topic and partition.
	 *
	 * @param key message key
	 * @param value message value
	 * @param callback optional asynchronous callback
	 */
	public void send(K key, V value, Callback callback) {
		if (callback == null) {
			callback = defaultCallback;
		}
		ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
		producer.send(record, callback);
	}

	/**
	 * Close this Kafka producer.
	 *
	 */
	public void close() {
		logger.info("Closing producer");
		producer.close();
	}
}
