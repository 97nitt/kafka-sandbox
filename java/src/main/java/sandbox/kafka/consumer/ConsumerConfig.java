package sandbox.kafka.consumer;

import lombok.Data;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * POJO for Kafka consumer configuration.
 *
 * For a complete list of Kafka consumer properties, see:
 * https://kafka.apache.org/documentation/#consumerconfigs
 *
 */
@Data
public class ConsumerConfig {

	/* Kafka broker URLs (comma-separated) */
	private String brokers;

	/* Kafka consumer group */
	private String group;

	/* Kafka topic */
	private String topic;

	/* Consumer poll timeout (in milliseconds) */
	private long pollTimeout = 1000;

	/* Message key deserializer */
	private Class<? extends Deserializer<?>> keyDeserializer;

	/* Message value deserializer */
	private Class<? extends Deserializer<?>> valueDeserializer;

	/* Additional configuration properties */
	private Properties properties;

	/**
	 * Add configuration property.
	 *
	 * @param name property name
	 * @param value property value
	 */
	public void addProperty(String name, Object value) {
		if (properties == null) {
			properties = new Properties();
		}
		properties.put(name, value);
	}

	/**
	 * Get Kafka consumer configuration properties.
	 *
	 * @return map of Kafka consumer configuration properties
	 */
	public Map<String, Object> getConsumerProperties() {
		// add required properties
		Map<String, Object> properties = new HashMap<>();
		properties.put("bootstrap.servers", brokers);
		properties.put("group.id", group);
		properties.put("key.deserializer", keyDeserializer);
		properties.put("value.deserializer", valueDeserializer);

		// add additional properties (possibly overwriting anything above)
		if (this.properties != null) {
			this.properties.forEach((k,v) -> properties.put(k.toString(), v));
		}

		return properties;
	}
}
