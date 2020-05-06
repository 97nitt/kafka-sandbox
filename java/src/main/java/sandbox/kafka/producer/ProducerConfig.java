package sandbox.kafka.producer;

import lombok.Data;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * POJO for Kafka producer configuration.
 *
 * For a complete list of Kafka producer properties, see:
 * https://kafka.apache.org/documentation/#producerconfigs
 *
 */
@Data
public class ProducerConfig {

	/* Kafka broker URLs (comma-separated) */
	private String brokers;

	/* Kafka topic */
	private String topic;

	/* Message key serializer */
	private Class<? extends Serializer> keySerializer;

	/* Message value serializer */
	private Class<? extends Serializer> valueSerializer;

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
	 * Get Kafka producer configuration properties.
	 *
	 * @return map of Kafka producer configuration properties
	 */
	public Map<String, Object> getProducerProperties() {
		// add required properties
		Map<String, Object> properties = new HashMap<>();
		properties.put("bootstrap.servers", brokers);
		properties.put("key.serializer", keySerializer);
		properties.put("value.serializer", valueSerializer);

		// add additional properties (possibly overwriting anything above)
		if (this.properties != null) {
			this.properties.forEach((k,v) -> properties.put(k.toString(), v));
		}

		return properties;
	}
}
