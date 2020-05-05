package sandbox.kafka.serde.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka serializer that can be used to serialize a POJO into an Avro-encoded byte array.
 *
 * An Avro schema will be generated from the POJO class and automatically registered with the schema registry. Updates
 * to the POJO class (adding/removing fields) will need to adhere to the schema compatibility rules enforced by the
 * registry. Use annotations in {@link org.apache.avro.reflect} to influence schema generation.
 *
 */
public class AvroSerializer implements Serializer<Object> {

	private final KafkaAvroSerializer serializer;

	/**
	 * Default constructor.
	 *
	 */
	public AvroSerializer() {
		this(new KafkaAvroSerializer());
	}

	/**
	 * Package-private constructor, for unit testing purposes.
	 *
	 * @param serializer Confluent Avro serializer
	 */
	AvroSerializer(KafkaAvroSerializer serializer) {
		this.serializer = serializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// enable schema reflection in Confluent serializer
		Map<String, Object> configMap = new HashMap<>(configs);
		configMap.put("schema.reflection", true);
		serializer.configure(configMap, isKey);
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		return serializer.serialize(topic, data);
	}

	@Override
	public void close() {
		serializer.close();
	}
}
