package sandbox.kafka.serde.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka deserializer that can be used to deserialize an Avro-encoded byte array into a Java class.
 *
 * <p>The following configuration properties are available to deserialize a Kafka message key or
 * value to a POJO:
 *
 * <p>key.deserializer.type=sandbox.kafka.models.MyKey
 * value.deserializer.type=sandbox.kafka.models.MyValue
 *
 * <p>When those properties are set, the {@link org.apache.avro.reflect.ReflectDatumReader} will be
 * enabled, and a "reader" schema will be generated from the configured Java class. That class does
 * not necessarily need to have all fields that exist in the "writer" schema used to write the
 * message. Using this approach, Kafka consumer applications can ignore message fields they do not
 * care about. And, if the schema registry is configured to enforce backwards compatibility (the
 * default configuration), consumers do not have to worry about keeping up with updates made to the
 * "writer" schema, the "reader" schema should always be compatible.
 *
 * <p>Use annotations in {@link org.apache.avro.reflect} to influence reader schema generation, if
 * needed.
 *
 * <p>If no type is defined, then this deserializer will return {@link
 * org.apache.avro.generic.GenericRecord}s.
 */
public class AvroDeserializer implements Deserializer<Object> {

  private final KafkaAvroDeserializer deserializer;
  private Schema schema;

  /** Default constructor. */
  public AvroDeserializer() {
    this(new KafkaAvroDeserializer());
  }

  /**
   * Package-private constructor, for unit testing purposes.
   *
   * @param deserializer Confluent Avro deserializer
   */
  AvroDeserializer(KafkaAvroDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public void configure(Map<String, ?> incoming, boolean isKey) {
    // make a copy of incoming configs
    Map<String, Object> configs = new HashMap<>(incoming);

    // look for configured reader type
    String typeKey = isKey ? "key.deserializer.type" : "value.deserializer.type";
    Object type = configs.get(typeKey);
    if (type != null) {
      // enable use of Avro ReflectDatumReader
      configs.put("schema.reflection", true);

      // generate reader schema via reflection
      schema = ReflectData.get().getSchema((Class<?>) type);
    }

    // configure underlying KafkaAvroDeserializer
    deserializer.configure(configs, isKey);
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    return deserializer.deserialize(topic, data, schema);
  }

  @Override
  public void close() {
    deserializer.close();
  }
}
