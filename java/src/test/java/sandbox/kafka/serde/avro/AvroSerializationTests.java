package sandbox.kafka.serde.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;
import sandbox.kafka.test.models.Thingy;
import sandbox.kafka.test.models.ThingyWithoutFoo;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroSerializationTests {

	@Test
	public void serdeWithSameWriterAndReaderSchema() {
		// given
		SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
		Map<String, Object> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://localhost:8081");
		configs.put("value.deserializer.type", Thingy.class);

		Serializer<Object> serializer = new AvroSerializer(new KafkaAvroSerializer(schemaRegistryClient));
		serializer.configure(configs, false);

		Deserializer<Object> deserializer = new AvroDeserializer(new KafkaAvroDeserializer(schemaRegistryClient));
		deserializer.configure(configs, false);

		Thingy thingy = new Thingy();
		thingy.setFoo("test");
		thingy.setBar("ing");

		// when
		byte[] serialized = serializer.serialize("topic", thingy);
		Thingy deserialized = (Thingy) deserializer.deserialize("topic", serialized);

		// then
		assertNotNull(deserialized);
		assertEquals(thingy.getFoo(), deserialized.getFoo());
		assertEquals(thingy.getBar(), deserialized.getBar());
	}

	@Test
	public void serdeWithDifferentWriterAndReaderSchema() {
		// given
		SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
		Map<String, Object> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://localhost:8081");
		configs.put("value.deserializer.type", ThingyWithoutFoo.class);

		Serializer<Object> serializer = new AvroSerializer(new KafkaAvroSerializer(schemaRegistryClient));
		serializer.configure(configs, false);

		Deserializer<Object> deserializer = new AvroDeserializer(new KafkaAvroDeserializer(schemaRegistryClient));
		deserializer.configure(configs, false);

		Thingy thingy = new Thingy();
		thingy.setFoo("test");
		thingy.setBar("ing");

		// when
		byte[] serialized = serializer.serialize("topic", thingy);
		ThingyWithoutFoo deserialized = (ThingyWithoutFoo) deserializer.deserialize("topic", serialized);

		// then
		assertNotNull(deserialized);
		assertEquals(thingy.getBar(), deserialized.getBar());
	}

	@Test
	public void serdeWithGenericRecord() {
		// given
		SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
		Map<String, Object> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://localhost:8081");

		Serializer<Object> serializer = new AvroSerializer(new KafkaAvroSerializer(schemaRegistryClient));
		serializer.configure(configs, false);

		Deserializer<Object> deserializer = new AvroDeserializer(new KafkaAvroDeserializer(schemaRegistryClient));
		deserializer.configure(configs, false);

		Thingy thingy = new Thingy();
		thingy.setFoo("test");
		thingy.setBar("ing");

		// when
		byte[] serialized = serializer.serialize("topic", thingy);
		GenericRecord deserialized = (GenericRecord) deserializer.deserialize("topic", serialized);

		// then
		assertNotNull(deserialized);
		assertEquals(thingy.getFoo(), deserialized.get("foo").toString());
		assertEquals(thingy.getBar(), deserialized.get("bar").toString());
	}
}
