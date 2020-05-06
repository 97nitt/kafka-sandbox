package sandbox.kafka.serde.json;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;
import sandbox.kafka.test.models.Thingy;
import sandbox.kafka.test.models.ThingyWithoutFoo;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JsonSerializationTests {

	@Test
	public void serdeWithSameWriterAndReaderSchema() {
		// given
		SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
		Serializer<Thingy> serializer = serializer(schemaRegistryClient, Thingy.class);
		Deserializer<Thingy> deserializer = deserializer(schemaRegistryClient, Thingy.class);

		Thingy thingy = new Thingy();
		thingy.setFoo("test");
		thingy.setBar("ing");

		// when
		byte[] serialized = serializer.serialize("topic", thingy);
		Thingy deserialized = deserializer.deserialize("topic", serialized);

		// then
		assertNotNull(deserialized);
		assertEquals(thingy.getFoo(), deserialized.getFoo());
		assertEquals(thingy.getBar(), deserialized.getBar());
	}

	@Test
	public void serdeWithDifferentWriterAndReaderSchema() {
		// given
		SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
		Serializer<Thingy> serializer = serializer(schemaRegistryClient, Thingy.class);
		Deserializer<ThingyWithoutFoo> deserializer = deserializer(schemaRegistryClient, ThingyWithoutFoo.class);

		Thingy thingy = new Thingy();
		thingy.setFoo("test");
		thingy.setBar("ing");

		// when
		byte[] serialized = serializer.serialize("topic", thingy);
		ThingyWithoutFoo deserialized = deserializer.deserialize("topic", serialized);

		// then
		assertNotNull(deserialized);
		assertEquals(thingy.getBar(), deserialized.getBar());
	}

	static <T> Serializer<T> serializer(SchemaRegistryClient schemaRegistryClient, Class<T> type) {
		Map<String, Object> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://localhost:8081");
		configs.put("json.value.type", type);

		Serializer<T> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient);
		serializer.configure(configs, false);

		return serializer;
	}

	static <T> Deserializer<T> deserializer(SchemaRegistryClient schemaRegistryClient, Class<T> type) {
		Map<String, Object> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://localhost:8081");
		configs.put("json.value.type", type);
		configs.put("json.fail.unknown.properties", false);

		Deserializer<T> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient);
		deserializer.configure(configs, false);

		return deserializer;
	}
}
