package sandbox.kafka.producer;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProducerConfigTests {

	@Test
	public void getProperties() {
		// given
		ProducerConfig config = new ProducerConfig();
		config.setBrokers("localhost:9092");
		config.setTopic("topic");
		config.setKeySerializer(ByteArraySerializer.class);
		config.setValueSerializer(StringSerializer.class);
		config.addProperty("client.id", "test");

		// when
		Map<String, Object> properties = config.getProducerProperties();

		// then
		assertNotNull(properties);
		assertEquals(4, properties.size());
		assertEquals("localhost:9092", properties.get("bootstrap.servers"));
		assertEquals(ByteArraySerializer.class, properties.get("key.serializer"));
		assertEquals(StringSerializer.class, properties.get("value.serializer"));
		assertEquals("test", properties.get("client.id"));
	}
}
