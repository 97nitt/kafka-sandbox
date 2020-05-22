package sandbox.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

public class ConsumerConfigTests {

  @Test
  public void getProperties() {
    // given
    ConsumerConfig config = new ConsumerConfig();
    config.setBrokers("localhost:9092");
    config.setGroup("group");
    config.setTopic("topic");
    config.setPollTimeout(500);
    config.setKeyDeserializer(ByteArrayDeserializer.class);
    config.setValueDeserializer(StringDeserializer.class);
    config.addProperty("client.id", "test");

    // when
    Map<String, Object> properties = config.getConsumerProperties();

    // then
    assertNotNull(properties);
    assertEquals(5, properties.size());
    assertEquals("localhost:9092", properties.get("bootstrap.servers"));
    assertEquals("group", properties.get("group.id"));
    assertEquals(ByteArrayDeserializer.class, properties.get("key.deserializer"));
    assertEquals(StringDeserializer.class, properties.get("value.deserializer"));
    assertEquals("test", properties.get("client.id"));
  }
}
