package sandbox.kafka.admin;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/** Container for Kafka topic configuration properties. */
@Data
public class TopicConfig {

  /* topic name */
  private String name;

  /* number of partitions */
  private int partitions = 1;

  /* replication factor */
  private short replicationFactor = 1;

  /* topic configurations */
  private Map<String, String> configs = new HashMap<>();

  /* create topic on startup */
  private boolean createOnStartup = false;

  /* delete topic on shutdown */
  private boolean deleteOnShutdown = false;
}
