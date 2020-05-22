package sandbox.kafka.requestreply.config;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import sandbox.kafka.admin.TopicConfig;
import sandbox.kafka.admin.TopicManager;
import sandbox.kafka.requestreply.users.User;
import sandbox.kafka.requestreply.users.UserResponse;

/** Kafka configuration class. */
@Configuration
public class KafkaConfig {

  /**
   * Bind Kafka configuration properties to an instance of {@link KafkaProperties}.
   *
   * @return Kafka configuration properties
   */
  @Bean
  @ConfigurationProperties("kafka")
  public KafkaProperties kafkaProperties() {
    return new KafkaProperties();
  }

  /**
   * Kafka admin client (used to programmatically manage Kafka topics).
   *
   * @param properties Kafka configuration properties
   * @return Kafka admin client
   */
  @Bean(destroyMethod = "close")
  public AdminClient adminClient(KafkaProperties properties) {
    return AdminClient.create(properties.getAdminProperties());
  }

  /**
   * Topic manager used to create and delete topics during application startup & shutdown.
   *
   * @param properties Kafka configuration properties
   * @param adminClient Kafka admin client
   * @return topic manager
   */
  @Bean
  public TopicManager topicManager(KafkaProperties properties, AdminClient adminClient) {
    return new TopicManager(
        adminClient, properties.getTopics().getData(), properties.getTopics().getResponse());
  }

  /**
   * Kafka producer for "users" topic.
   *
   * @param properties Kafka configuration properties
   * @return Kafka producer
   */
  @Bean(destroyMethod = "close")
  public Producer<String, User> userProducer(KafkaProperties properties) {
    Map<String, Object> configs = properties.getProducerProperties("users-producer");
    return new KafkaProducer<>(configs);
  }

  /**
   * Kafka consumer for "users" topic.
   *
   * @param properties Kafka configuration properties
   * @return Kafka consumer
   */
  @Bean(destroyMethod = "close")
  public Consumer<String, User> userConsumer(KafkaProperties properties) {
    Map<String, Object> configs = properties.getConsumerProperties("users-consumer");
    Consumer<String, User> consumer = new KafkaConsumer<>(configs);

    Collection<TopicPartition> assignments =
        getPartitionAssignments(properties.getTopics().getData());
    consumer.assign(assignments);
    return consumer;
  }

  /**
   * Kafka producer for "users-response" topic.
   *
   * @param properties Kafka configuration properties
   * @return Kafka producer
   */
  @Bean(destroyMethod = "close")
  public Producer<String, UserResponse> userResponseProducer(KafkaProperties properties) {
    Map<String, Object> configs = properties.getProducerProperties("users-response-producer");
    return new KafkaProducer<>(configs);
  }

  /**
   * Kafka consumer for "users-response" topic.
   *
   * @param properties Kafka configuration properties
   * @return Kafka consumer
   */
  @Bean(destroyMethod = "close")
  public Consumer<String, UserResponse> userResponseConsumer(KafkaProperties properties) {
    Map<String, Object> configs = properties.getConsumerProperties("users-response-consumer");
    Consumer<String, UserResponse> consumer = new KafkaConsumer<>(configs);

    Collection<TopicPartition> assignments =
        getPartitionAssignments(properties.getTopics().getResponse());
    consumer.assign(assignments);
    return consumer;
  }

  private Collection<TopicPartition> getPartitionAssignments(TopicConfig topic) {
    return IntStream.range(0, topic.getPartitions())
        .mapToObj(p -> new TopicPartition(topic.getName(), p))
        .collect(Collectors.toList());
  }
}
