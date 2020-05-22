package sandbox.kafka.admin;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for managing Kafka topics.
 *
 * <p>This service can optionally create topics on startup or delete topics on shutdown.
 */
public class TopicManager {

  private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);

  private final AdminClient adminClient;
  private final Collection<TopicConfig> topics;

  /**
   * Constructor.
   *
   * @param adminClient Kafka admin client
   * @param topics topics to create/delete during application startup/shutdown (optional)
   */
  public TopicManager(AdminClient adminClient, TopicConfig... topics) {
    this(adminClient, Arrays.asList(topics));
  }

  /**
   * Constructor.
   *
   * @param adminClient Kafka admin client
   * @param topics topics to create/delete during application startup/shutdown (optional)
   */
  public TopicManager(AdminClient adminClient, Collection<TopicConfig> topics) {
    this.adminClient = adminClient;
    this.topics = topics;
  }

  /**
   * Create topics that have been registered with this service and configured to be created after
   * this service is created.
   *
   * <p>This method is intended to be executed *once* during application startup.
   */
  @PostConstruct
  public void createTopics() {
    Collection<TopicConfig> toCreate =
        topics.stream().filter(TopicConfig::isCreateOnStartup).collect(Collectors.toList());

    create(toCreate);
  }

  /**
   * Delete topics that have been registered with this service and configured to be deleted before
   * this service is destroyed.
   *
   * <p>This method is intended to be executed *once* during application shutdown.
   */
  @PreDestroy
  public void deleteTopics() {
    Collection<String> toDelete =
        topics
            .stream()
            .filter(TopicConfig::isDeleteOnShutdown)
            .map(TopicConfig::getName)
            .collect(Collectors.toList());

    delete(toDelete);
  }

  /**
   * Create Kafka topics.
   *
   * @param configs configuration(s) of topics to create
   */
  public void create(TopicConfig... configs) {
    create(Arrays.asList(configs));
  }

  /**
   * Create Kafka topics.
   *
   * @param configs configurations of topics to create
   */
  public void create(Collection<TopicConfig> configs) {
    Collection<NewTopic> topics =
        configs
            .stream()
            .map(
                c ->
                    new NewTopic(c.getName(), c.getPartitions(), c.getReplicationFactor())
                        .configs(c.getConfigs()))
            .collect(Collectors.toList());

    CreateTopicsResult result = adminClient.createTopics(topics);
    result
        .values()
        .forEach(
            (topic, future) -> {
              try {
                future.get();
                logger.info("Successfully created topic: {}", topic);
              } catch (ExecutionException e) {
                logger.error("Failed to create topic: {}", topic, e.getCause());
              } catch (InterruptedException e) {
                logger.warn(
                    "Thread interrupted while waiting for confirmation that topic was created: {}",
                    topic,
                    e);
              }
            });
  }

  /**
   * Delete Kafka topics.
   *
   * @param topics topic(s) to delete
   */
  public void delete(String... topics) {
    delete(Arrays.asList(topics));
  }

  /**
   * Delete Kafka topics.
   *
   * @param topics topics to delete
   */
  public void delete(Collection<String> topics) {
    DeleteTopicsResult result = adminClient.deleteTopics(topics);
    result
        .values()
        .forEach(
            (topic, future) -> {
              try {
                future.get();
                logger.info("Successfully deleted topic: {}", topic);
              } catch (ExecutionException e) {
                logger.error("Failed to delete topic: {}", topic, e.getCause());
              } catch (InterruptedException e) {
                logger.warn(
                    "Thread interrupted while waiting for confirmation that topic was deleted: {}",
                    topic,
                    e);
              }
            });
  }
}
