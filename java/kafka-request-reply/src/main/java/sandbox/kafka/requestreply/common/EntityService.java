package sandbox.kafka.requestreply.common;

import java.util.Collection;
import java.util.UUID;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.async.DeferredResult;
import sandbox.kafka.requestreply.config.KafkaProperties;

/**
 * Service responsible for managing CRUD operations for some entity.
 *
 * @param <K> entity key type
 * @param <V> entity value type
 */
public class EntityService<K, V, R> {

  private static final Logger logger = LoggerFactory.getLogger(EntityService.class);

  private final Producer<K, V> producer;
  private final String topic;
  private final String responseTopic;
  private final Store<K, V> store;
  private final DeferredResultService deferredResults;

  /**
   * Constructor.
   *
   * @param properties Kafka configuration properties
   * @param producer Kafka producer used to publish entity updates to a log compacted topic
   * @param store local entity store
   * @param deferredResults local cache of deferred API responses
   */
  public EntityService(
      KafkaProperties properties,
      Producer<K, V> producer,
      Store<K, V> store,
      DeferredResultService deferredResults) {

    this.producer = producer;
    this.topic = properties.getTopics().getData().getName();
    this.responseTopic = properties.getTopics().getResponse().getName();
    this.store = store;
    this.deferredResults = deferredResults;
  }

  /**
   * Get all entities.
   *
   * @return all entities
   */
  public Collection<V> getAll() {
    return store.getAll();
  }

  /**
   * Get entity by id.
   *
   * @param id entity id
   * @return entity (null if no entity exists with given id)
   */
  public V getById(K id) {
    return store.get(id);
  }

  /**
   * Save entity.
   *
   * @param id entity id
   * @param entity entity state to save
   * @return deferred response
   */
  public DeferredResult<R> save(K id, V entity) {
    return publish(id, entity);
  }

  /**
   * Delete an entity.
   *
   * @param id id of entity to delete
   * @return deferred response
   */
  public DeferredResult<R> delete(K id) {
    return publish(id, null);
  }

  private DeferredResult<R> publish(K id, V entity) {
    // generate a transaction id
    String txnId = UUID.randomUUID().toString();

    // build Kafka message
    Collection<Header> headers = HeaderUtils.createHeaders(responseTopic, txnId);
    ProducerRecord<K, V> record =
        new ProducerRecord<>(topic, null, null, id, entity, headers);

    // publish message
    producer.send(
        record,
        (meta, ex) -> {
          if (ex == null) {
            logger.debug(
                "Successfully sent Kafka message: topic={}, partition={}, offset={}, timestamp={}, key={}, value={}, transaction-id={}",
                meta.topic(),
                meta.partition(),
                meta.offset(),
                meta.timestamp(),
                id,
                entity,
                txnId);

          } else {
            logger.error(
                "Failed to send Kafka message: topic={}, partition={}, key={}, value={}, transaction-id={}",
                meta.topic(),
                meta.partition(),
                id,
                entity,
                txnId,
                ex);
          }
        });

    // create a deferred response
    return deferredResults.create(txnId);
  }
}
