package sandbox.kafka.requestreply.common;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import sandbox.kafka.requestreply.config.KafkaProperties;

/**
 * Kafka consumer that reacts to entity updates (create, update, delete) published to Kafka.
 *
 * Updates are persisted to a {@link Store}, then a response is published to Kafka to notify the
 * source of the transaction that it has been completed.
 *
 */
public class EntityListener<K, V, R> {

  private static final Logger logger = LoggerFactory.getLogger(EntityListener.class);

  private final Consumer<K, V> consumer;
  private final Store<K, V> store;
  private final Producer<String, R> producer;
  private final String responseTopic;
  private final ResponseBuilder<V, R> responseBuilder;

  /**
   * Constructor.
   *
   * @param properties Kafka configuration properties
   * @param consumer Kafka consumer of entity updates
   * @param store local entity store
   * @param producer Kafka producer to send responses to entity updates
   * @param responseBuilder function that creates a response (R) from received entity (V)
   */
  public EntityListener(
      KafkaProperties properties,
      Consumer<K, V> consumer,
      Store<K, V> store,
      Producer<String, R> producer,
      ResponseBuilder<V, R> responseBuilder) {

    this.consumer = consumer;
    this.store = store;
    this.producer = producer;
    this.responseTopic = properties.getTopics().getResponse().getName();
    this.responseBuilder = responseBuilder;
  }

  @Scheduled(fixedDelayString = "500")
  public void poll() {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));
    records.forEach(
        r -> {
          String replyTopic = getReplyTopic(r.headers());
          String txnId = getTransactionId(r.headers());
          logger.debug(
              "Received entity update: topic={}, partition={}, offset={}, key={}, value={}, transaction-id={}",
              r.topic(),
              r.partition(),
              r.offset(),
              r.key(),
              r.value(),
              txnId);

          // update received
          K id = r.key();
          V entity = r.value();

          // update store
          R response = (entity != null) ? save(id, entity) : delete(id);

          // send response
          sendReply(replyTopic, txnId, response);
        });
  }

  private R save(K id, V entity) {
    try {
      store.add(id, entity);
      return responseBuilder.success(entity, "Successfully saved entity");
    } catch (Exception e) {
      logger.error("Failed to save entity: id={}, value={}", id, entity, e);
      return responseBuilder.failed(entity, e.getMessage());
    }
  }

  private R delete(K id) {
    try {
      V removed = store.remove(id);
      return responseBuilder.success(removed, "Successfully deleted entity");
    } catch (Exception e) {
      logger.error("Failed to delete entity: id={}", id, e);
      return responseBuilder.failed(null, e.getMessage());
    }
  }

  private String getReplyTopic(Headers headers) {
    String topic = HeaderUtils.getReplyTopic(headers);
    if (topic == null) {
      logger.error("reply-to header is missing, will not be able to send reply: {}", headers);
    }
    return topic;
  }

  private String getTransactionId(Headers headers) {
    String txnId = HeaderUtils.getTransactionId(headers);
    if (txnId == null) {
      logger.error("transaction-id header is missing, will not be able to send reply: {}", headers);
    }
    return txnId;
  }

  private void sendReply(String topic, String txnId, R response) {
    // only send response when reply topic in message headers == the response topic for this
    // application instance
    if (responseTopic.equals(topic)) {
      Headers headers = new RecordHeaders();
      headers.add(new RecordHeader("transaction-id", txnId.getBytes()));
      ProducerRecord<String, R> record =
          new ProducerRecord<>(topic, null, null, null, response, headers);

      logger.debug(
          "Sending reply: topic={}, key={}, value={}, transaction-id={}",
          record.topic(),
          record.key(),
          record.value(),
          txnId);

      producer.send(
          record,
          (meta, ex) -> {
            if (ex == null) {
              logger.debug(
                  "Successfully sent response: topic={}, partition={}, offset={}, timestamp={}, key={}, value={}, transaction-id={}",
                  meta.topic(),
                  meta.partition(),
                  meta.offset(),
                  meta.timestamp(),
                  record.key(),
                  record.value(),
                  txnId);

            } else {
              logger.error(
                  "Failed to send response: topic={}, partition={}, key={}, value={}, transaction-id={}",
                  meta.topic(),
                  meta.partition(),
                  record.key(),
                  record.value(),
                  txnId,
                  ex);
            }
          });
    }
  }
}
