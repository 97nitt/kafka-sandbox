package sandbox.kafka.requestreply.common;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * Kafka consumer that reacts to transaction responses to complete {@link DeferredResult}s.
 *
 * @param <K> response key type
 * @param <V> response value type
 */
public class ResponseListener<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(ResponseListener.class);

  private final Consumer<K, V> consumer;
  private final DeferredResultService deferredResults;

  /**
   * Constructor.
   *
   * @param consumer Kafka consumer of transaction responses
   */
  public ResponseListener(Consumer<K, V> consumer, DeferredResultService deferredResults) {
    this.consumer = consumer;
    this.deferredResults = deferredResults;
  }

  @Scheduled(fixedDelayString = "500")
  public void poll() {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));
    records.forEach(
        r -> {
          // get transaction id from message header
          String txnId = HeaderUtils.getTransactionId(r.headers());

          logger.debug(
              "Received response: topic={}, partition={}, offset={}, key={}, value={}, transaction-id={}",
              r.topic(),
              r.partition(),
              r.offset(),
              r.key(),
              r.value(),
              txnId);

          // lookup deferred API response
          DeferredResult<V> response = deferredResults.get(txnId);
          if (response != null) {
            logger.debug("Completing deferred API response for transaction-id={}", txnId);
            response.setResult(r.value());
          } else {
            logger.warn(
                "Did not find a deferred API response for transaction-id={}", txnId);
          }
        });
  }
}
