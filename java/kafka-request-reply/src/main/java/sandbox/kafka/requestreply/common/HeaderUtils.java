package sandbox.kafka.requestreply.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Provides utility methods for creating and parsing Kafka message headers.
 *
 */
public class HeaderUtils {

  private static final String HEADER_REPLY_TOPIC = "reply-to";
  private static final String HEADER_TRANSACTION_ID = "transaction-id";

  /**
   * Create Kafka message headers.
   *
   * @param replyTopic reply topic
   * @param transactionId transaction id
   * @return Kafka message headers
   */
  public static Collection<Header> createHeaders(String replyTopic, String transactionId) {
    return Arrays.asList(
        new RecordHeader(HEADER_REPLY_TOPIC, replyTopic.getBytes()),
        new RecordHeader(HEADER_TRANSACTION_ID, transactionId.getBytes()));
  }

  /**
   * Get value of reply topic header.
   *
   * @param headers Kafka message headers
   * @return reply topic
   */
  public static String getReplyTopic(Headers headers) {
    return getStringValue(headers, HEADER_REPLY_TOPIC);
  }

  /**
   * Get value of transaction id header.
   *
   * @param headers Kafka message headers
   * @return transaction id
   */
  public static String getTransactionId(Headers headers) {
    return getStringValue(headers, HEADER_TRANSACTION_ID);
  }

  /**
   * Get header value as a String.
   *
   * @param headers Kafka message headers
   * @param name header name
   * @return header value, or null if header does not exist
   */
  public static String getStringValue(Headers headers, String name) {
    return getHeaderValue(headers, name, String::new);
  }

  /**
   * Get header value.
   *
   * @param headers Kafka message headers
   * @param name header name
   * @param toT function that converts header value from a byte array to desired type T
   * @param <T> desired header value type
   * @return header value, or null if header does not exist
   */
  private static <T> T getHeaderValue(Headers headers, String name, Function<byte[], T> toT) {
    Header header = headers.lastHeader(name);
    if (header != null && header.value() != null) {
      return toT.apply(header.value());
    } else {
      return null;
    }
  }
}
