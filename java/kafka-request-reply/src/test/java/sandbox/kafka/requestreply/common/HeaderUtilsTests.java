package sandbox.kafka.requestreply.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collection;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

public class HeaderUtilsTests {

  @Test
  public void createHeaders() {
    // given
    String replyTopic = "reply";
    String transactionId = "txn-id";

    // when
    Collection<Header> headers = HeaderUtils.createHeaders(replyTopic, transactionId);

    // then
    assertNotNull(headers);
    assertEquals(2, headers.size());
    headers.forEach(h -> {
      switch(h.key()) {
        case "reply-to":
          assertArrayEquals(replyTopic.getBytes(), h.value());
          break;

        case "transaction-id":
          assertArrayEquals(transactionId.getBytes(), h.value());
          break;

        default:
          fail("Unexpected header key: " + h.key());
      }
    });
  }

  @Test
  public void getHeaderValues() {
    // given
    Headers headers = new RecordHeaders(Arrays.asList(
        new RecordHeader("reply-to", "reply".getBytes()),
        new RecordHeader("transaction-id", "txn-id".getBytes()),
        new RecordHeader("null", (byte[]) null)));

    // then
    assertEquals("reply", HeaderUtils.getReplyTopic(headers));
    assertEquals("txn-id", HeaderUtils.getTransactionId(headers));
    assertNull(HeaderUtils.getStringValue(headers, "null"));
    assertNull(HeaderUtils.getStringValue(headers, "missing"));
  }
}
