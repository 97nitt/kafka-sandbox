package sandbox.kafka.requestreply.common;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * Create {@link DeferredResult}s and store them in a local cache.
 *
 */
@Service
public class DeferredResultService {

  private final Map<String, DeferredResult<?>> cache;

  /**
   * Default constructor.
   *
   */
  public DeferredResultService() {
    this(new HashMap<>());
  }

  /**
   * Package-private constructor, for unit testing.
   *
   * @param cache local cache of {@link DeferredResult}s
   */
  DeferredResultService(Map<String, DeferredResult<?>> cache) {
    this.cache = cache;
  }

  /**
   * Create a deferred result and add to cache.
   *
   * @param transactionId globally unique transaction id
   */
  public <T> DeferredResult<T> create(String transactionId) {
    DeferredResult<T> result = new DeferredResult<>();
    cache.put(transactionId, result);
    return result;
  }

  /**
   * Get (and remove) result from cache.
   *
   * @param transactionId transaction id
   * @return deferred result
   */
  @SuppressWarnings("unchecked")
  public <T> DeferredResult<T> get(String transactionId) {
    return (DeferredResult<T>) cache.remove(transactionId);
  }
}
