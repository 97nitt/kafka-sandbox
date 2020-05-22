package sandbox.kafka.requestreply.common;

public interface ResponseBuilder<V, R> {

  /**
   * Build response for a successful transaction.
   *
   * @param entity entity
   * @param message contextual message
   * @return response
   */
  R success(V entity, String message);

  /**
   * Build response for a failed transaction.
   *
   * @param entity entity
   * @param message contextual message
   * @return response
   */
  R failed(V entity, String message);
}
