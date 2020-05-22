package sandbox.kafka.requestreply.common;

import java.util.Collection;

public interface Store<K, V> {

  /**
   * Get all values in the store.
   *
   * @return all values
   */
  Collection<V> getAll();

  /**
   * Get value by key.
   *
   * @param key key
   * @return value
   */
  V get(K key);

  /**
   * Add a value to the store.
   *
   * @param key key
   * @param value value
   */
  void add(K key, V value);

  /**
   * Remove a value from the store.
   *
   * @param key key
   * @return value (null if key did not exist in the store)
   */
  V remove(K key);
}
