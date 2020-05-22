package sandbox.kafka.requestreply.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link Store} implementation backed by a local cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LocalStore<K, V> implements Store<K, V> {

  private final Map<K, V> cache;

  /**
   * Default constructor.
   *
   */
  public LocalStore() {
    this(new HashMap<>());
  }

  /**
   * Package-private constructor, for unit testing.
   *
   * @param cache local cache
   */
  LocalStore(Map<K, V> cache) {
    this.cache = cache;
  }

  @Override
  public Collection<V> getAll() {
    return cache.values();
  }

  @Override
  public V get(K key) {
    return cache.get(key);
  }

  @Override
  public void add(K key, V value) {
    cache.put(key, value);
  }

  @Override
  public V remove(K key) {
    return cache.remove(key);
  }
}
