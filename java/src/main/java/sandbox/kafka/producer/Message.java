package sandbox.kafka.producer;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka message.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Getter
public class Message<K, V> {

	/* message key */
	private final K key;

	/* message value */
	private final V value;

	/* message headers */
	private final Map<String, String> headers;

	/**
	 * Constructor.
	 *
	 * @param value message value
	 */
	public Message(V value) {
		this(null, value);
	}

	/**
	 * Constructor.
	 *
	 * @param key message key
	 * @param value message value
	 */
	public Message(K key, V value) {
		this.key = key;
		this.value = value;
		this.headers = new HashMap<>();
	}

	/**
	 * Add message header.
	 *
	 * @param name header name
	 * @param value header value
	 */
	public void addHeader(String name, String value) {
		headers.put(name, value);
	}

	/**
	 * Get message header.
	 *
	 * @param name header name
	 * @return header value, null if header does not exist
	 */
	public String getHeader(String name) {
		return headers.get(name);
	}
}
