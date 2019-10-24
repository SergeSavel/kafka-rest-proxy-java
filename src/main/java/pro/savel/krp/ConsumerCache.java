package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class ConsumerCache<K, V> {

	@Autowired
	private final ConsumerFactory<K, V> kafkaConsumerFactory;

	private final ConcurrentMap<String, ConcurrentMap<String, ConsumerWrapper<K, V>>> cache = new ConcurrentHashMap<>();

	private final ThreadLocal<ConsumerWrapper<K, V>> currentWrapper = new ThreadLocal<>();

	public Consumer<K, V> getConsumer(String groupId, String clientId) {

		if (currentWrapper.get() != null)
			throw new IllegalStateException("Current thread already holds a consumer.");

		if (groupId == null || clientId == null)
			return createConsumer(groupId, clientId);
		!!!
		else {

			var wrapper = cache
					.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
					.computeIfAbsent(clientId, k -> new ConsumerWrapper<>(createConsumer(groupId, clientId)));

			if (!wrapper.lock.tryLock())
				throw new ConsumerLockedException();

			currentWrapper.set(wrapper);

			return wrapper.consumer;
		}
	}

	public void releaseConsumer() {
		var wrapper = currentWrapper.get();
		if (wrapper != null) {
			wrapper.lastUsed = System.currentTimeMillis();
			currentWrapper.remove();
			wrapper.lock.unlock();
		}
	}

	private Consumer<K, V> createConsumer(String groupId, String clientIdSuffix) {
		return kafkaConsumerFactory.createConsumer(groupId, clientIdSuffix);
	}

	private class ConsumerWrapper<K, V> {
		private final Lock lock = new ReentrantLock();
		private final Consumer<K, V> consumer;
		private long lastUsed;

		private ConsumerWrapper(Consumer<K, V> consumer) {
			this.consumer = consumer;
		}
	}

}
