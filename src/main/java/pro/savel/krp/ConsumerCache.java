package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@EnableScheduling
public class ConsumerCache<K, V> implements DisposableBean {

	private static final long CONSUMER_TTL_MILLIS = 15000L;

	private final ConsumerFactory<K, V> consumerFactory;

	private ConcurrentHashMap<String, ConcurrentMap<String, ConsumerWrapper>> wrappers = new ConcurrentHashMap<>();

	public ConsumerCache(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public Consumer<K, V> getConsumer(String groupId, String clientId) {

		if (groupId == null || clientId == null)
			return consumerFactory.createConsumer(groupId, clientId);

		var currentGroup = wrappers.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>());

		ConsumerWrapper wrapper;
		do {
			wrapper = currentGroup.computeIfAbsent(clientId, k -> new ConsumerWrapper(groupId, clientId));
			if (!wrapper.locked.compareAndSet(false, true))
				throw new ConsumerLockedException();
		} while (currentGroup.containsKey(clientId));

		return wrapper.consumer;
	}

	public void releaseConsumer(Consumer<K, V> consumer, String groupId, String clientId) {

		if (groupId == null || clientId == null) {
			consumer.close();
			return;
		}

		var currentGroup = wrappers.get(groupId);
		var wrapper = currentGroup.get(clientId);
		if (wrapper == null) {
			consumer.close();
			return;
		}

		if (System.currentTimeMillis() - wrapper.created >= CONSUMER_TTL_MILLIS) {
			currentGroup.remove(wrapper.clientId);
			wrapper.consumer.close();
		}

		if (!wrapper.locked.compareAndSet(true, false))
			throw new ConsumerNotLockedException();
	}

	@Scheduled(fixedRate = CONSUMER_TTL_MILLIS)
	private void cleanup() {
		for (var group : wrappers.values()) {
			var wrappers = new ArrayList<>(group.values());
			for (var wrapper : wrappers) {
				if (System.currentTimeMillis() - wrapper.created >= CONSUMER_TTL_MILLIS) {
					group.remove(wrapper.clientId);
					if (!wrapper.locked.get()) {
						wrapper.consumer.close();
					}
				}
			}
		}
	}

	@Override
	public void destroy() {
		wrappers.values().stream()
				.flatMap(clientIds -> clientIds.values().stream())
				.forEach(wrapper -> wrapper.consumer.close());
	}

	@ResponseStatus(value = HttpStatus.CONFLICT, reason = "The consumer is already locked.")
	static class ConsumerLockedException extends RuntimeException {
	}

	@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "The consumer is not locked.")
	static class ConsumerNotLockedException extends RuntimeException {
	}

	private class ConsumerWrapper {
		final String groupId, clientId;
		final long created;
		final Consumer<K, V> consumer;
		final AtomicBoolean locked;

		private ConsumerWrapper(final String groupId, final String clientId) {
			this.groupId = groupId;
			this.clientId = clientId;
			this.created = System.currentTimeMillis();
			this.consumer = consumerFactory.createConsumer(groupId, clientId);
			this.locked = new AtomicBoolean();
		}
	}
}
