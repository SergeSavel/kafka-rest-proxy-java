package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component
@EnableScheduling
public class ConsumerCache<K, V> {

	private static final long CONSUMER_TTL_MILLIS = 10000L;

	private final ConsumerFactory<K, V> consumerFactory;
	private ConcurrentHashMap<String, ConcurrentMap<String, Lock>> locks = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, ConcurrentMap<String, ConsumerWrapper>> ids = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Consumer<K, V>, ConsumerWrapper> consumers = new ConcurrentHashMap<>();

	public ConsumerCache(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public Consumer<K, V> getConsumer(final String groupId, final String clientId) {

		if (groupId == null || clientId == null)
			return consumerFactory.createConsumer(groupId, clientId);

		final var lock = lock(groupId, clientId);

		final var wrapper = ids
				.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
				.computeIfAbsent(clientId, k -> new ConsumerWrapper(groupId, clientId));
		wrapper.lock();

		consumers.putIfAbsent(wrapper.consumer, wrapper);

		lock.unlock();

		return wrapper.consumer;
	}

	public void releaseConsumer(Consumer<K, V> consumer) {

		var wrapper = consumers.get(consumer);

		if (wrapper == null) {
			consumer.close();
			return;
		}

		final var lock = lock(wrapper.groupId, wrapper.clientId);

		checkTTL(wrapper);
		wrapper.unlock();

		lock.unlock();
	}

	private Lock lock(final String groupId, final String clientId) {
		var lock = getLock(groupId, clientId);
		lock.lock();
		return lock;
	}

	private Lock tryLock(final String groupId, final String clientId) {
		var lock = getLock(groupId, clientId);
		if (lock.tryLock())
			return lock;
		else
			return null;
	}

	private Lock getLock(final String groupId, final String clientId) {
		return locks
				.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
				.computeIfAbsent(clientId, k -> new ReentrantLock());
	}

	@PreDestroy
	private void done() {
		consumers.keySet().forEach(Consumer::close);
	}

	@Scheduled(fixedRate = CONSUMER_TTL_MILLIS)
	private void cleanup() {
		var wrappers = new ArrayList<>(consumers.values());
		for (var wrapper : wrappers) {
			var lock = tryLock(wrapper.groupId, wrapper.clientId);
			if (lock == null) continue;

			if (wrapper.locked()) continue;

			wrapper.lock();
			checkTTL(wrapper);
			wrapper.unlock();

			lock.unlock();
		}
	}

	private void checkTTL(ConsumerWrapper wrapper) {
		if (System.currentTimeMillis() - wrapper.created >= CONSUMER_TTL_MILLIS) {
			wrapper.consumer.close();
			ids.get(wrapper.groupId).remove(wrapper.clientId);
			locks.get(wrapper.groupId).remove(wrapper.clientId);
			consumers.remove(wrapper.consumer);
		}
	}

	private class ConsumerWrapper {
		final String groupId, clientId;
		final long created;
		final Consumer<K, V> consumer;
		private CountDownLatch latch;

		private ConsumerWrapper(final String groupId, final String clientId) {
			this.groupId = groupId;
			this.clientId = clientId;
			this.created = System.currentTimeMillis();
			this.consumer = consumerFactory.createConsumer(groupId, clientId);
		}

		void lock() {
			waitForRelease();
			latch = new CountDownLatch(1);
		}

		void unlock() {
			latch.countDown();
		}

		boolean locked() {
			return (latch == null || latch.getCount() == 0L);
		}

		private void waitForRelease() {
			if (latch == null) return;
			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new RuntimeException("Thread interrupted.", e);
			}
		}
	}
}
