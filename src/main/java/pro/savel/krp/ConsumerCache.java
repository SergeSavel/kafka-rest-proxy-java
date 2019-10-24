package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConsumerCache<K, V> {

	@Autowired
	private final ConsumerFactory<K, V> kafkaConsumerFactory;

	private final ConcurrentMap<String, ConcurrentMap<String, Consumer<K, V>>> cache = new ConcurrentHashMap<>();

	public Consumer<K, V> getConsumer(String groupId, String clientIdSuffix) {

		if (groupId == null || clientIdSuffix == null)
			return createConsumer(groupId, clientIdSuffix);
		else {
			var groupConsumers = cache.computeIfAbsent(groupId, (key) -> new ConcurrentHashMap<>());

			// dummy!
			return createConsumer(groupId, clientIdSuffix);
		}
	}

	public void freeConsumer(Consumer<K, V> consumer) {

	}

	private Consumer<K, V> createConsumer(String groupId, String clientIdSuffix) {
		return kafkaConsumerFactory.createConsumer(groupId, clientIdSuffix);
	}
}
