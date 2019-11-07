package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Component
public class ConsumerCacheImpl<K, V> implements ConsumerCache<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	public ConsumerCacheImpl(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	@Override
	public Consumer<K, V> getConsumer(String groupId, String clientId) {
		return consumerFactory.createConsumer(groupId, clientId);
	}

	@Override
	public void releaseConsumer(Consumer<K, V> consumer) {
		consumer.close();
	}
}
