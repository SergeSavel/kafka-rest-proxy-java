package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;

interface ConsumerCache<K, V> {

	Consumer<K, V> getConsumer(String groupId, String clientId);

	void releaseConsumer(Consumer<K, V> consumer);
}
