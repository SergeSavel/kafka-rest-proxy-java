package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.TopicInfo;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class Service {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ConsumerCache<String, String> consumerCache;

	public Service(KafkaTemplate<String, String> kafkaTemplate, ConsumerCache<String, String> consumerCache) {
		this.kafkaTemplate = kafkaTemplate;
		this.consumerCache = consumerCache;
	}

	public Mono<Void> postData(String topic, Mono<Message> monoMessage) {

		return monoMessage
				.map(message -> createProducerRecord(topic, message))
				.flatMap(record -> Mono.fromFuture(kafkaTemplate.send(record).completable()))
				.then();
	}

	private ProducerRecord<String, String> createProducerRecord(String topic, Message message) {

		var producerRecord = new ProducerRecord<>(topic, message.getKey(), message.getValue());
		var messageHeaders = message.getHeaders();
		if (messageHeaders != null) {
			Headers headers = producerRecord.headers();
			messageHeaders.forEach(
					(key, value) -> headers.add(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
		}
		return producerRecord;
	}

	public Mono<TopicInfo> getTopicInfo(String topic, Integer partition, String groupId, String clientId) {

		return Mono.using(() -> consumerCache.getConsumer(groupId, clientId),
				consumer -> Mono.just(createTopicInfo(topic, partition, consumer))
						.subscribeOn(Schedulers.elastic()),
				consumerCache::releaseConsumer);
	}

	private TopicInfo createTopicInfo(final String topic, final Integer partition, Consumer<String, String> consumer) {

		Collection<TopicPartition> topicPartitions;
		if (partition == null)
			topicPartitions = consumer.partitionsFor(topic).stream()
					.map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
					.collect(Collectors.toUnmodifiableSet());
		else
			topicPartitions = Collections.singleton(new TopicPartition(topic, partition));

		var beginningOffsets = consumer.beginningOffsets(topicPartitions);
		var endOffsets = consumer.endOffsets(topicPartitions);

		var partitions = new ArrayList<TopicInfo.PartitionInfo>(topicPartitions.size());
		topicPartitions.forEach(tp ->
				partitions.add(TopicInfo.createPartiton(tp.partition(), beginningOffsets.get(tp), endOffsets.get(tp))));

		partitions.sort(Comparator.comparingInt(partitionInfo -> partitionInfo.name));

		return new TopicInfo(topic, partitions);
	}

	public Mono<Collection<Record>> getData(String topic, int partition, long offset, Long timeout,
	                                        String groupId, String clientId) {

		if (timeout == null)
			timeout = 1000L;
		final long _timeout = timeout;

		final TopicPartition topicPartition = new TopicPartition(topic, partition);

		return Mono.using(
				() -> consumerCache.getConsumer(groupId, clientId),
				consumer -> Mono.just(getConsumerRecords(topicPartition, offset, _timeout, consumer))
						.subscribeOn(Schedulers.elastic()),
				consumerCache::releaseConsumer)
				.flatMapIterable(consumerRecords -> consumerRecords.records(topicPartition))
				.map(this::createRecord)
				.collectList()
				.map(list -> list);
	}

	private ConsumerRecords<String, String> getConsumerRecords(TopicPartition topicPartition, long offset, Long timeout,
	                                                           Consumer<String, String> consumer) {

		consumer.assign(Collections.singletonList(topicPartition));
		consumer.seek(topicPartition, offset);
		return consumer.poll(Duration.ofMillis(timeout));
	}

	private Record createRecord(ConsumerRecord<String, String> consumerRecord) {

		Map<String, String> headersMap = new HashMap<String, String>();
		for (Header header : consumerRecord.headers())
			headersMap.put(
					header.key(),
					header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));

		return new Record(
			consumerRecord.timestamp(),
			consumerRecord.offset(),
			consumerRecord.key(),
			headersMap,
			consumerRecord.value()
		);
	}
}
