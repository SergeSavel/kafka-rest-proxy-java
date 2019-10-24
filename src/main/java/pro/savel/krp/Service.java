package pro.savel.krp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate

	@Autowired
	ConsumerCache<String, String> consumerCache;

	public Mono<Void> postData(String topic, Mono<Message> monoMessage) {

		return monoMessage
				.map(message -> createProducerRecord(topic, message))
				.flatMap(record -> Mono.fromFuture(kafkaTemplate.send(record).completable()))
				.then();
	}

	private ProducerRecord<String, String> createProducerRecord(String topic, Message message) {

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.getKey(), message.getValue());
		Map<String, String> messageHeaders = message.getHeaders();
		if (messageHeaders != null) {
			Headers headers = producerRecord.headers();
			messageHeaders.forEach(
					(key, value) -> headers.add(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
		}
		return producerRecord;
	}

	public Mono<TopicInfo> getTopicInfo(String topic, String groupId, String clientId) {

		return Mono.just(null)
				.publishOn(Schedulers.elastic())
				.map(empty -> {
					Consumer<String, String> consumer = null;
					try {
						consumer = consumerCache.getConsumer(groupId, clientId);
						return createTopicInfo(topic, consumer);
					} finally {
						if (consumer != null) consumerCache.releaseConsumer(consumer);
					}
				});
	}

	private TopicInfo createTopicInfo(String topic, Consumer<String, String> consumer) {

		Map<TopicPartition, org.apache.kafka.common.PartitionInfo> partitionInfos = consumer.partitionsFor(topic)
				.stream().collect(Collectors.toMap(
						info -> new TopicPartition(info.topic(), info.partition()),
						info -> info));

		Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitionInfos.keySet());
		Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitionInfos.keySet());

		List<TopicInfo.PartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
		for (Map.Entry<TopicPartition, org.apache.kafka.common.PartitionInfo> entry : partitionInfos.entrySet()) {
			TopicPartition tp = entry.getKey();
			org.apache.kafka.common.PartitionInfo pi = entry.getValue();
			topic = pi.topic();
			TopicInfo.PartitionInfo partiton = TopicInfo.createPartiton(
					pi.partition(), beginningOffsets.get(tp), endOffsets.get(tp));
			partitions.add(partiton);
		}

		partitions.sort(Comparator.comparingInt(partition -> partition.name));

		return new TopicInfo(topic, partitions);
	}

	public Collection<Record> getData(String topic, int partition, long offset, Long timeout, Long limit,
	                                  String idHeader, String groupId, String clientId) {

		if (timeout == null)
			timeout = 1000L;

		Map<String, Object> consumerProps = this.consumerProps;
		if (limit != null || groupId != null || clientId != null) {
			consumerProps = new HashMap<>(consumerProps);
			if (limit != null) consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, limit);
			if (groupId != null) consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			if (clientId != null) consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		}

		TopicPartition topicPartition = new TopicPartition(topic, partition);
		ConsumerRecords<String, String> consumerRecords;
		try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps, keyDeserializer, valueDeserializer)) {
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);
			consumerRecords = consumer.poll(Duration.ofMillis(timeout));
		}

		List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
		List<Record> result = records.stream()
				.map(this::createRecord)
				.collect(Collectors.toList());

		for (Record record : result) {
			record.calcID(idHeader);
		}

		return result;
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
