package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.Topic;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class Service {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ConsumerFactory<String, String> kafkaConsumerFactory;

	public Service(KafkaTemplate<String, String> kafkaTemplate, ConsumerFactory<String, String> kafkaConsumerFactory) {
		this.kafkaTemplate = kafkaTemplate;
		this.kafkaConsumerFactory = kafkaConsumerFactory;
	}

	public void postData(String topic, String recordKey, Map<String, String> recordHeaders, String recordValue) {

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);

		if (recordHeaders != null) {
			Headers headers = producerRecord.headers();
			recordHeaders.forEach((key, value) -> headers.add(key, value.getBytes(StandardCharsets.UTF_8)));
		}

		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(producerRecord);
		SendResult<String, String> sendResult;
		try {
			sendResult = future.get();
		} catch (Exception e) {
			throw new RuntimeException("Unable to send message: " + e.getMessage(), e);
		}
	}

	public Topic getTopicInfo(String topicName, String consumerGroup, String clientIdPrefix, String clientIdSuffix) {

		Map<TopicPartition, PartitionInfo> partitionInfos;
		Map<TopicPartition, Long> beginningOffsets;
		Map<TopicPartition, Long> endOffsets;
		try (Consumer<String, String> consumer =
				     kafkaConsumerFactory.createConsumer(consumerGroup, clientIdPrefix, clientIdSuffix)) {

			partitionInfos = consumer.partitionsFor(topicName)
					.stream().collect(Collectors.toMap(
							info -> new TopicPartition(info.topic(), info.partition()),
							info -> info));

			beginningOffsets = consumer.beginningOffsets(partitionInfos.keySet());
			endOffsets = consumer.endOffsets(partitionInfos.keySet());
		}

		List<Topic.Partition> partitions = new ArrayList<>(partitionInfos.size());
		for (Map.Entry<TopicPartition, PartitionInfo> entry : partitionInfos.entrySet()) {
			TopicPartition tp = entry.getKey();
			PartitionInfo pi = entry.getValue();
			topicName = pi.topic();
			Topic.Partition partition = Topic.createPartiton(
					pi.partition(), beginningOffsets.get(tp), endOffsets.get(tp));
			partitions.add(partition);
		}

		partitions.sort(Comparator.comparingInt(partition -> partition.name));

		return new Topic(topicName, partitions);
	}

	public Collection<Record> getData(String topic, int partition, long offset, String commit,
	                                  Long timeout, Long limit, String idHeader, String consumerGroup,
	                                  String clientIdPrefix, String clientIdSuffix) {

		if (timeout == null)
			timeout = 1000L;

		Properties extraProps = null;
		if (limit != null) {
			extraProps = new Properties();
			extraProps.put("max.poll.records", limit.toString());
		}

		TopicPartition topicPartition = new TopicPartition(topic, partition);

		ConsumerRecords<String, String> consumerRecords;
		try (Consumer<String, String> consumer =
				     kafkaConsumerFactory.createConsumer(consumerGroup, clientIdPrefix, clientIdSuffix, extraProps)) {

			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);

			if (consumerGroup != null && "before".equals(commit))
				consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));

			consumerRecords = consumer.poll(Duration.ofMillis(timeout));

			if (consumerGroup != null && "after".equals(commit))
				consumer.commitSync();
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
			headersMap.put(header.key(), new String(header.value()));
		return new Record(
			consumerRecord.timestamp(),
			consumerRecord.offset(),
			consumerRecord.key(),
			headersMap,
			consumerRecord.value()
		);
	}
}
