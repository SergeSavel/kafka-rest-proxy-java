package pro.savel.krp;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

	public void post(String topic, String recordKey, Map<String, String> recordHeaders, String recordValue) {

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);

		Headers headers = producerRecord.headers();
		recordHeaders.forEach((key, value) -> headers.add(key, value.getBytes()));

		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(producerRecord);
		SendResult<String, String> sendResult;
		try {
			sendResult = future.get();
		} catch (Exception e) {
			throw new RuntimeException("Unable to send message: " + e.getMessage(), e);
		}
	}

	public Topic getTopic(String topicName, String consumerGroup, String clientIdPrefix, String clientIdSuffix) {

		if (consumerGroup == null)
			consumerGroup = getDefaultConsumerGroup();

		try (Consumer<String, String> consumer =
				     kafkaConsumerFactory.createConsumer(consumerGroup, clientIdPrefix, clientIdSuffix)) {

			Map<TopicPartition, PartitionInfo> partitionInfos = consumer.partitionsFor(topicName)
					.stream().collect(Collectors.toMap(
							info -> new TopicPartition(info.topic(), info.partition()),
							info -> info));

			Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitionInfos.keySet());
			Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitionInfos.keySet());

			List<Topic.Partition> partitions = new ArrayList<>(partitionInfos.size());
			for (Map.Entry<TopicPartition, PartitionInfo> entry : partitionInfos.entrySet()) {
				TopicPartition tp = entry.getKey();
				PartitionInfo pi = entry.getValue();
				topicName = pi.topic();
				Topic.Partition partition = Topic.createPartiton(
						pi.partition(), beginningOffsets.get(tp), endOffsets.get(tp));
				partitions.add(partition);
			}

			return new Topic(topicName, partitions);
		}
	}

	public Collection<Record> getData(String topic, int partition, long offset, Long limit,
	                                  String consumerGroup, String clientIdPrefix, String clientIdSuffix) {

		if (consumerGroup == null)
			consumerGroup = getDefaultConsumerGroup();

		Properties extraProps = null;
		if (limit != null) {
			extraProps = new Properties();
			extraProps.put("max.poll.records", limit.toString());
		}

		try (Consumer<String, String> consumer =
				     kafkaConsumerFactory.createConsumer(consumerGroup, clientIdPrefix, clientIdSuffix, extraProps)) {

			List<Record> result;

			TopicPartition topicPartition = new TopicPartition(topic, partition);
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
			result = records.stream()
					.map(this::createRecord)
					.collect(Collectors.toList());

			return result;
		}
	}

	private String getDefaultConsumerGroup() {
		return UUID.randomUUID().toString();
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
