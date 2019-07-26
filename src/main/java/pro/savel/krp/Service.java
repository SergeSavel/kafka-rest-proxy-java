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
		try {
			future.get();
		} catch (Exception e) {
			throw new RuntimeException("Unable to send message: " + e.getMessage(), e);
		}
	}

	public int[] getTopicPartitions(String topic) {

		try (Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer(UUID.randomUUID().toString(), null)) {
			List<PartitionInfo> partitions = consumer.partitionsFor(topic);

			return partitions.stream()
					.mapToInt(PartitionInfo::partition)
					.toArray();
		}
	}

	public Collection<Record> getData(String topic, int partition, String group, Long offset, Long limit) {

		if (group == null)
			group = UUID.randomUUID().toString();

		Properties extraProps = null;
		if (limit != null) {
			extraProps = new Properties();
			extraProps.put("max.poll.records", limit.toString());
		}

		try (Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer(group, null, null, extraProps)) {

			List<Record> result;

			TopicPartition topicPartition = new TopicPartition(topic, partition);
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
			result = records.stream()
					.map(this::getRecord)
					.collect(Collectors.toList());

			return result;
		}
	}

	private Record getRecord(ConsumerRecord<String, String> consumerRecord) {
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
