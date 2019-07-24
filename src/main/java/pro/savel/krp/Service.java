package pro.savel.krp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import pro.savel.krp.objects.Record;

import java.time.Duration;
import java.util.*;

@org.springframework.stereotype.Service
public class Service {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ConsumerFactory<String, String> kafkaConsumerFactory;

	public Service(KafkaTemplate<String, String> kafkaTemplate, ConsumerFactory<String, String> kafkaConsumerFactory) {
		this.kafkaTemplate = kafkaTemplate;
		this.kafkaConsumerFactory = kafkaConsumerFactory;
	}

	public void post(String topic, String recordKey, Map<String, String> recordHeaders, String recordValue) {

		var producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);

		var headers = producerRecord.headers();
		recordHeaders.forEach((key, value) -> headers.add(key, value.getBytes()));

		var future = kafkaTemplate.send(producerRecord);
		try {
			future.get();
		} catch (Exception e) {
			throw new RuntimeException("Unable to send message: " + e.getMessage(), e);
		}
	}

	public int[] getTopicPartitions(String topic) {

		try (var consumer = kafkaConsumerFactory.createConsumer(UUID.randomUUID().toString(), null)) {
			var partitions = consumer.partitionsFor(topic);

			return partitions.stream()
					.mapToInt(PartitionInfo::partition)
					.toArray();
		}
	}

	public Collection<Record> getData(String topic, int partition, String consumerGroup, Long offset, Long limit) {

		if (consumerGroup == null)
			consumerGroup = UUID.randomUUID().toString();

		Properties extraProps = null;
		if (limit != null) {
			extraProps = new Properties();
			extraProps.put("max.poll.records", limit.toString());
		}

		try (var consumer = kafkaConsumerFactory.createConsumer(consumerGroup, null, null, extraProps)) {

			var result = new ArrayList<Record>();

			var topicPartition = new TopicPartition(topic, partition);
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);

			var consumerRecords = consumer.poll(Duration.ofMillis(1000));
			var records = consumerRecords.records(topicPartition);
			for (var record : records)
				result.add(getRecord(record));

			return result;
		}
	}

	private Record getRecord(ConsumerRecord<String, String> consumerRecord) {
		var headersMap = new HashMap<String, String>();
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
