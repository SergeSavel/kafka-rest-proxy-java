package pro.savel.krp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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

	public void postTopic(String topic, String key, String data) {

		if (key == null)
			kafkaTemplate.send(topic, data);
		else
			kafkaTemplate.send(topic, key, data);
	}

	public int[] getTopicPartitions(String topic) {

		try (var consumer = kafkaConsumerFactory.createConsumer(UUID.randomUUID().toString(), null)) {
			var partitions = consumer.partitionsFor(topic);

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
			extraProps.put("max.poll.records", limit);
		}

		try (var consumer = kafkaConsumerFactory.createConsumer(group, null, null, extraProps)) {

			var result = new ArrayList<Record>();

			var topicPartition = new TopicPartition(topic, partition);
			consumer.assign(Collections.singletonList(topicPartition));
			consumer.seek(topicPartition, offset);
			var consumerRecords = consumer.poll(Duration.ZERO);
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
