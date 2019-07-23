package pro.savel.krp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

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

	public String getData(String topic, int partition, long offset, long limit) {

		if (limit == 0)
			limit = 1000;

		try (var consumer = kafkaConsumerFactory.createConsumer(UUID.randomUUID().toString(), null)) {

			var topicPartition = new TopicPartition(topic, partition);
			consumer.assign(Arrays.asList(topicPartition));
			consumer.seek(topicPartition, offset);
			var consumerRecords = consumer.poll(Duration.ZERO);
			var records = consumerRecords.records(topicPartition);
			for (ConsumerRecord<String, String> record : records) {
				record.
			}

		}

		//props.setProperty("bootstrap.servers", "localhost:9092");
		//props.setProperty("group.id", "test");
		//props.setProperty("enable.auto.commit", "false");
		//props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		//consumer.seek();
		//consumer.subscribe(Arrays.asList("foo", "bar"));
		//final int minBatchSize = 200;
		//List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		//while (true) {
		//	ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
		//	for (ConsumerRecord<String, String> record : records) {
		//		buffer.add(record);
		//	}
		//	if (buffer.size() >= minBatchSize) {
		//		insertIntoDb(buffer);
		//		consumer.commitSync();
		//		buffer.clear();
		//	}
		//}

		return null;
	}
}
