package pro.savel.krp;

import org.springframework.kafka.core.KafkaTemplate;

@org.springframework.stereotype.Service
public class Service {

	private final KafkaTemplate<String, String> kafkaTemplate;

	public Service(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void postTopic(String topic, String key, String data) {

		if (key == null)
			kafkaTemplate.send(topic, data);
		else
			kafkaTemplate.send(topic, key, data);
	}

	public String getTopicInfo(String topic) {

		return null;
	}

	public String getPartition(String topic, long offset, long limit) {

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
