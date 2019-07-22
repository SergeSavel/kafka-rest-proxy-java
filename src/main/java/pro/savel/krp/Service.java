package pro.savel.krp;

import org.springframework.kafka.core.KafkaTemplate;

@org.springframework.stereotype.Service
public class Service {

	private final KafkaTemplate kafkaTemplate;

	public Service(KafkaTemplate kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void postTopic(String topic, String key, byte[] data) {

		kafkaTemplate.send(topic, key, data);
	}
}
