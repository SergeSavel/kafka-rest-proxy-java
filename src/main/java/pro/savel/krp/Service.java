// Copyright 2019-2020 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.krp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.TopicInfo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class Service {

	@Autowired
	private Environment env;

	private Map<String, Object> producerProps;
	private Map<String, Object> consumerProps;
	private Producer<String, String> producer = null;

	private Deserializer<String> keyDeserializer, valueDeserializer;

	@PostConstruct
	public void init() {

		this.producerProps = createProducerProps();
		this.consumerProps = createConsumerProps();
		this.producer = new KafkaProducer<>(this.producerProps);

		keyDeserializer = new StringDeserializer();
		valueDeserializer = new StringDeserializer();
	}

	@PreDestroy
	public void done() {
		if (producer != null)
			producer.close();
	}

	public Map<String, Object> createProducerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				env.getRequiredProperty("spring.kafka.bootstrap-servers"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		setIfSet(props, ProducerConfig.CLIENT_ID_CONFIG,
				"spring.kafka.consumer.client-id");
		setIfSet(props, ProducerConfig.ACKS_CONFIG,
				"spring.kafka.producer.acks");
		setIfSet(props, ProducerConfig.COMPRESSION_TYPE_CONFIG,
				"spring.kafka.producer.compression-type");
		setIfSet(props, ProducerConfig.LINGER_MS_CONFIG,
				"spring.kafka.producer.properties.linger.ms");
		setIfSet(props, ProducerConfig.BUFFER_MEMORY_CONFIG,
				"spring.kafka.producer.properties.buffer.memory");
		setIfSet(props, ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
				"spring.kafka.producer.properties.max.request.size");
		setIfSet(props, "security.protocol",
				"spring.kafka.properties.security.protocol");
		setIfSet(props, "sasl.mechanism",
				"spring.kafka.properties.sasl.mechanism");
		return props;
	}

	public Map<String, Object> createConsumerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				env.getRequiredProperty("spring.kafka.bootstrap-servers"));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				"false");
		setIfSet(props, ConsumerConfig.CLIENT_ID_CONFIG,
				"spring.kafka.consumer.client-id");
		setIfSet(props, "security.protocol",
				"spring.kafka.properties.security.protocol");
		setIfSet(props, "sasl.mechanism",
				"spring.kafka.properties.sasl.mechanism");
		return props;
	}

	private void setIfSet(Map<String, Object> props, String propName, String envPropName) {
		if (env.containsProperty(envPropName))
			props.put(propName, env.getProperty(envPropName));
	}

	public void postData(String topic, Message message) {

		ProducerRecord<String, String> producerRecord = createProducerRecord(topic, message);

		Future<RecordMetadata> future = producer.send(producerRecord);

		RecordMetadata recordMetadata;
		try {
			recordMetadata = future.get();
		} catch (Exception e) {
			throw new RuntimeException("Unable to send message: " + e.getMessage(), e);
		}
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

	public TopicInfo getTopicInfo(String topic) {
		TopicInfo topicInfo;
		try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps, keyDeserializer, valueDeserializer)) {
			topicInfo = createTopicInfo(topic, consumer);
		}
		return topicInfo;
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
