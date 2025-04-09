// Copyright 2025 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import pro.savel.kafka.common.ClientWrapper;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.UnauthenticatedException;
import pro.savel.kafka.common.exceptions.UnauthorizedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class ConsumerWrapper extends ClientWrapper {

    private final KafkaConsumer<byte[], byte[]> consumer;

//    private Map<String, Map<Integer, Long>> finalOffsets;

    protected ConsumerWrapper(String name, Map<String, String> config, int expirationTimeout) {
        super(name, config, expirationTimeout);
        var properties = getProperties(config);
        consumer = new KafkaConsumer<>(properties);
    }

    private static Properties getProperties(Map<String, String> config) {
        var properties = new Properties(config.size());
        config.forEach(properties::setProperty);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }

    public Collection<TopicPartition> assignment() {
        return consumer.assignment();
    }

    public void assign(Collection<TopicPartition> assignment) throws BadRequestException, UnauthenticatedException, UnauthorizedException {
        try {
            consumer.assign(assignment);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new BadRequestException("Unable to assign consumer.", e);
        }
//        if (enablePartitionEof) {
//            Map<TopicPartition, Long> endOffsets;
//            try {
//                endOffsets = consumer.endOffsets(assignment);
//            } catch (AuthenticationException e) {
//                throw new UnauthenticatedException("Unable to get end offsets.", e);
//            } catch (AuthorizationException e) {
//                throw new UnauthorizedException("Unable to get end offsets.", e);
//            }
//            finalOffsets = new HashMap<>();
//            endOffsets.forEach((topicPartition, endOffset) -> {
//                var partitionOffsets = finalOffsets.computeIfAbsent(topicPartition.topic(), k -> new HashMap<>());
//                partitionOffsets.put(topicPartition.partition(), endOffset);
//            });
//        }
//        else {
//            finalOffsets = null;
//        }
    }

    public long position(TopicPartition topicPartition) throws BadRequestException, UnauthenticatedException, UnauthorizedException {
        try {
            return consumer.position(topicPartition);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new BadRequestException("Unable to get position.", e);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to get position.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to get position.", e);
        }
    }

    public void seek(TopicPartition partition, long offset) throws BadRequestException {
        try {
            consumer.seek(partition, offset);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new BadRequestException("Unable to set position.", e);
        }
//        checkFinalOffsets(partition.topic(), partition.partition(), offset);
    }

    public Collection<String> subscription() {
        return consumer.subscription();
    }

    public void subscribe(SubscriptionPattern pattern) throws BadRequestException {
        try {
            consumer.subscribe(pattern);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new BadRequestException("Unable to subscribe.", e);
        }
    }

    public void subscribe(Collection<String> topics) throws BadRequestException {
        try {
            consumer.subscribe(topics);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new BadRequestException("Unable to subscribe.", e);
        }
    }

    public Collection<PartitionInfo> partitionsFor(String topic) throws UnauthenticatedException, UnauthorizedException {
        try {
            return consumer.partitionsFor(topic);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to get partitions.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to get partitions.", e);
        }
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) throws UnauthenticatedException, UnauthorizedException {
        try {
            return consumer.beginningOffsets(partitions);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to get beginning offsets.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to get beginning offsets.", e);
        }
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) throws UnauthenticatedException, UnauthorizedException {
        try {
            return consumer.endOffsets(partitions);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to get end offsets.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to get end offsets.", e);
        }
    }

    public Collection<ConsumerRecord<byte[], byte[]>> poll(long timeout) throws BadRequestException, UnauthenticatedException, UnauthorizedException {
        Collection<ConsumerRecord<byte[], byte[]>> result;
//        if (finalOffsets != null && finalOffsets.isEmpty()) {
//            return Collections.emptyList();
//        }
        ConsumerRecords<byte[], byte[]> records;
        try {
            records = consumer.poll(Duration.ofMillis(timeout));
        } catch (InvalidTopicException | InvalidOffsetException | IllegalArgumentException | IllegalStateException |
                 ArithmeticException e) {
            throw new BadRequestException("Unable to poll records.", e);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to poll records.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to poll records.", e);
        }
        result = new ArrayList<>(records.count());
        records.forEach(record -> {
            result.add(record);
//            checkFinalOffsets(record.topic(), record.partition(), record.offset() + 1);
        });
        return result;
    }

//    private void checkFinalOffsets(String topic, Integer partition, long offset) {
//        if (finalOffsets != null) {
//            var partitionOffsets = finalOffsets.get(topic);
//            if (partitionOffsets != null) {
//                var finalOffset = partitionOffsets.get(partition);
//                if (finalOffset != null && finalOffset >= offset) {
//                    partitionOffsets.remove(partition);
//                    if (partitionOffsets.isEmpty())
//                        finalOffsets.remove(topic);
//                }
//            }
//        }
//    }

    public void commit(OffsetCommitCallback callback) {
        consumer.commitAsync(callback);
    }
}
