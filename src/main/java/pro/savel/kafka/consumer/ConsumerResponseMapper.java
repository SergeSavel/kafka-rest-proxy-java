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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import pro.savel.kafka.common.CommonMapper;
import pro.savel.kafka.common.contract.PartitionInfo;
import pro.savel.kafka.consumer.responses.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ConsumerResponseMapper {

    public static ConsumerListResponse mapListResponse(Collection<ConsumerWrapper> source) {
        if (source == null)
            return null;
        var result = new ConsumerListResponse(source.size());
        source.forEach(wrapper -> result.add(mapConsumer(wrapper)));
        return result;
    }

    private static ConsumerListResponse.Consumer mapConsumer(ConsumerWrapper source) {
        if (source == null)
            return null;
        var result = new ConsumerListResponse.Consumer();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        return result;
    }

    public static ConsumerCreateResponse mapCreateResponse(ConsumerWrapper source) {
        if (source == null)
            return null;
        var result = new ConsumerCreateResponse();
        result.setId(source.getId());
        result.setToken(source.getToken());
        return result;
    }

    public static ConsumerPollResponse mapPollResponse(ConsumerRecords<byte[], byte[]> source) {
        if (source == null)
            return null;
        var result = new ConsumerPollResponse(source.count());
        source.forEach(record -> result.add(mapMessage(record)));
        return result;
    }

    public static ConsumerMessage mapMessage(ConsumerRecord<byte[], byte[]> source) {
        if (source == null)
            return null;
        var result = new ConsumerMessage();
        result.setTimestamp(source.timestamp());
        result.setTopic(source.topic());
        result.setPartition(source.partition());
        result.setOffset(source.offset());
        result.setHeaders(mapHeaders(source.headers()));
        result.setKey(source.key());
        result.setValue(source.value());
        return result;
    }

    public static Collection<ConsumerMessage.Header> mapHeaders(Headers source) {
        if (source == null)
            return null;
        var result = new ArrayList<ConsumerMessage.Header>();
        source.forEach(header -> result.add(mapHeader(header)));
        return result;
    }

    public static ConsumerMessage.Header mapHeader(Header source) {
        if (source == null)
            return null;
        var result = new ConsumerMessage.Header();
        result.setKey(source.key());
        result.setValue(source.value());
        return result;
    }

    public static ConsumerAssignmentResponse mapAssignmentResponse(Collection<org.apache.kafka.common.TopicPartition> source) {
        if (source == null)
            return null;
        var result = new ConsumerAssignmentResponse(source.size());
        source.forEach(partition -> result.add(mapTopicPartition(partition)));
        return result;
    }

    public static TopicPartition mapTopicPartition(org.apache.kafka.common.TopicPartition source) {
        if (source == null)
            return null;
        return new TopicPartition(source.topic(), source.partition());
    }

    public static ConsumerPositionResponse mapPositionResponse(long source) {
        var result = new ConsumerPositionResponse();
        result.setOffset(source);
        return result;
    }

    public static ConsumerSubscriptionResponse mapSubscriptionResponse(Collection<String> source) {
        if (source == null)
            return null;
        return new ConsumerSubscriptionResponse(source);
    }

    public static ConsumerPartitionsResponse mapPartitionsResponse(Collection<org.apache.kafka.common.PartitionInfo> source) {
        if (source == null)
            return null;
        var result = new ConsumerPartitionsResponse(source.size());
        source.forEach(partitionInfo -> result.add(CommonMapper.mapPartitionInfo(partitionInfo)));
        return result;
    }

    public static ConsumerOffsetsResponse mapOffsetsResponse(Map<org.apache.kafka.common.TopicPartition, Long> source) {
        if (source == null)
            return null;
        var result = new ConsumerOffsetsResponse();
        source.forEach((topicPartition, offset) -> {
            var offsets = result.computeIfAbsent(topicPartition.topic(), k -> new ArrayList<>());
            offsets.add(new PartitionOffset(topicPartition.partition(), offset));
        });
        return result;
    }

    public static ConsumerTopicsResponse mapTopicsResponse(Map<String, List<org.apache.kafka.common.PartitionInfo>> source) {
        if (source == null)
            return null;
        var result = new ConsumerTopicsResponse(source.size());
        source.forEach((topic, partitionsSource) -> {
            var partitions = new ArrayList<PartitionInfo>(partitionsSource.size());
            partitionsSource.forEach(partitionSource -> partitions.add(CommonMapper.mapPartitionInfo(partitionSource)));
            result.put(topic, partitions);
        });
        return result;
    }
}
