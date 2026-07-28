// Copyright 2025 Sergey Savelev (serge@savel.pro)
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
import pro.savel.kafka.consumer.responses.*;

import java.util.*;

public class ConsumerResponseMapper {

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
        var result = new ConsumerPartitionsResponse();
        result.setPartitions(new ArrayList<>(source.size()));
        source.forEach(partitionInfoSource -> {
            result.setTopic(partitionInfoSource.topic());
            var partitionInfo = new ConsumerPartitionsResponse.PartitionInfo();
            partitionInfo.setPartition(partitionInfoSource.partition());
            result.getPartitions().add(partitionInfo);
        });
        return result;
    }

    public static ConsumerOffsetsResponse mapOffsetsResponse(Map<org.apache.kafka.common.TopicPartition, Long> source) {
        if (source == null)
            return null;
        var map = new HashMap<String, ConsumerOffsetsResponse.TopicOffsets>();
        source.forEach((topicPartition, offset) -> {
            var topicOffsets = map.computeIfAbsent(topicPartition.topic(), ConsumerOffsetsResponse.TopicOffsets::new);
            topicOffsets.getOffsets().add(new PartitionOffset(topicPartition.partition(), offset));
        });
        return new ConsumerOffsetsResponse(map.values());
    }

    public static ConsumerTopicsResponse mapTopicsResponse(Map<String, List<org.apache.kafka.common.PartitionInfo>> source) {
        if (source == null)
            return null;
        var result = new ConsumerTopicsResponse(source.size());
        source.forEach((topic, partitionsSource) -> {
            var topicInfo = new ConsumerTopicsResponse.TopicInfo();
            topicInfo.setTopic(topic);
            topicInfo.setPartitions(new ArrayList<>(partitionsSource.size()));
            partitionsSource.forEach(partitionSource -> {
                var partitionInfo = new ConsumerTopicsResponse.PartitionInfo();
                partitionInfo.setPartition(partitionSource.partition());
                topicInfo.getPartitions().add(partitionInfo);
            });
            result.add(topicInfo);
        });
        return result;
    }
}
