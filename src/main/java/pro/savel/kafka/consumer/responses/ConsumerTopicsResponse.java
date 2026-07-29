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

package pro.savel.kafka.consumer.responses;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ConsumerTopicsResponse extends ArrayList<ConsumerTopicsResponse.TopicInfo> implements ConsumerResponse {

    private ConsumerTopicsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static ConsumerTopicsResponse of(Map<String, List<org.apache.kafka.common.PartitionInfo>> source) {
        if (source == null)
            return null;
        var result = new ConsumerTopicsResponse(source.size());
        source.forEach((topic, partitionsSource) -> {
            var topicInfo = new TopicInfo();
            topicInfo.topic = topic;
            topicInfo.partitions = new ArrayList<>(partitionsSource.size());
            partitionsSource.forEach(partitionSource -> topicInfo.partitions.add(PartitionInfo.of(partitionSource)));
            result.add(topicInfo);
        });
        return result;
    }

    @Getter
    public static class PartitionInfo {

        private int partition;

        private PartitionInfo() {
        }

        private static PartitionInfo of(org.apache.kafka.common.PartitionInfo source) {
            if (source == null)
                return null;
            var result = new PartitionInfo();
            result.partition = source.partition();
            return result;
        }
    }

    @Getter
    public static class TopicInfo {

        private String topic;
        private Collection<PartitionInfo> partitions;

        private TopicInfo() {
        }
    }
}