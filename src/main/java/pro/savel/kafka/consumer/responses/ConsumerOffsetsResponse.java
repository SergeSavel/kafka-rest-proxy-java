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
import java.util.HashMap;
import java.util.Map;

public class ConsumerOffsetsResponse extends ArrayList<ConsumerOffsetsResponse.TopicOffsets> implements ConsumerResponse {

    private ConsumerOffsetsResponse(int size) {
        super(size);
    }

    public static ConsumerOffsetsResponse of(Map<org.apache.kafka.common.TopicPartition, Long> source) {
        if (source == null)
            return null;
        var map = new HashMap<String, TopicOffsets>();
        source.forEach((topicPartition, offset) -> {
            var topicOffsets = map.computeIfAbsent(topicPartition.topic(), TopicOffsets::new);
            topicOffsets.offsets.add(new TopicOffsets.PartitionOffset(topicPartition.partition(), offset));
        });
        var result = new ConsumerOffsetsResponse(map.size());
        result.addAll(map.values());
        return result;
    }

    @Getter
    public static class TopicOffsets {

        private final String topic;
        private final ArrayList<PartitionOffset> offsets;

        private TopicOffsets(String topic) {
            this.topic = topic;
            this.offsets = new ArrayList<>();
        }

        public record PartitionOffset(int partition, Long offset) {
        }
    }
}
