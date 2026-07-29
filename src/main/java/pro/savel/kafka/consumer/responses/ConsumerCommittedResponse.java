// Copyright 2026 Sergey Savelev (serge@savel.pro)
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConsumerCommittedResponse extends ArrayList<ConsumerCommittedResponse.TopicOffsets> implements ConsumerResponse {

    private ConsumerCommittedResponse(int size) {
        super(size);
    }

    public static ConsumerCommittedResponse of(Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> source) {
        if (source == null)
            return null;
        var map = new HashMap<String, TopicOffsets>();
        source.forEach((topicPartition, offsetAndMetadata) -> {
            var topicOffsets = map.computeIfAbsent(topicPartition.topic(), TopicOffsets::new);
            topicOffsets.offsets.add(PartitionOffset.of(topicPartition.partition(), offsetAndMetadata));
        });
        var result = new ConsumerCommittedResponse(map.size());
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
    }

    @Getter
    public static class PartitionOffset {

        private int partition;
        private Long offset;
        private String metadata;

        private PartitionOffset() {
        }

        private static PartitionOffset of(int partition, OffsetAndMetadata source) {
            var result = new PartitionOffset();
            result.partition = partition;
            if (source != null) {
                result.offset = source.offset();
                result.metadata = source.metadata();
            }
            return result;
        }
    }
}
