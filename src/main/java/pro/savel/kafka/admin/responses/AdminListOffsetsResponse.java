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

package pro.savel.kafka.admin.responses;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Map;

public class AdminListOffsetsResponse extends ArrayList<AdminListOffsetsResponse.OffsetListing>
        implements AdminResponse {

    private AdminListOffsetsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminListOffsetsResponse of(
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> source) {
        if (source == null)
            return null;
        var result = new AdminListOffsetsResponse(source.size());
        source.forEach((topicPartition, info) -> result.add(OffsetListing.of(topicPartition, info)));
        return result;
    }

    @Getter
    public static class OffsetListing {

        private String topic;
        private int partition;
        private long offset;
        private long timestamp;
        private Integer leaderEpoch;

        private OffsetListing() {
        }

        private static OffsetListing of(org.apache.kafka.common.TopicPartition topicPartition,
                org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo info) {
            if (topicPartition == null || info == null)
                return null;
            var result = new OffsetListing();
            result.topic = topicPartition.topic();
            result.partition = topicPartition.partition();
            result.offset = info.offset();
            result.timestamp = info.timestamp();
            result.leaderEpoch = info.leaderEpoch().orElse(null);
            return result;
        }
    }
}
