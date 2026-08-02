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
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class AdminDescribeProducersResponse extends ArrayList<AdminDescribeProducersResponse.PartitionProducerState>
        implements AdminResponse {

    private AdminDescribeProducersResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminDescribeProducersResponse of(
            Map<TopicPartition, DescribeProducersResult.PartitionProducerState> source) {
        if (source == null)
            return null;
        var result = new AdminDescribeProducersResponse(source.size());
        source.forEach((topicPartition, partitionProducerState) -> result
                .add(PartitionProducerState.of(topicPartition, partitionProducerState)));
        return result;
    }

    @Getter
    public static class PartitionProducerState {

        private String topic;
        private int partition;
        private Collection<ProducerState> activeProducers;

        private PartitionProducerState() {
        }

        private static PartitionProducerState of(
                TopicPartition topicPartition,
                DescribeProducersResult.PartitionProducerState source) {
            if (topicPartition == null || source == null)
                return null;
            var result = new PartitionProducerState();
            result.topic = topicPartition.topic();
            result.partition = topicPartition.partition();
            result.activeProducers = ProducerState.of(source.activeProducers());
            return result;
        }
    }

    @Getter
    public static class ProducerState {

        private long producerId;
        private int producerEpoch;
        private int lastSequence;
        private long lastTimestamp;
        private Long currentTransactionStartOffset;
        private Integer coordinatorEpoch;

        private ProducerState() {
        }

        private static ProducerState of(org.apache.kafka.clients.admin.ProducerState source) {
            if (source == null)
                return null;
            var result = new ProducerState();
            result.producerId = source.producerId();
            result.producerEpoch = source.producerEpoch();
            result.lastSequence = source.lastSequence();
            result.lastTimestamp = source.lastTimestamp();
            result.currentTransactionStartOffset = source.currentTransactionStartOffset()
                    .isPresent() ? source.currentTransactionStartOffset().getAsLong() : null;
            result.coordinatorEpoch = source.coordinatorEpoch()
                    .isPresent() ? source.coordinatorEpoch().getAsInt() : null;
            return result;
        }

        private static ArrayList<ProducerState> of(
                Collection<org.apache.kafka.clients.admin.ProducerState> source) {
            if (source == null)
                return null;
            var result = new ArrayList<ProducerState>(source.size());
            source.forEach(sourceItem -> result.add(of(sourceItem)));
            return result;
        }
    }
}
