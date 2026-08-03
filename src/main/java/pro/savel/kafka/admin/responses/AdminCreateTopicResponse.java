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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

@Getter
public class AdminCreateTopicResponse implements AdminResponse {

    protected String topicId;
    protected Integer numPartitions;
    protected Integer replicationFactor;
    protected AdminConfigResponse config;

    protected AdminCreateTopicResponse() {
    }

    public static AdminCreateTopicResponse of(KafkaFuture<Uuid> idFuture, KafkaFuture<Integer> numPartitionsFuture, KafkaFuture<Integer> replicationFactorFuture, KafkaFuture<Config> configFuture) {
        var result = new AdminCreateTopicResponse();
        var topicId = get(idFuture);
        if (topicId != null)
            result.topicId = topicId.toString();
        result.numPartitions = get(numPartitionsFuture);
        result.replicationFactor = get(replicationFactorFuture);
        result.config = AdminConfigResponse.of(get(configFuture));
        return result;
    }

    protected static <T> T get(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error while constructing create topic response", e);
        }
    }
}
