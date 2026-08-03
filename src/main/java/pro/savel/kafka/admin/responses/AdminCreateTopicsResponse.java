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
import pro.savel.kafka.common.Utils;

import java.util.ArrayList;

public class AdminCreateTopicsResponse extends ArrayList<AdminCreateTopicResponse> implements AdminResponse {

    private AdminCreateTopicsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    @Getter
    public static class CreationResult extends AdminCreateTopicResponse {

        private boolean success = true;
        private String errorMessage = null;
        private String topicName;

        private CreationResult() {
            super();
        }

        private static CreationResult of(String topicName, KafkaFuture<Void> statusFuture, KafkaFuture<Uuid> idFuture, KafkaFuture<Integer> numPartitionsFuture, KafkaFuture<Integer> replicationFactorFuture, KafkaFuture<Config> configFuture) {
            var result = new CreationResult();
            result.topicName = topicName;
            try {
                statusFuture.get();
            } catch (Exception e) {
                result.success = false;
                result.errorMessage = Utils.combineErrorMessage(e);
                return result;
            }
            var topicId = get(idFuture);
            if (topicId != null)
                result.topicId = topicId.toString();
            result.numPartitions = get(numPartitionsFuture);
            result.replicationFactor = get(replicationFactorFuture);
            result.config = AdminConfigResponse.of(get(configFuture));
            return result;
        }
    }

    public static AdminCreateTopicsResponse of(org.apache.kafka.clients.admin.CreateTopicsResult source) {
        if (source == null)
            return null;
        var result = new AdminCreateTopicsResponse(source.values().size());
        source.values().forEach((topicName, statusFuture) -> result.add(CreationResult.of(
                topicName,
                statusFuture,
                source.topicId(topicName),
                source.numPartitions(topicName),
                source.replicationFactor(topicName),
                source.config(topicName)
        )));
        return result;
    }
}
