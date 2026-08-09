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
import pro.savel.kafka.admin.AdminResponseMapper;
import pro.savel.kafka.common.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

        private static CreationResult of(String topicName, KafkaFuture<Void> statusFuture, KafkaFuture<Uuid> idFuture, KafkaFuture<Integer> numPartitionsFuture, KafkaFuture<Integer> replicationFactorFuture) throws ExecutionException, InterruptedException {
            var result = new CreationResult();
            result.topicName = topicName;
            try {
                statusFuture.get();
            } catch (Exception e) {
                result.success = false;
                result.errorMessage = Utils.rootErrorMessage(e);
                return result;
            }
            result.topicId = AdminResponseMapper.mapUuid(idFuture.get());
            result.numPartitions = numPartitionsFuture.get();
            result.replicationFactor = replicationFactorFuture.get();
            return result;
        }
    }

    public static AdminCreateTopicsResponse of(org.apache.kafka.clients.admin.CreateTopicsResult source) throws ExecutionException, InterruptedException {
        if (source == null)
            return null;
        var result = new AdminCreateTopicsResponse(source.values().size());
        for (Map.Entry<String, KafkaFuture<Void>> entry : source.values().entrySet()) {
            var topicName = entry.getKey();
            var statusFuture = entry.getValue();
            result.add(CreationResult.of(
                    topicName,
                    statusFuture,
                    source.topicId(topicName),
                    source.numPartitions(topicName),
                    source.replicationFactor(topicName)
            ));
        }
        return result;
    }
}
