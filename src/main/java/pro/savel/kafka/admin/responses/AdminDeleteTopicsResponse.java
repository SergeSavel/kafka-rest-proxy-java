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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import pro.savel.kafka.admin.AdminResponseMapper;
import pro.savel.kafka.common.Utils;

import java.util.ArrayList;
import java.util.Map;

@Getter
public class AdminDeleteTopicsResponse extends ArrayList<AdminDeleteTopicsResponse.TopicDeletionResult> implements AdminResponse {

    private AdminDeleteTopicsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminDeleteTopicsResponse ofUuids(Map<Uuid, KafkaFuture<Void>> source) {
        if (source == null)
            return null;
        var result = new AdminDeleteTopicsResponse(source.size());
        source.forEach((topicId, topicDeletionResult) -> result.add(TopicDeletionResult.ofUuid(topicId, topicDeletionResult)));
        return result;
    }

    public static AdminDeleteTopicsResponse ofNames(Map<String, KafkaFuture<Void>> source) {
        if (source == null)
            return null;
        var result = new AdminDeleteTopicsResponse(source.size());
        source.forEach((topicName, topicDeletionResult) -> result.add(TopicDeletionResult.ofName(topicName, topicDeletionResult)));
        return result;
    }

    @Getter
    public static class TopicDeletionResult {

        private String topicId = null;
        private String topicName = null;
        private boolean success = true;
        private String errorMessage;

        private static TopicDeletionResult ofUuid(Uuid topicId, KafkaFuture<Void> topicDeletionResult) {
            if (topicDeletionResult == null)
                return null;
            var result = new TopicDeletionResult();
            result.topicId = AdminResponseMapper.mapUuid(topicId);
            try {
                var ignore = topicDeletionResult.get();
            } catch (Exception e) {
                result.success = false;
                result.errorMessage = Utils.rootErrorMessage(e);
            }
            return result;
        }

        private static TopicDeletionResult ofName(String topicName, KafkaFuture<Void> topicDeletionResult) {
            if (topicDeletionResult == null)
                return null;
            var result = new TopicDeletionResult();
            result.topicName = topicName;
            try {
                var ignore = topicDeletionResult.get();
            } catch (Exception e) {
                result.success = false;
                result.errorMessage = Utils.rootErrorMessage(e);
            }
            return result;
        }
    }
}
