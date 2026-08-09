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

import java.util.concurrent.ExecutionException;

@Getter
public class AdminCreateTopicResponse implements AdminResponse {

    protected String topicId;
    protected Integer numPartitions;
    protected Integer replicationFactor;

    protected AdminCreateTopicResponse() {
    }

    public static AdminCreateTopicResponse of(KafkaFuture<Uuid> idFuture, KafkaFuture<Integer> numPartitionsFuture, KafkaFuture<Integer> replicationFactorFuture) throws ExecutionException, InterruptedException {
        var result = new AdminCreateTopicResponse();
        result.topicId = AdminResponseMapper.mapUuid(idFuture.get());
        result.numPartitions = numPartitionsFuture.get();
        result.replicationFactor = replicationFactorFuture.get();
        return result;
    }
}
