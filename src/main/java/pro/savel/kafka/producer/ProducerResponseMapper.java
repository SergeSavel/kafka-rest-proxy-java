// Copyright 2025 Sergey Savelev
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

package pro.savel.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import pro.savel.kafka.producer.responses.ProducerCreateResponse;
import pro.savel.kafka.producer.responses.ProducerListResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.util.Collection;

public abstract class ProducerResponseMapper {

    public static ProducerListResponse mapListResponse(Collection<ProducerWrapper> source) {
        if (source == null)
            return null;
        var result = new ProducerListResponse(source.size());
        source.forEach(wrapper -> result.add(mapProducer(wrapper)));
        return result;
    }

    private static ProducerListResponse.Producer mapProducer(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new ProducerListResponse.Producer();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        return result;
    }

    public static ProducerCreateResponse mapCreateResponse(ProducerWrapper source) {
        if (source == null)
            return null;
        var result = new ProducerCreateResponse();
        result.setId(source.getId());
        result.setToken(source.getToken());
        return result;
    }

    public static ProducerSendResponse mapSendResponse(RecordMetadata source) {
        var result = new ProducerSendResponse();
        result.setTopic(source.topic());
        result.setPartition(source.partition());
        result.setOffset(source.offset());
        result.setTimestamp(source.timestamp());
        return result;
    }
}
