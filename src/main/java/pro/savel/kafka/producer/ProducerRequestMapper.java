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

package pro.savel.kafka.producer;

import pro.savel.kafka.producer.requests.ProducerSendRequest;
import pro.savel.kafka.producer.requests.ProducerSendStringRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ProducerRequestMapper {

    public static ProducerSendRequest mapProduceRequest(ProducerSendStringRequest stringRequest) {

        var request = new ProducerSendRequest();
        request.setProducerId(stringRequest.getProducerId());
        request.setToken(stringRequest.getToken());
        request.setTopic(stringRequest.getTopic());
        request.setPartition(stringRequest.getPartition());

        var headersSource = stringRequest.getHeaders();
        if (headersSource != null) {
            var headers = new ArrayList<ProducerSendRequest.Header>(headersSource.size());
            headersSource.forEach((key, value) ->
                    headers.add(new ProducerSendRequest.Header(key, value != null ? value.getBytes(StandardCharsets.UTF_8) : null)));
            request.setHeaders(headers);
        }

        var keySource = stringRequest.getKey();
        if (keySource != null)
            request.setKey(keySource.getBytes(StandardCharsets.UTF_8));

        var valueSource = stringRequest.getValue();
        if (valueSource != null)
            request.setValue(valueSource.getBytes(StandardCharsets.UTF_8));

        return request;
    }
}
