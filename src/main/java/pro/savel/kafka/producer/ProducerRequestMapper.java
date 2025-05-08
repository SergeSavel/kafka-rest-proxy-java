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
import java.util.HashMap;

public class ProducerRequestMapper {

    public static ProducerSendRequest mapProduceRequest(ProducerSendStringRequest stringRequest) {
        var headers = new HashMap<String, byte[]>(stringRequest.getHeaders().size());
        stringRequest.getHeaders().forEach((key, value) -> headers.put(key, value.getBytes(StandardCharsets.UTF_8)));
        var request = new ProducerSendRequest();
        request.setProducerId(stringRequest.getProducerId());
        request.setToken(stringRequest.getToken());
        request.setTopic(stringRequest.getTopic());
        request.setPartition(stringRequest.getPartition());
        request.setHeaders(headers);
        request.setKey(stringRequest.getKey().getBytes(StandardCharsets.UTF_8));
        request.setValue(stringRequest.getValue().getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
