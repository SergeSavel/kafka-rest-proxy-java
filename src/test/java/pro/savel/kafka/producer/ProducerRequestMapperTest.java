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

package pro.savel.kafka.producer;

import org.junit.jupiter.api.Test;
import pro.savel.kafka.producer.requests.ProducerSendStringRequest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerRequestMapperTest {

    @Test
    void mapProduceRequest_basicFields_mapped() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("prod-1");
        source.setToken("token-1");
        source.setTopic("test-topic");
        source.setPartition(3);

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertEquals("prod-1", result.getProducerId());
        assertEquals("token-1", result.getToken());
        assertEquals("test-topic", result.getTopic());
        assertEquals(3, result.getPartition());
    }

    @Test
    void mapProduceRequest_keyAndValue_convertedToBytes() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("p");
        source.setToken("t");
        source.setTopic("topic");
        source.setKey("my-key");
        source.setValue("my-value");

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertArrayEquals("my-key".getBytes(StandardCharsets.UTF_8), result.getKey());
        assertArrayEquals("my-value".getBytes(StandardCharsets.UTF_8), result.getValue());
    }

    @Test
    void mapProduceRequest_nullKeyAndValue_remainNull() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("p");
        source.setToken("t");
        source.setTopic("topic");

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertNull(result.getKey());
        assertNull(result.getValue());
    }

    @Test
    void mapProduceRequest_headers_convertedToBytes() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("p");
        source.setToken("t");
        source.setTopic("topic");
        source.setHeaders(Map.of("h1", "v1", "h2", "v2"));

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertNotNull(result.getHeaders());
        assertEquals(2, result.getHeaders().size());
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), result.getHeaders().get("h1"));
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), result.getHeaders().get("h2"));
    }

    @Test
    void mapProduceRequest_nullHeaders_remainNull() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("p");
        source.setToken("t");
        source.setTopic("topic");

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertNull(result.getHeaders());
    }

    @Test
    void mapProduceRequest_nullHeaderValue_mappedAsNull() {
        var source = new ProducerSendStringRequest();
        source.setProducerId("p");
        source.setToken("t");
        source.setTopic("topic");
        source.setHeaders(new java.util.HashMap<>(Map.of("h1", "v1")));
        source.getHeaders().put("h2", null);

        var result = ProducerRequestMapper.mapProduceRequest(source);

        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), result.getHeaders().get("h1"));
        assertNull(result.getHeaders().get("h2"));
    }
}
