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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.producer.responses.ProducerResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProducerResponseSerializerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static ProducerSendResponse createSendResponse(String topic, int partition, long offset,
                                                           long timestamp, int keySize, int valueSize)
    {
        var metadata = new RecordMetadata(new TopicPartition(topic, partition), offset, 0,
                timestamp, keySize, valueSize);
        return ProducerSendResponse.of(metadata);
    }

//region JSON

    @Test
    void serializeJson_null_returnsNull() throws Exception {
        assertNull(ProducerResponseSerializer.serializeJson(objectMapper, null));
    }

    @Test
    void serializeJson_sendResponse_returnsValidJson() throws Exception {
        var response = createSendResponse("test-topic", 2, 100L, 1234567890L, 10, 200);

        var buf = ProducerResponseSerializer.serializeJson(objectMapper, response);
        assertNotNull(buf);

        var json = buf.toString(StandardCharsets.UTF_8);
        assertTrue(json.contains("\"topic\":\"test-topic\""));
        assertTrue(json.contains("\"partition\":2"));
        assertTrue(json.contains("\"offset\":100"));
        buf.release();
    }

//endregion

//region Binary

    @Test
    void serializeBinary_null_returnsNull() {
        assertNull(ProducerResponseSerializer.serializeBinary(null));
    }

    @Test
    void serializeBinary_sendResponse_returnsValidBinary() {
        var response = createSendResponse("my-topic", 5, 42L, 9999L, 8, 64);

        var buf = ProducerResponseSerializer.serializeBinary(response);
        assertNotNull(buf);

        assertEquals(1, buf.readShort());  // version
        var topicLen = buf.readInt();
        assertEquals("my-topic".length(), topicLen);
        var topicBytes = new byte[topicLen];
        buf.readBytes(topicBytes);
        assertEquals("my-topic", new String(topicBytes, StandardCharsets.UTF_8));
        assertEquals(5, buf.readInt());     // partition
        assertEquals(42L, buf.readLong());  // offset
        assertEquals(9999L, buf.readLong()); // timestamp
        assertEquals(8, buf.readInt());      // serializedKeySize
        assertEquals(64, buf.readInt());     // serializedValueSize
        buf.release();
    }

    @Test
    void serializeBinary_unsupportedType_throwsException() {
        ProducerResponse unsupported = new ProducerResponse() {};
        assertThrows(IllegalArgumentException.class,
                () -> ProducerResponseSerializer.serializeBinary(unsupported));
    }

//endregion
}
