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
import org.junit.jupiter.api.Test;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProducerResponseSerializerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

//region JSON

    @Test
    void serializeJson_null_returnsNull() throws Exception {
        assertNull(ProducerResponseSerializer.serializeJson(objectMapper, null));
    }

    @Test
    void serializeJson_sendResponse_returnsValidJson() throws Exception {
        var response = new ProducerSendResponse();
        response.setTopic("test-topic");
        response.setPartition(2);
        response.setOffset(100L);
        response.setTimestamp(1234567890L);
        response.setSerializedKeySize(10);
        response.setSerializedValueSize(200);

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
        var response = new ProducerSendResponse();
        response.setTopic("my-topic");
        response.setPartition(5);
        response.setOffset(42L);
        response.setTimestamp(9999L);
        response.setSerializedKeySize(8);
        response.setSerializedValueSize(64);

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
        assertThrows(IllegalArgumentException.class,
                () -> ProducerResponseSerializer.serializeBinary(new ProducerSendResponse() {
                    // anonymous subclass — not ProducerSendResponse.class
                }));
    }

//endregion
}
