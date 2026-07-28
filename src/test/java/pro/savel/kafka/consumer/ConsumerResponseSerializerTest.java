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

package pro.savel.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.consumer.responses.ConsumerMessage;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerResponseSerializerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

//region JSON

    @Test
    void serializeJson_null_returnsNull() throws Exception {
        assertNull(ConsumerResponseSerializer.serializeJson(objectMapper, null));
    }

    @Test
    void serializeJson_emptyPollResponse_returnsEmptyArray() throws Exception {
        var response = new ConsumerPollResponse();
        var buf = ConsumerResponseSerializer.serializeJson(objectMapper, response);
        assertNotNull(buf);
        assertEquals("[]", buf.toString(StandardCharsets.UTF_8));
        buf.release();
    }

    @Test
    void serializeJson_pollResponse_convertsBytesToString() throws Exception {
        var msg = new ConsumerMessage();
        msg.setTopic("test");
        msg.setPartition(0);
        msg.setOffset(1L);
        msg.setTimestamp(100L);
        msg.setKey("key".getBytes(StandardCharsets.UTF_8));
        msg.setValue("value".getBytes(StandardCharsets.UTF_8));
        msg.setHeaders(List.of());

        var response = new ConsumerPollResponse();
        response.add(msg);

        var buf = ConsumerResponseSerializer.serializeJson(objectMapper, response);
        assertNotNull(buf);
        var json = buf.toString(StandardCharsets.UTF_8);
        assertTrue(json.contains("\"key\":\"key\""));
        assertTrue(json.contains("\"value\":\"value\""));
        buf.release();
    }

//endregion

//region Binary

    @Test
    void serializeBinary_null_returnsNull() {
        assertNull(ConsumerResponseSerializer.serializeBinary(null));
    }

    @Test
    void serializeBinary_emptyPollResponse_returnsValidBinary() {
        var response = new ConsumerPollResponse();
        var buf = ConsumerResponseSerializer.serializeBinary(response);
        assertNotNull(buf);
        assertEquals(1, buf.readShort());  // version
        assertEquals(0, buf.readInt());    // size = 0
        buf.release();
    }

    @Test
    void serializeBinary_pollResponseWithMessage_returnsValidBinary() {
        var header = new ConsumerMessage.Header();
        header.setKey("h1");
        header.setValue("hv1".getBytes(StandardCharsets.UTF_8));

        var msg = new ConsumerMessage();
        msg.setTopic("my-topic");
        msg.setPartition(3);
        msg.setOffset(42L);
        msg.setTimestamp(999L);
        msg.setHeaders(List.of(header));
        msg.setKey("k".getBytes(StandardCharsets.UTF_8));
        msg.setValue("v".getBytes(StandardCharsets.UTF_8));

        var response = new ConsumerPollResponse();
        response.add(msg);

        var buf = ConsumerResponseSerializer.serializeBinary(response);
        assertNotNull(buf);

        assertEquals(1, buf.readShort());   // version
        assertEquals(1, buf.readInt());      // 1 message

        // topic
        var topicLen = buf.readInt();
        assertEquals("my-topic".length(), topicLen);
        var topicBytes = new byte[topicLen];
        buf.readBytes(topicBytes);
        assertEquals("my-topic", new String(topicBytes, StandardCharsets.UTF_8));

        assertEquals(3, buf.readInt());      // partition
        assertEquals(42L, buf.readLong());   // offset
        assertEquals(999L, buf.readLong());  // timestamp

        assertEquals(1, buf.readInt());      // 1 header
        // header key
        var hkLen = buf.readInt();
        var hkBytes = new byte[hkLen];
        buf.readBytes(hkBytes);
        assertEquals("h1", new String(hkBytes, StandardCharsets.UTF_8));
        assertEquals(0, buf.readByte());     // header value not null
        var hvLen = buf.readInt();
        var hvBytes = new byte[hvLen];
        buf.readBytes(hvBytes);
        assertEquals("hv1", new String(hvBytes, StandardCharsets.UTF_8));

        assertEquals(0, buf.readByte());     // key not null
        var kLen = buf.readInt();
        var kBytes = new byte[kLen];
        buf.readBytes(kBytes);
        assertEquals("k", new String(kBytes, StandardCharsets.UTF_8));

        assertEquals(0, buf.readByte());     // value not null
        var vLen = buf.readInt();
        var vBytes = new byte[vLen];
        buf.readBytes(vBytes);
        assertEquals("v", new String(vBytes, StandardCharsets.UTF_8));

        buf.release();
    }

    @Test
    void serializeBinary_nullKeyAndValue_writesNullMarkers() {
        var msg = new ConsumerMessage();
        msg.setTopic("t");
        msg.setPartition(0);
        msg.setOffset(0L);
        msg.setTimestamp(0L);
        msg.setHeaders(List.of());
        msg.setKey(null);
        msg.setValue(null);

        var response = new ConsumerPollResponse();
        response.add(msg);

        var buf = ConsumerResponseSerializer.serializeBinary(response);
        assertNotNull(buf);

        buf.readShort();   // version
        buf.readInt();      // size
        buf.readInt();      // topic len
        buf.skipBytes(1);   // topic
        buf.readInt();      // partition
        buf.readLong();     // offset
        buf.readLong();     // timestamp
        buf.readInt();      // headers count

        assertEquals(1, buf.readByte());     // key is null
        assertEquals(1, buf.readByte());     // value is null

        buf.release();
    }

    @Test
    void serializeBinary_unsupportedType_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> ConsumerResponseSerializer.serializeBinary(new ConsumerPollResponse() {}));
    }

//endregion
}
