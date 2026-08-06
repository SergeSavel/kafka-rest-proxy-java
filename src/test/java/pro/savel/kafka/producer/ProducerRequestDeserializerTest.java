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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerRequestDeserializerTest {

    @Test
    void validPayload_parsesAllFields() {
        var buf = validSendPayload("value");
        try {
            var request = ProducerRequestDeserializer.deserializeBinarySend(buf);
            assertEquals("producer-1", request.getProducerId());
            assertEquals("token-1", request.getToken());
            assertEquals("topic-1", request.getTopic());
            assertEquals(3, request.getPartition());
            assertArrayEquals("hv".getBytes(StandardCharsets.UTF_8), request.getHeaders().get("header-key"));
            assertArrayEquals("k".getBytes(StandardCharsets.UTF_8), request.getKey());
            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), request.getValue());
        } finally {
            buf.release();
        }
    }

    @Test
    void emptyPayload_throwsBadRequest() {
        var buf = Unpooled.EMPTY_BUFFER;
        assertThrows(BadRequestException.class, () -> ProducerRequestDeserializer.deserializeBinarySend(buf));
    }

    @Test
    void truncatedVersion_throwsBadRequest() {
        var buf = Unpooled.wrappedBuffer(new byte[]{0});
        try {
            assertThrows(BadRequestException.class, () -> ProducerRequestDeserializer.deserializeBinarySend(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    void truncatedString_throwsBadRequest() {
        var buf = Unpooled.buffer();
        buf.writeShort(1); // version
        buf.writeInt(10);  // producerId length
        buf.writeBytes("pro".getBytes(StandardCharsets.UTF_8));
        try {
            assertThrows(BadRequestException.class, () -> ProducerRequestDeserializer.deserializeBinarySend(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    void truncatedAtNullableFlag_throwsBadRequest() {
        var buf = Unpooled.buffer();
        buf.writeShort(1); // version
        writeString(buf, "producer-1");
        writeString(buf, "token-1");
        writeString(buf, "topic-1");
        // partition null flag is missing
        try {
            assertThrows(BadRequestException.class, () -> ProducerRequestDeserializer.deserializeBinarySend(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    void truncatedValue_returnsShorterValue() {
        var buf = validSendPayload("value");
        var truncated = buf.readSlice(buf.readableBytes() - 2);
        try {
            var request = ProducerRequestDeserializer.deserializeBinarySend(truncated);
            assertArrayEquals("val".getBytes(StandardCharsets.UTF_8), request.getValue());
        } finally {
            buf.release();
        }
    }

    @Test
    void unsupportedVersion_throwsBadRequest() {
        var buf = Unpooled.buffer();
        buf.writeShort(2);
        try {
            var exception = assertThrows(BadRequestException.class,
                    () -> ProducerRequestDeserializer.deserializeBinarySend(buf));
            assertEquals("Unsupported version: 2", exception.getMessage());
        } finally {
            buf.release();
        }
    }

    private static ByteBuf validSendPayload(String value) {
        var buf = Unpooled.buffer();
        buf.writeShort(1); // version
        writeString(buf, "producer-1");
        writeString(buf, "token-1");
        writeString(buf, "topic-1");
        buf.writeByte(0); // partition is not null
        buf.writeInt(3);
        buf.writeInt(1); // headers count
        writeString(buf, "header-key");
        buf.writeByte(0); // header value is not null
        writeBytes(buf, "hv");
        buf.writeByte(0); // key is not null
        writeBytes(buf, "k");
        buf.writeByte(0); // value is not null
        buf.writeBytes(value.getBytes(StandardCharsets.UTF_8));
        return buf;
    }

    private static void writeString(ByteBuf buf, String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    private static void writeBytes(ByteBuf buf, String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
