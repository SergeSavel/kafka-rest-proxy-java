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

package pro.savel.kafka.consumer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.consumer.responses.*;

import java.io.IOException;
import java.io.OutputStream;

public class ConsumerResponseSerializer {

    public static ByteBuf serializeJson(ObjectMapper objectMapper, ByteBufAllocator allocator,
                                        ConsumerResponse response) throws IOException {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ConsumerPollResponse.class)
            return serializePollJson(objectMapper, allocator, (ConsumerPollResponse) response);
        return JsonUtils.serializeJson(objectMapper, allocator, response);
    }

    public static ByteBuf serializeBinary(ByteBufAllocator allocator, ConsumerResponse response) {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ConsumerPollResponse.class)
            return serializePollBinary(allocator, (ConsumerPollResponse) response);
        throw new IllegalArgumentException("Binary serialization of response class " + responseClass + " not supported");
    }

    private static ByteBuf serializePollJson(ObjectMapper objectMapper, ByteBufAllocator allocator,
                                             ConsumerPollResponse response) throws IOException {
        var buf = allocator.buffer();
        try {
            try (var outputStream = new ByteBufOutputStream(buf);
                 var generator = objectMapper.getFactory().createGenerator((OutputStream) outputStream)) {
                generator.writeStartArray();
                for (var message : response) {
                    writePollJsonMessage(generator, message);
                }
                generator.writeEndArray();
            }
            return buf;
        } catch (IOException | RuntimeException e) {
            buf.release();
            throw e;
        }
    }

    static ByteBuf serializePollJsonMessage(ObjectMapper objectMapper, ByteBufAllocator allocator,
                                            ConsumerPollResponse.Message message, boolean first) throws IOException {
        var buf = allocator.buffer();
        try {
            buf.writeByte(first ? '[' : ',');
            try (var outputStream = new ByteBufOutputStream(buf);
                 var generator = objectMapper.getFactory().createGenerator((OutputStream) outputStream)) {
                writePollJsonMessage(generator, message);
            }
            return buf;
        } catch (IOException | RuntimeException e) {
            buf.release();
            throw e;
        }
    }

    private static void writePollJsonMessage(JsonGenerator generator, ConsumerPollResponse.Message message)
            throws IOException {
        if (message == null) {
            generator.writeNull();
            return;
        }
        generator.writeStartObject();
        generator.writeNumberField("timestamp", message.getTimestamp());
        generator.writeStringField("topic", message.getTopic());
        generator.writeNumberField("partition", message.getPartition());
        generator.writeNumberField("offset", message.getOffset());
        generator.writeFieldName("headers");
        if (message.getHeaders() == null) {
            generator.writeStartArray();
            generator.writeEndArray();
        } else {
            generator.writeStartArray();
            for (var header : message.getHeaders()) {
                if (header == null) {
                    generator.writeNull();
                    continue;
                }
                generator.writeStartObject();
                generator.writeStringField("key", header.getKey());
                writeUtf8Field(generator, "value", header.getValue());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }
        writeUtf8Field(generator, "key", message.getKey());
        writeUtf8Field(generator, "value", message.getValue());
        generator.writeEndObject();
    }

    private static void writeUtf8Field(JsonGenerator generator, String name, byte[] value)
            throws IOException {
        generator.writeFieldName(name);
        if (value == null)
            generator.writeNull();
        else
            generator.writeUTF8String(value, 0, value.length);
    }

    private static ByteBuf serializePollBinary(ByteBufAllocator allocator, ConsumerPollResponse response) {
        var buf = allocator.buffer(calculatePollBinaryCapacity(response));
        try {
            buf.writeShort(1); //version
            buf.writeInt(response.size());
            for (ConsumerPollResponse.Message message : response)
                writePollBinaryMessage(buf, message);
            return buf;
        } catch (Exception e) {
            buf.release();
            throw e;
        }
    }

    static ByteBuf serializePollBinaryHeader(ByteBufAllocator allocator, int messageCount) {
        var buf = allocator.buffer(Short.BYTES + Integer.BYTES);
        buf.writeShort(1); // version
        buf.writeInt(messageCount);
        return buf;
    }

    static ByteBuf serializePollBinaryMessage(ByteBufAllocator allocator, ConsumerPollResponse.Message message) {
        var buf = allocator.buffer(calculatePollBinaryMessageCapacity(message));
        try {
            writePollBinaryMessage(buf, message);
            return buf;
        } catch (RuntimeException e) {
            buf.release();
            throw e;
        }
    }

    private static void writePollBinaryMessage(ByteBuf buf, ConsumerPollResponse.Message message) {
        writeBytes(buf, message.getTopic());
        buf.writeInt(message.getPartition());
        buf.writeLong(message.getOffset());
        buf.writeLong(message.getTimestamp());
        var headers = message.getHeaders();
        buf.writeInt(headers == null ? 0 : headers.size());
        if (headers != null) {
            for (ConsumerPollResponse.Message.Header header : headers) {
                writeBytes(buf, header.getKey());
                if (header.getValue() == null)
                    buf.writeByte(1); // is null
                else {
                    buf.writeByte(0); // is not null
                    writeBytes(buf, header.getValue());
                }
            }
        }
        if (message.getKey() == null)
            buf.writeByte(1); // is null
        else {
            buf.writeByte(0); // is not null
            writeBytes(buf, message.getKey());
        }
        if (message.getValue() == null)
            buf.writeByte(1); // is null
        else {
            buf.writeByte(0); // is not null
            writeBytes(buf, message.getValue());
        }
    }

    private static void writeBytes(ByteBuf buf, String value) {
        var length = ByteBufUtil.utf8Bytes(value);
        buf.writeInt(length);
        ByteBufUtil.writeUtf8(buf, value);
    }

    private static void writeBytes(ByteBuf buf, byte[] bytes) {
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    private static int calculatePollBinaryCapacity(ConsumerPollResponse response) {
        long capacity = Short.BYTES + Integer.BYTES;
        for (var message : response)
            capacity += calculatePollBinaryMessageCapacity(message);
        return Math.toIntExact(capacity);
    }

    private static int calculatePollBinaryMessageCapacity(ConsumerPollResponse.Message message) {
        long capacity = serializedStringSize(message.getTopic())
                + Integer.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;
        if (message.getHeaders() != null) {
            for (var header : message.getHeaders()) {
                capacity += serializedStringSize(header.getKey()) + Byte.BYTES;
                if (header.getValue() != null)
                    capacity += serializedBytesSize(header.getValue());
            }
        }
        capacity += Byte.BYTES;
        if (message.getKey() != null)
            capacity += serializedBytesSize(message.getKey());
        capacity += Byte.BYTES;
        if (message.getValue() != null)
            capacity += serializedBytesSize(message.getValue());
        return Math.toIntExact(capacity);
    }

    private static long serializedStringSize(String value) {
        return Integer.BYTES + ByteBufUtil.utf8Bytes(value);
    }

    private static long serializedBytesSize(byte[] value) {
        return Integer.BYTES + value.length;
    }
}
