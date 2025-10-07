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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pro.savel.kafka.consumer.responses.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class ConsumerResponseSerializer {

    public static ByteBuf serializeJson(ObjectMapper objectMapper, ConsumerResponse response) throws JsonProcessingException {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ConsumerPollResponse.class)
            response = toPollStringResponse((ConsumerPollResponse) response);
        var bytes = objectMapper.writeValueAsBytes(response);
        return Unpooled.wrappedBuffer(bytes);
    }

    public static ByteBuf serializeBinary(ConsumerResponse response) {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ConsumerPollResponse.class)
            return toPollBinaryResponse((ConsumerPollResponse) response);
        throw new IllegalArgumentException("Binary serialization of response class " + responseClass + " not supported");
    }

    private static ConsumerPollStringResponse toPollStringResponse(ConsumerPollResponse source) {
        if (source == null)
            return null;
        var result = new ConsumerPollStringResponse(source.size());
        source.forEach(item -> result.add(toStringMessage(item)));
        return result;
    }

    private static ConsumerStringMessage toStringMessage(ConsumerMessage source) {
        if (source == null)
            return null;
        var result = new ConsumerStringMessage();
        result.setTimestamp(source.getTimestamp());
        result.setTopic(source.getTopic());
        result.setPartition(source.getPartition());
        result.setOffset(source.getOffset());
        result.setHeaders(toStringMessageHeaders(source.getHeaders()));
        result.setKey(toStringUtf8(source.getKey()));
        result.setValue(toStringUtf8(source.getValue()));
        return result;
    }

    private static Collection<ConsumerStringMessage.Header> toStringMessageHeaders(Collection<ConsumerMessage.Header> source) {
        if (source == null)
            return null;
        var result = new ArrayList<ConsumerStringMessage.Header>(source.size());
        source.forEach(item -> result.add(toStringMessageHeader(item)));
        return result;
    }

    private static ConsumerStringMessage.Header toStringMessageHeader(ConsumerMessage.Header source) {
        if (source == null)
            return null;
        var result = new ConsumerStringMessage.Header();
        result.setKey(source.getKey());
        result.setValue(toStringUtf8(source.getValue()));
        return result;
    }

    private static String toStringUtf8(byte[] source) {
        if (source == null)
            return null;
        return new String(source, StandardCharsets.UTF_8);
    }

    private static ByteBuf toPollBinaryResponse(ConsumerPollResponse response) {
        var buf = Unpooled.buffer();
        buf.writeShort(1); //version
        buf.writeInt(response.size());
        for (ConsumerMessage message : response) {
            buf.writeLong(message.getTimestamp());
            writeBytes(buf, message.getTopic());
            buf.writeInt(message.getPartition());
            buf.writeLong(message.getOffset());
            buf.writeInt(message.getHeaders().size());
            for (ConsumerMessage.Header header : message.getHeaders()) {
                writeBytes(buf, header.getKey());
                if (header.getValue() == null)
                    buf.writeByte(1); // is null
                else {
                    buf.writeByte(0); // is not null
                    writeBytes(buf, header.getValue());
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
        return buf;
    }

    private static void writeBytes(ByteBuf buf, String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    private static void writeBytes(ByteBuf buf, byte[] bytes) {
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
