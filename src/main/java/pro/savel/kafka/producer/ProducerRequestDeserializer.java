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

import io.netty.buffer.ByteBuf;
import pro.savel.kafka.producer.requests.ProducerSendRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ProducerRequestDeserializer {

    public static ProducerSendRequest deserializeBinarySend(ByteBuf buf) {
        if (buf == null)
            return null;
        var version = buf.readShort();
        if (version == 1)
            return deserializeBinarySendV1(buf);
        else
            throw new IllegalArgumentException("Unsupported version: " + version);
    }

    private static ProducerSendRequest deserializeBinarySendV1(ByteBuf buf) {
        var request = new ProducerSendRequest();
        request.setProducerId(readUuid(buf));
        request.setToken(readString(buf));
        request.setTopic(readString(buf));
        request.setPartition(readNullableInt(buf));
        request.setHeaders(readHeders(buf));
        request.setKey(readNullableBytes(buf));
        request.setValue(readNullableBytesToTheEnd(buf));
        return request;
    }

    private static Map<String, byte[]> readHeders(ByteBuf buf) {
        var headersCount = buf.readInt();
        var headers = new HashMap<String, byte[]>(headersCount);
        for (int i = 1; i <= headersCount; i++) {
            var headerKey = readString(buf);
            var headerValue = readNullableBytes(buf);
            headers.put(headerKey, headerValue);
        }
        return headers;
    }

    private static byte[] readBytes(ByteBuf buf, int length) {
        var bytes = new byte[length];
        buf.readBytes(bytes);
        return bytes;
    }

    private static String readString(ByteBuf buf, int length) {
        var bytes = readBytes(buf, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readString(ByteBuf buf) {
        var length = buf.readInt();
        return readString(buf, length);
    }

    private static UUID readUuid(ByteBuf buf) {
        var uuidString = readString(buf);
        return UUID.fromString(uuidString);
    }

    private static Integer readNullableInt(ByteBuf buf) {
        var isNull = buf.readByte();
        if (isNull == 1)
            return null;
        return buf.readInt();
    }

    private static byte[] readNullableBytes(ByteBuf buf) {
        var isNull = buf.readByte();
        if (isNull == 1)
            return null;
        int length = buf.readInt();
        return readBytes(buf, length);
    }

    private static byte[] readNullableBytesToTheEnd(ByteBuf buf) {
        var isNull = buf.readByte();
        if (isNull == 1)
            return null;
        return readBytes(buf, buf.readableBytes());
    }
}
