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
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.producer.requests.ProducerSendRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ProducerRequestDeserializer {

    public static ProducerSendRequest deserializeBinarySend(ByteBuf buf) throws BadRequestException {
        if (buf == null)
            return null;
        var version = readPositiveShort(buf);
        if (version == 1)
            return deserializeBinarySendV1(buf);
        else
            throw new IllegalArgumentException("Unsupported version: " + version);
    }

    private static ProducerSendRequest deserializeBinarySendV1(ByteBuf buf) throws BadRequestException {
        var request = new ProducerSendRequest();
        request.setProducerId(readString(buf));
        request.setToken(readString(buf));
        request.setTopic(readString(buf));
        request.setPartition(readNullableInt(buf));
        request.setHeaders(readHeders(buf));
        request.setKey(readNullableBytes(buf));
        request.setValue(readNullableBytesToTheEnd(buf));
        return request;
    }

    private static Map<String, byte[]> readHeders(ByteBuf buf) throws BadRequestException {
        var headersCount = readPositiveInt(buf);
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

    private static String readString(ByteBuf buf) throws BadRequestException {
        var length = readPositiveInt(buf);
        return readString(buf, length);
    }

    private static Integer readNullableInt(ByteBuf buf) throws BadRequestException {
        var isNull = readBoolean(buf);
        if (isNull)
            return null;
        return readPositiveInt(buf);
    }

    private static byte[] readNullableBytes(ByteBuf buf) throws BadRequestException {
        var isNull = readBoolean(buf);
        if (isNull)
            return null;
        int length = readPositiveInt(buf);
        return readBytes(buf, length);
    }

    private static byte[] readNullableBytesToTheEnd(ByteBuf buf) throws BadRequestException {
        var isNull = readBoolean(buf);
        if (isNull)
            return null;
        return readBytes(buf, buf.readableBytes());
    }

    private static short readPositiveShort(ByteBuf buf) throws BadRequestException {
        var value = buf.readShort();
        if (value < 0)
            throw new BadRequestException("Illegal value in binary content");
        return value;
    }

    private static int readPositiveInt(ByteBuf buf) throws BadRequestException {
        var value = buf.readInt();
        if (value < 0)
            throw new BadRequestException("Illegal value in binary content");
        return value;
    }

    private static boolean readBoolean(ByteBuf buf) throws BadRequestException {
        var value = buf.readByte();
        if (value == 0)
            return false;
        else if (value == 1)
            return true;
        throw new BadRequestException("Illegal value in binary content");
    }
}
