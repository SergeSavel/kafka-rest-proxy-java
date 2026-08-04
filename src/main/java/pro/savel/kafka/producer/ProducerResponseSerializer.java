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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.producer.responses.ProducerResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.io.IOException;

public class ProducerResponseSerializer {

    public static ByteBuf serializeJson(ObjectMapper objectMapper, ByteBufAllocator allocator,
                                        ProducerResponse response) throws IOException {
        return JsonUtils.serializeJson(objectMapper, allocator, response);
    }

    public static ByteBuf serializeBinary(ByteBufAllocator allocator, ProducerResponse response) {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ProducerSendResponse.class)
            return serializeSend(allocator, (ProducerSendResponse) response);
        else
            throw new IllegalArgumentException("Response class " + responseClass + " not supported");
    }

    private static ByteBuf serializeSend(ByteBufAllocator allocator, ProducerSendResponse response) {
        var topicLength = ByteBufUtil.utf8Bytes(response.getTopic());
        var capacity = Short.BYTES + Integer.BYTES + topicLength + Integer.BYTES
                + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;
        var buf = allocator.buffer(capacity);
        try {
            buf.writeShort(1); //version
            writeString(buf, response.getTopic());
            buf.writeInt(response.getPartition());
            buf.writeLong(response.getOffset());
            buf.writeLong(response.getTimestamp());
            buf.writeInt(response.getSerializedKeySize());
            buf.writeInt(response.getSerializedValueSize());
            return buf;
        } catch (Exception e) {
            buf.release();
            throw e;
        }
    }

    private static void writeString(ByteBuf buf, String value) {
        var length = ByteBufUtil.utf8Bytes(value);
        buf.writeInt(length);
        ByteBufUtil.writeUtf8(buf, value);
    }
}
