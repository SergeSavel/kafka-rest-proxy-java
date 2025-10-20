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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pro.savel.kafka.producer.responses.ProducerResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.nio.charset.StandardCharsets;

public class ProducerResponseSerializer {

    public static ByteBuf serializeJson(ObjectMapper objectMapper, ProducerResponse response) throws JsonProcessingException {
        if (response == null)
            return null;
        var bytes = objectMapper.writeValueAsBytes(response);
        return Unpooled.wrappedBuffer(bytes);
    }

    public static ByteBuf serializeBinary(ProducerResponse response) {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        if (responseClass == ProducerSendResponse.class)
            return serializeSend((ProducerSendResponse) response);
        else
            throw new IllegalArgumentException("Response class " + responseClass + " not supported");
    }

    private static ByteBuf serializeSend(ProducerSendResponse response) {
        var buf = Unpooled.buffer();
        buf.writeShort(1); //version
        writeString(buf, response.getTopic());
        buf.writeInt(response.getPartition());
        buf.writeLong(response.getOffset());
        buf.writeLong(response.getTimestamp());
        buf.writeInt(response.getSerializedKeySize());
        buf.writeInt(response.getSerializedValueSize());
        return buf;
    }

    private static void writeString(ByteBuf buf, String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
