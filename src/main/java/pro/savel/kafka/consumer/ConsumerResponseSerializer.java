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
import pro.savel.kafka.consumer.responses.ConsumerResponse;

public class ConsumerResponseSerializer {

    public static ByteBuf serializeJson(ObjectMapper objectMapper, ConsumerResponse response) throws JsonProcessingException {
        if (response == null)
            return null;
        var bytes = objectMapper.writeValueAsBytes(response);
        return Unpooled.wrappedBuffer(bytes);
    }

    public static ByteBuf serializeBinary(ConsumerResponse response) {
        if (response == null)
            return null;
        var responseClass = response.getClass();
        throw new IllegalArgumentException("Response class " + responseClass + " not supported");
    }
}
