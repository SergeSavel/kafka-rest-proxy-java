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

package pro.savel.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.io.IOException;
import java.io.InputStream;

public abstract class JsonUtils {

    public static <T> T parseJson(ObjectMapper objectMapper, ByteBuf byteBuf, Class<T> clazz) throws BadRequestException {
        T result;
        try (var inputStream = new ByteBufInputStream(byteBuf)) {
            result = objectMapper.readValue((InputStream) inputStream, clazz);
        } catch (IOException e) {
            throw new BadRequestException("Unable to parse json message.", e);
        }
        return result;
    }
}
