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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerPollChunkedInputTest {

    private static final int CHUNK_SIZE = 11;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    @Test
    void jsonChunks_matchExistingWireFormatAndRespectLimit() throws Exception {
        var response = response();
        var expected = ConsumerResponseSerializer.serializeJson(objectMapper, allocator, response);
        var input = new ConsumerPollChunkedInput(objectMapper, response, Serde.JSON, CHUNK_SIZE);

        var actual = readAll(input, CHUNK_SIZE);

        assertTrue(ByteBufUtil.equals(expected, actual));
        expected.release();
        actual.release();
    }

    @Test
    void binaryChunks_matchExistingWireFormatAndRespectLimit() throws Exception {
        var response = response();
        var expected = ConsumerResponseSerializer.serializeBinary(allocator, response);
        var input = new ConsumerPollChunkedInput(objectMapper, response, Serde.BINARY, CHUNK_SIZE);

        var actual = readAll(input, CHUNK_SIZE);

        assertTrue(ByteBufUtil.equals(expected, actual));
        expected.release();
        actual.release();
    }

    @Test
    void emptyResponses_preserveWireFormat() throws Exception {
        var response = ConsumerPollResponse.of(new ConsumerRecords<>(Collections.emptyMap(), Collections.emptyMap()));

        var json = readAll(new ConsumerPollChunkedInput(objectMapper, response, Serde.JSON, 1), 1);
        assertEquals("[]", json.toString(StandardCharsets.UTF_8));
        json.release();

        var binary = readAll(new ConsumerPollChunkedInput(objectMapper, response, Serde.BINARY, 1), 1);
        assertEquals(1, binary.readShort());
        assertEquals(0, binary.readInt());
        binary.release();
    }

    @Test
    void invalidArguments_areRejected() {
        var response = response();
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerPollChunkedInput(objectMapper, response, Serde.JSON, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerPollChunkedInput(objectMapper, response, null, CHUNK_SIZE));
    }

    private ByteBuf readAll(ConsumerPollChunkedInput input, int chunkSize) throws Exception {
        var result = allocator.buffer();
        var chunks = 0;
        try {
            while (!input.isEndOfInput()) {
                var chunk = input.readChunk(allocator);
                assertNotNull(chunk);
                assertTrue(chunk.readableBytes() <= chunkSize);
                result.writeBytes(chunk);
                chunk.release();
                chunks++;
            }
            assertTrue(chunks > 1);
            assertEquals(result.readableBytes(), input.progress());
            return result;
        } catch (Throwable e) {
            result.release();
            throw e;
        } finally {
            input.close();
        }
    }

    private static ConsumerPollResponse response() {
        var first = record("topic-1", 0, 1, "key", "value-1");
        var second = record("topic-1", 0, 2, "ключ\"", "строка\n");
        var records = new ConsumerRecords<byte[], byte[]>(
                Map.of(new TopicPartition("topic-1", 0), List.of(first, second)), Map.of());
        return ConsumerPollResponse.of(records);
    }

    private static ConsumerRecord<byte[], byte[]> record(
            String topic, int partition, long offset, String key, String value) {
        var headers = new RecordHeaders(List.of(
                new RecordHeader("header", "значение\n".getBytes(StandardCharsets.UTF_8))));
        var keyBytes = key.getBytes(StandardCharsets.UTF_8);
        var valueBytes = value.getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(topic, partition, offset, 100L, TimestampType.CREATE_TIME,
                keyBytes.length, valueBytes.length, keyBytes, valueBytes, headers, Optional.empty());
    }
}
