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
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.HttpRequestFlowControlHandler;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.consumer.requests.ConsumerPollRequest;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives a binary poll response through the REAL production handler chain
 * (HttpRequestFlowControlHandler -> ChunkedWriteHandler -> ConsumerResponseEncoder), unlike
 * ConsumerPollChunkedInputTest which exercises ConsumerPollChunkedInput in isolation.
 */
class ConsumerPollPipelineIntegrationTest {

    @Test
    void binaryPollResponse_throughRealPipeline_isNotTruncated() {
        var objectMapper = new ObjectMapper();
        var chunkBytes = 16; // small on purpose, to force many chunk boundaries
        var encoder = new ConsumerResponseEncoder(objectMapper, chunkBytes);
        var channel = new EmbeddedChannel(new HttpRequestFlowControlHandler(), new ChunkedWriteHandler(), encoder);

        var response = manyMessagesResponse(50);
        var expected = ConsumerResponseSerializer.serializeBinary(UnpooledByteBufAllocator.DEFAULT, response);

        var pollRequest = new ConsumerPollRequest();
        pollRequest.setConsumerId("c-1");
        pollRequest.setToken("t-1");
        var requestBearer = new RequestBearer(pollRequest, Serde.BINARY, true);
        var responseBearer = new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);

        channel.writeOutbound(responseBearer);

        var reassembled = Unpooled.buffer();
        Object out;
        int contentChunks = 0;
        boolean sawLast = false;
        while ((out = channel.readOutbound()) != null) {
            if (out instanceof HttpResponse) {
                // headers only, no body bytes
            } else if (out instanceof HttpContent content) {
                contentChunks++;
                reassembled.writeBytes(content.content());
                if (out instanceof LastHttpContent)
                    sawLast = true;
                content.release();
            }
        }

        assertTrue(sawLast, "stream never delivered a LastHttpContent - client would hang or see it as truncated");
        assertTrue(contentChunks > 1, "test is only meaningful if the response actually spans multiple chunks");
        assertEquals(expected.readableBytes(), reassembled.readableBytes(),
                "reassembled byte count differs from the reference non-chunked serialization - response is truncated/corrupted");
        assertEquals(0, expected.compareTo(reassembled));

        expected.release();
        reassembled.release();
        channel.finishAndReleaseAll();
    }

    private static ConsumerPollResponse manyMessagesResponse(int count) {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>(count);
        for (int i = 0; i < count; i++) {
            var key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            var value = ("value-" + i + "-" + "x".repeat(i % 7)).getBytes(StandardCharsets.UTF_8);
            records.add(new ConsumerRecord<>("topic", 0, i, 100L + i, TimestampType.CREATE_TIME,
                    key.length, value.length, key, value, new RecordHeaders(), Optional.empty()));
        }
        var consumerRecords = new ConsumerRecords<byte[], byte[]>(
                Map.of(new TopicPartition("topic", 0), records), Map.of());
        return ConsumerPollResponse.of(consumerRecords);
    }
}
