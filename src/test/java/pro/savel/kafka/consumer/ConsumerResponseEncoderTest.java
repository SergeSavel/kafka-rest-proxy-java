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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
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
import pro.savel.kafka.consumer.requests.ConsumerTouchRequest;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;
import pro.savel.kafka.consumer.responses.ConsumerSubscriptionResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerResponseEncoderTest {

    @Test
    void pollResponse_isStreamedAndCompletesRequestFlow() {
        var channel = new EmbeddedChannel(
                new HttpRequestFlowControlHandler(),
                new ChunkedWriteHandler(),
                new ConsumerResponseEncoder(new ObjectMapper(), 8));
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, POST, "/consumer/poll");
        assertTrue(channel.writeInbound(request));
        ReferenceCountUtil.release(channel.readInbound());
        assertFalse(channel.config().isAutoRead());

        var response = ConsumerPollResponse.of(records());
        var bearer = new ConsumerResponseBearer(
                new RequestBearer(new ConsumerPollRequest(), Serde.JSON, true), HttpResponseStatus.OK, response);
        assertTrue(channel.writeOutbound(bearer));
        channel.runPendingTasks();

        var headers = assertInstanceOf(HttpResponse.class, channel.readOutbound());
        assertTrue(HttpUtil.isTransferEncodingChunked(headers));
        assertFalse(headers.headers().contains(HttpHeaderNames.CONTENT_LENGTH));

        var body = Unpooled.buffer();
        var lastContentSeen = false;
        Object outbound;
        while ((outbound = channel.readOutbound()) != null) {
            var content = assertInstanceOf(HttpContent.class, outbound);
            if (content instanceof LastHttpContent)
                lastContentSeen = true;
            else {
                assertTrue(content.content().readableBytes() <= 8);
                body.writeBytes(content.content());
            }
            content.release();
        }

        assertTrue(lastContentSeen);
        assertTrue(channel.config().isAutoRead());
        assertTrue(body.toString(StandardCharsets.UTF_8).contains("\"value\":\"payload\""));
        body.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void nonPollResponse_withBinarySerde_returnsNotAcceptable() {
        var channel = new EmbeddedChannel(new ConsumerResponseEncoder(new ObjectMapper(), 64 * 1024));
        var bearer = new ConsumerResponseBearer(
                new RequestBearer(new ConsumerTouchRequest(), Serde.BINARY, true),
                HttpResponseStatus.OK,
                ConsumerSubscriptionResponse.of(List.of()));
        assertTrue(channel.writeOutbound(bearer));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response.status());
        assertEquals("Binary response format is not supported.",
                response.content().toString(StandardCharsets.UTF_8));
        response.release();
        channel.finishAndReleaseAll();
    }

    private static ConsumerRecords<byte[], byte[]> records() {
        var record = new ConsumerRecord<>("topic", 0, 1, 100L, TimestampType.CREATE_TIME,
                3, 7, "key".getBytes(StandardCharsets.UTF_8), "payload".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(), Optional.empty());
        return new ConsumerRecords<>(Map.of(new TopicPartition("topic", 0), List.of(record)), Map.of());
    }
}
