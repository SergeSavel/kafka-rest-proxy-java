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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import jakarta.validation.Validation;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.consumer.requests.ConsumerPollRequest;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerRequestDecoderTest {

    private static final int READ_TIMEOUT_SECONDS = 300;
    private static final long LIMIT_MS = READ_TIMEOUT_SECONDS * 1000L;

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(decoder(READ_TIMEOUT_SECONDS));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private static ConsumerRequestDecoder decoder(int maxPollTimeoutSeconds) {
        var objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        var validatorFactory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        return new ConsumerRequestDecoder(objectMapper, validatorFactory, maxPollTimeoutSeconds);
    }

    @Test
    void decodePoll_timeoutAboveLimit_returnsBadRequest() {
        assertFalse(channel.writeInbound(pollRequest(LIMIT_MS + 1)));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void decodePoll_timeoutAtLimit_passesRequest() {
        assertTrue(channel.writeInbound(pollRequest(LIMIT_MS)));

        RequestBearer bearer = channel.readInbound();
        var request = (ConsumerPollRequest) bearer.request();
        assertEquals(LIMIT_MS, request.getTimeout());
    }

    @Test
    void decodePoll_limitFollowsConfiguredReadTimeout() {
        // A timeout the default limit would have accepted, rejected by a decoder built with a
        // lower read timeout - so the bound really tracks the setting rather than a constant.
        var strictChannel = new EmbeddedChannel(decoder(60));

        assertTrue(strictChannel.writeInbound(pollRequest(60_000)));
        RequestBearer accepted = strictChannel.readInbound();
        assertEquals(60_000, ((ConsumerPollRequest) accepted.request()).getTimeout());

        assertFalse(strictChannel.writeInbound(pollRequest(60_001)));
        FullHttpResponse response = strictChannel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
        strictChannel.finishAndReleaseAll();
    }

    @Test
    void decodePoll_timeoutMs_passesRequest() {
        assertTrue(channel.writeInbound(pollRequestWithBody(
                "{\"consumerId\":\"consumer-1\",\"token\":\"token-1\",\"timeoutMs\":1000}")));

        RequestBearer bearer = channel.readInbound();
        var request = (ConsumerPollRequest) bearer.request();
        assertEquals(1000L, request.resolveTimeoutMs());
    }

    @Test
    void decodePoll_timeoutMsAboveLimit_returnsBadRequest() {
        assertFalse(channel.writeInbound(pollRequestWithBody(
                "{\"consumerId\":\"consumer-1\",\"token\":\"token-1\",\"timeoutMs\":" + (LIMIT_MS + 1) + "}")));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void decodePoll_bothTimeoutFields_timeoutMsWins() {
        assertTrue(channel.writeInbound(pollRequestWithBody(
                "{\"consumerId\":\"consumer-1\",\"token\":\"token-1\",\"timeout\":5000,\"timeoutMs\":1000}")));

        RequestBearer bearer = channel.readInbound();
        var request = (ConsumerPollRequest) bearer.request();
        assertEquals(1000L, request.resolveTimeoutMs());
    }

    @Test
    void decodePoll_noTimeout_returnsBadRequest() {
        assertFalse(channel.writeInbound(pollRequestWithBody(
                "{\"consumerId\":\"consumer-1\",\"token\":\"token-1\"}")));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    private static FullHttpRequest pollRequest(long timeout) {
        return pollRequestWithBody("{\"consumerId\":\"consumer-1\",\"token\":\"token-1\",\"timeout\":" + timeout + "}");
    }

    private static FullHttpRequest pollRequestWithBody(String bodyJson) {
        var body = Unpooled.copiedBuffer(bodyJson, StandardCharsets.UTF_8);
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/consumer/poll", body);
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
        return request;
    }
}
