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

package pro.savel.kafka;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.HttpUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonRequestSizeLimitHandlerTest {

    @Test
    void invalidLimit_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> new JsonRequestSizeLimitHandler(0));
    }

    @Test
    void jsonContentLengthAboveLimit_returns413() {
        var channel = new EmbeddedChannel(new JsonRequestSizeLimitHandler(4));
        var request = request(HttpUtils.APPLICATION_JSON, 5);

        assertFalse(channel.writeInbound(request));
        assertResponseStatus(channel, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        channel.finishAndReleaseAll();
    }

    @Test
    void chunkedJsonAboveLimit_returns413() {
        var channel = new EmbeddedChannel(new JsonRequestSizeLimitHandler(4));
        var request = request(HttpUtils.APPLICATION_JSON, null);
        request.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        assertTrue(channel.writeInbound(request));
        assertSame(request, channel.readInbound());

        var firstChunk = new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[3]));
        assertTrue(channel.writeInbound(firstChunk));
        var firstInbound = channel.readInbound();
        assertSame(firstChunk, firstInbound);
        ReferenceCountUtil.release(firstInbound);

        var lastChunk = new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[2]));
        assertFalse(channel.writeInbound(lastChunk));
        assertEquals(0, lastChunk.refCnt());
        assertResponseStatus(channel, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        channel.finishAndReleaseAll();
    }

    @Test
    void binaryRequestAboveJsonLimit_passesThrough() {
        var channel = new EmbeddedChannel(new JsonRequestSizeLimitHandler(4));
        var request = request(HttpUtils.APPLICATION_OCTET_STREAM, 5);
        var content = new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[5]));

        assertTrue(channel.writeInbound(request));
        assertSame(request, channel.readInbound());
        assertTrue(channel.writeInbound(content));
        var contentInbound = channel.readInbound();
        assertSame(content, contentInbound);
        ReferenceCountUtil.release(contentInbound);
        channel.finishAndReleaseAll();
    }

    private static DefaultHttpRequest request(String contentType, Integer contentLength) {
        var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        if (contentLength != null)
            request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, contentLength);
        return request;
    }

    private static void assertResponseStatus(EmbeddedChannel channel, HttpResponseStatus expectedStatus) {
        var response = channel.<FullHttpResponse>readOutbound();
        assertEquals(expectedStatus, response.status());
        ReferenceCountUtil.release(response);
    }
}
