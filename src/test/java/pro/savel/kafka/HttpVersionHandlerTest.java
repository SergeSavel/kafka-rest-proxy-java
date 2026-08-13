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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpVersionHandlerTest {

    @Test
    void http11Request_passesThrough() {
        var channel = new EmbeddedChannel(new HttpVersionHandler());
        var request = request(HttpVersion.HTTP_1_1);

        assertTrue(channel.writeInbound(request));
        assertSame(request, channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void http10Request_returns505() {
        var channel = new EmbeddedChannel(new HttpVersionHandler());
        var request = request(HttpVersion.HTTP_1_0);

        assertFalse(channel.writeInbound(request));
        assertResponseStatus(channel, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        channel.finishAndReleaseAll();
    }

    @Test
    void rejectedRequestBody_discardedUntilLastContent() throws Exception {
        var handler = new HttpVersionHandler();
        var ctx = mock(ChannelHandlerContext.class);
        when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

        handler.channelRead(ctx, request(HttpVersion.HTTP_1_0));
        verify(ctx).writeAndFlush(any());
        verify(ctx, never()).fireChannelRead(any());

        var content = new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[4]));
        handler.channelRead(ctx, content);
        assertEquals(0, content.refCnt());

        var lastContent = new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[2]));
        handler.channelRead(ctx, lastContent);
        assertEquals(0, lastContent.refCnt());

        var nextRequest = request(HttpVersion.HTTP_1_1);
        handler.channelRead(ctx, nextRequest);
        verify(ctx).fireChannelRead(nextRequest);
    }

    @Test
    void rejectedBodylessRequest_emptyLastContentDiscarded() throws Exception {
        var handler = new HttpVersionHandler();
        var ctx = mock(ChannelHandlerContext.class);
        when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

        handler.channelRead(ctx, request(HttpVersion.HTTP_1_0));
        handler.channelRead(ctx, LastHttpContent.EMPTY_LAST_CONTENT);
        verify(ctx, never()).fireChannelRead(any());

        var nextRequest = request(HttpVersion.HTTP_1_1);
        handler.channelRead(ctx, nextRequest);
        verify(ctx).fireChannelRead(nextRequest);
    }

    private static HttpRequest request(HttpVersion version) {
        return new DefaultHttpRequest(version, HttpMethod.POST, "/");
    }

    private static void assertResponseStatus(EmbeddedChannel channel, HttpResponseStatus expectedStatus) {
        var response = channel.<FullHttpResponse>readOutbound();
        assertEquals(expectedStatus, response.status());
        ReferenceCountUtil.release(response);
    }
}
