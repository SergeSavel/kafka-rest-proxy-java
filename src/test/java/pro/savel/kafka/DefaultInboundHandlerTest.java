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
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultInboundHandlerTest {

    @Test
    void lastHttpContent_droppedWithoutClosingChannel() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());

        assertFalse(channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT));
        assertTrue(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void httpContent_droppedWithoutClosingChannel() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());
        var content = new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[4]));

        assertFalse(channel.writeInbound(content));
        assertEquals(0, content.refCnt());
        assertTrue(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void unexpectedMessage_closesChannel() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());

        assertFalse(channel.writeInbound("unexpected"));
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void readTimeout_closesChannelWithoutWritingResponse() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());

        channel.pipeline().fireExceptionCaught(ReadTimeoutException.INSTANCE);

        assertNull(channel.readOutbound());
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void writeTimeout_closesChannelWithoutWritingResponse() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());

        channel.pipeline().fireExceptionCaught(WriteTimeoutException.INSTANCE);

        assertNull(channel.readOutbound());
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void timeoutDuringChunkedResponse_leavesAlreadyWrittenBytesIntact() {
        var channel = new EmbeddedChannel(new DefaultInboundHandler());
        var firstChunk = new DefaultHttpContent(Unpooled.wrappedBuffer("part".getBytes(StandardCharsets.UTF_8)));
        channel.writeOutbound(firstChunk);

        channel.pipeline().fireExceptionCaught(WriteTimeoutException.INSTANCE);

        var written = channel.readOutbound();
        assertInstanceOf(HttpContent.class, written);
        ((HttpContent) written).release();
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
