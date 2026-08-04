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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpRequestFlowControlHandlerTest {

    @Test
    void pipelinedRequests_areProcessedOneAtATime() {
        var channel = new EmbeddedChannel(new HttpRequestFlowControlHandler());
        var firstRequest = request("/first");
        var secondRequest = request("/second");

        assertTrue(channel.writeInbound(firstRequest));
        var firstInbound = channel.readInbound();
        assertSame(firstRequest, firstInbound);
        ReferenceCountUtil.release(firstInbound);
        assertFalse(channel.config().isAutoRead());

        assertFalse(channel.writeInbound(secondRequest));
        assertNull(channel.readInbound());

        var firstResponse = response();
        assertTrue(channel.writeOutbound(firstResponse));
        channel.runPendingTasks();

        var firstOutbound = channel.readOutbound();
        assertSame(firstResponse, firstOutbound);
        ReferenceCountUtil.release(firstOutbound);
        var secondInbound = channel.readInbound();
        assertSame(secondRequest, secondInbound);
        ReferenceCountUtil.release(secondInbound);
        assertFalse(channel.config().isAutoRead());

        var secondResponse = response();
        assertTrue(channel.writeOutbound(secondResponse));
        channel.runPendingTasks();

        var secondOutbound = channel.readOutbound();
        assertSame(secondResponse, secondOutbound);
        ReferenceCountUtil.release(secondOutbound);
        assertTrue(channel.config().isAutoRead());
        channel.finishAndReleaseAll();
    }

    @Test
    void channelClose_releasesQueuedRequests() {
        var channel = new EmbeddedChannel(new HttpRequestFlowControlHandler());
        var activeRequest = request("/active");
        var queuedRequest = request("/queued");

        assertTrue(channel.writeInbound(activeRequest));
        ReferenceCountUtil.release(channel.readInbound());
        assertFalse(channel.writeInbound(queuedRequest));
        assertEquals(1, queuedRequest.refCnt());

        channel.close();

        assertEquals(0, queuedRequest.refCnt());
        channel.finishAndReleaseAll();
    }

    private static DefaultFullHttpRequest request(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }

    private static DefaultFullHttpResponse response() {
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    }
}
