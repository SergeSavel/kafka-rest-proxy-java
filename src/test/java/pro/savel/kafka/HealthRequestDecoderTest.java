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
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.HttpUtils;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HealthRequestDecoderTest {

    @Test
    void get_returnsOkWithJsonContentType() {
        var channel = new EmbeddedChannel(new HealthRequestDecoder());
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health");
        assertFalse(channel.writeInbound(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals("{\"status\":\"UP\"}", response.content().toString(StandardCharsets.UTF_8));
        assertEquals(HttpUtils.APPLICATION_JSON_CHARSET_UTF8, response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        response.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void post_returnsMethodNotAllowed() {
        var channel = new EmbeddedChannel(new HealthRequestDecoder());
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/health");
        assertFalse(channel.writeInbound(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
        response.release();
        channel.finishAndReleaseAll();
    }
}
