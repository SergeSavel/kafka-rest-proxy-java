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

package pro.savel.kafka.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.admin.requests.management.AdminTouchRequest;
import pro.savel.kafka.admin.responses.AdminDeleteTopicsResponse;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.contract.Serde;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminResponseEncoderTest {

    @Test
    void binarySerde_returnsNotAcceptable() {
        var channel = new EmbeddedChannel(new AdminResponseEncoder(new ObjectMapper()));
        var bearer = new AdminResponseBearer(
                new RequestBearer(new AdminTouchRequest(), Serde.BINARY, true),
                HttpResponseStatus.OK,
                AdminDeleteTopicsResponse.ofNames(Map.of()));
        assertTrue(channel.writeOutbound(bearer));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response.status());
        assertEquals("Binary response format is not supported.",
                response.content().toString(StandardCharsets.UTF_8));
        response.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void nullResponse_withBinarySerde_isWrittenWithoutBody() {
        var channel = new EmbeddedChannel(new AdminResponseEncoder(new ObjectMapper()));
        var bearer = new AdminResponseBearer(
                new RequestBearer(new AdminTouchRequest(), Serde.BINARY, true),
                HttpResponseStatus.NO_CONTENT,
                null);
        assertTrue(channel.writeOutbound(bearer));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.status());
        assertEquals(0, response.content().readableBytes());
        assertNull(response.headers().get("Content-Type"));
        response.release();
        channel.finishAndReleaseAll();
    }
}
