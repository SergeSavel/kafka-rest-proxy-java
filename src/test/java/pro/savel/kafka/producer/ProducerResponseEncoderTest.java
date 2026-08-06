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

package pro.savel.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.producer.requests.ProducerTouchRequest;
import pro.savel.kafka.producer.responses.ProducerPartitionsResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerResponseEncoderTest {

    @Test
    void nonSendResponse_withBinarySerde_returnsNotAcceptable() {
        var channel = new EmbeddedChannel(new ProducerResponseEncoder(new ObjectMapper()));
        var bearer = new ProducerResponseBearer(
                new RequestBearer(new ProducerTouchRequest(), Serde.BINARY, true),
                HttpResponseStatus.OK,
                ProducerPartitionsResponse.of(List.of()));
        assertTrue(channel.writeOutbound(bearer));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response.status());
        assertEquals("Binary response format is not supported.",
                response.content().toString(StandardCharsets.UTF_8));
        response.release();
        channel.finishAndReleaseAll();
    }
}
