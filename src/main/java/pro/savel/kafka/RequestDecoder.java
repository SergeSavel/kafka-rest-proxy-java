// Copyright 2025 Sergey Savelev
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.producer.ProducerRequestDecoder;

import java.io.IOException;

@ChannelHandler.Sharable
public class RequestDecoder extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RequestDecoder.class);

    private final ProducerRequestDecoder producerDecoder;

    public RequestDecoder(ObjectMapper objectMapper) {
        producerDecoder = new ProducerRequestDecoder(objectMapper);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws IOException {
        var uri = httpRequest.uri();
        if (uri.startsWith(ProducerRequestDecoder.URI_PREFIX)) {
            producerDecoder.decode(ctx, httpRequest);
        } else {
            HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("An error occurred while decoding the request.", cause);
        ctx.close();
    }
}
