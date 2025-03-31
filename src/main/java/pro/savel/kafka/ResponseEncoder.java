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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.ResponseBearer;
import pro.savel.kafka.common.contract.Serde;

@ChannelHandler.Sharable
public class ResponseEncoder extends SimpleChannelInboundHandler<ResponseBearer> {

    private static final Logger logger = LoggerFactory.getLogger(ResponseEncoder.class);

    private final ObjectMapper objectMapper;

    public ResponseEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ResponseBearer bearer) {

        FullHttpResponse httpResponse;
        try {
            httpResponse = getHttpResponse(bearer);
        } catch (JsonProcessingException e) {
            logger.error("An error occurred during request serialization.", e);
            HttpUtils.writeInternalServerErrorAndClose(ctx, bearer.protocolVersion());
            return;
        }

        var future = ctx.writeAndFlush(httpResponse);

        if (!bearer.connectionKeepAlive()) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private FullHttpResponse getHttpResponse(ResponseBearer bearer) throws JsonProcessingException {
        FullHttpResponse httpResponse;

        if (bearer.response() == null) {
            httpResponse = new DefaultFullHttpResponse(bearer.protocolVersion(), bearer.status());
        } else {
            if (bearer.serializeTo() == Serde.JSON) {
                var bytes = objectMapper.writeValueAsBytes(bearer.response());
                httpResponse = new DefaultFullHttpResponse(bearer.protocolVersion(), bearer.status(), Unpooled.wrappedBuffer(bytes));
                httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_JSON_CHARSET_UTF8);
            } else {
                throw new RuntimeException("Unimplemented");
            }
        }
        httpResponse.headers().setInt(HttpUtils.ASCII_CONTENT_LENGTH, httpResponse.content().readableBytes());

        if (bearer.connectionKeepAlive()) {
            if (!bearer.protocolVersion().isKeepAliveDefault()) {
                httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
        } else {
            httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.CLOSE);
        }

        return httpResponse;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("An error occurred while processing the request.", cause);
        ctx.close();
    }
}
