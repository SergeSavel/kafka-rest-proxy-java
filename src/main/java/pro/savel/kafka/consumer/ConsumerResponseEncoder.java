// Copyright 2025 Sergey Savelev (serge@savel.pro)
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.Serde;

@ChannelHandler.Sharable
public class ConsumerResponseEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerResponseEncoder.class);

    private final ObjectMapper objectMapper;

    public ConsumerResponseEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ConsumerResponseBearer bearer) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Decoding consumer response.");
                }
                var httpResponse = createHttpResponse(bearer);
                var future = ctx.write(httpResponse, promise);
                if (!bearer.isConnectionKeepAlive()) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            } catch (Exception e) {
                var message = "An error occurred during consumer response serialization.";
                logger.error(message, e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, bearer.getProtocolVersion(), message);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private FullHttpResponse createHttpResponse(ConsumerResponseBearer bearer) throws JsonProcessingException {
        FullHttpResponse httpResponse;
        if (bearer.getResponse() == null) {
            httpResponse = new DefaultFullHttpResponse(bearer.getProtocolVersion(), bearer.getStatus());
        } else {
            if (bearer.getSerializeTo() == Serde.JSON) {
                var content = ConsumerResponseSerializer.serializeJson(objectMapper, bearer.getResponse());
                httpResponse = new DefaultFullHttpResponse(bearer.getProtocolVersion(), bearer.getStatus(), content);
                httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_JSON_CHARSET_UTF8);
            } else if (bearer.getSerializeTo() == Serde.BINARY) {
                var content = ConsumerResponseSerializer.serializeBinary(bearer.getResponse());
                httpResponse = new DefaultFullHttpResponse(bearer.getProtocolVersion(), bearer.getStatus(), content);
                httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_OCTET_STREAM);
            } else {
                throw new IllegalStateException("Unexpected serde: " + bearer.getSerializeTo());
            }
        }
        httpResponse.headers().setInt(HttpUtils.ASCII_CONTENT_LENGTH, httpResponse.content().readableBytes());
        var isKeepAliveDefault = bearer.getProtocolVersion().isKeepAliveDefault();
        if (bearer.isConnectionKeepAlive()) {
            if (!isKeepAliveDefault) {
                httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
        } else {
            if (isKeepAliveDefault) {
                httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.CLOSE);
            }
        }
        return httpResponse;
    }
}
