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

package pro.savel.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.ResponseBearer;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.producer.responses.ProducerResponse;

@ChannelHandler.Sharable
public class ProducerResponseEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ProducerResponseEncoder.class);

    private final ObjectMapper objectMapper;

    public ProducerResponseEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ResponseBearer bearer && bearer.response() instanceof ProducerResponse) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Decoding producer response.");
                }
                var httpResponse = createHttpResponse(bearer);
                var future = ctx.write(httpResponse, promise);
                if (!bearer.connectionKeepAlive()) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            } catch (Exception e) {
                var message = "An error occurred during producer response serialization.";
                logger.error(message, e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, bearer.protocolVersion(), message);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private FullHttpResponse createHttpResponse(ResponseBearer bearer) throws JsonProcessingException {
        var response = (ProducerResponse) bearer.response();
        FullHttpResponse httpResponse;
        if (bearer.response() == null) {
            httpResponse = new DefaultFullHttpResponse(bearer.protocolVersion(), bearer.status());
        } else {
            if (bearer.serializeTo() == Serde.JSON) {
                var content = ProducerResponseSerializer.serializeJson(objectMapper, response);
                httpResponse = new DefaultFullHttpResponse(bearer.protocolVersion(), bearer.status(), content);
                if (bearer.status() != HttpResponseStatus.NO_CONTENT)
                    httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_JSON_CHARSET_UTF8);
            } else if (bearer.serializeTo() == Serde.BINARY) {
                var content = ProducerResponseSerializer.serializeBinary(response);
                httpResponse = new DefaultFullHttpResponse(bearer.protocolVersion(), bearer.status(), content);
                if (bearer.status() != HttpResponseStatus.NO_CONTENT)
                    httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_OCTET_STREAM);
            } else {
                throw new IllegalStateException("Unexpected serde: " + bearer.serializeTo());
            }
        }
        httpResponse.headers().setInt(HttpUtils.ASCII_CONTENT_LENGTH, httpResponse.content().readableBytes());
        var isKeepAliveDefault = bearer.protocolVersion().isKeepAliveDefault();
        if (bearer.connectionKeepAlive()) {
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
