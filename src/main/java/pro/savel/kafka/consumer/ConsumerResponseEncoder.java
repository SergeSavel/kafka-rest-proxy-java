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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.consumer.responses.ConsumerPollResponse;

import java.io.IOException;

@ChannelHandler.Sharable
public class ConsumerResponseEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerResponseEncoder.class);

    private final ObjectMapper objectMapper;
    private final int responseChunkBytes;

    public ConsumerResponseEncoder(ObjectMapper objectMapper, int responseChunkBytes) {
        if (responseChunkBytes <= 0)
            throw new IllegalArgumentException("responseChunkBytes must be greater than 0");
        this.objectMapper = objectMapper;
        this.responseChunkBytes = responseChunkBytes;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ConsumerResponseBearer bearer) {
            try {
                if (bearer.getResponse() instanceof ConsumerPollResponse pollResponse)
                    writePollResponse(ctx, bearer, pollResponse, promise);
                else if (bearer.getResponse() != null && bearer.getSerializeTo() == Serde.BINARY) {
                    promise.setSuccess();
                    HttpUtils.writeNotAcceptableAndClose(ctx, "Binary response format is not supported.");
                } else
                    writeFullResponse(ctx, bearer, promise);
            } catch (Exception e) {
                var message = "An error occurred during consumer response serialization.";
                logger.error(message, e);
                promise.setFailure(e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, message);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private void writeFullResponse(ChannelHandlerContext ctx, ConsumerResponseBearer bearer, ChannelPromise promise)
            throws IOException {
        var httpResponse = createHttpResponse(ctx, bearer);
        var future = ctx.write(httpResponse, promise);
        if (!bearer.isConnectionKeepAlive())
            future.addListener(ChannelFutureListener.CLOSE);
    }

    private void writePollResponse(ChannelHandlerContext ctx, ConsumerResponseBearer bearer,
                                   ConsumerPollResponse pollResponse, ChannelPromise promise) {
        var httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, bearer.getStatus());
        if (bearer.getSerializeTo() == Serde.JSON)
            httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_JSON_CHARSET_UTF8);
        else if (bearer.getSerializeTo() == Serde.BINARY)
            httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_OCTET_STREAM);
        else
            throw new IllegalStateException("Unexpected serde: " + bearer.getSerializeTo());
        // Canonical-case header names: Netty's own constants are lowercase, which an RFC-legal
        // client accepts but a case-sensitive one does not. The encoder matches names
        // case-insensitively, so the chunked framing still applies.
        httpResponse.headers().set(HttpUtils.ASCII_TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        if (!bearer.isConnectionKeepAlive())
            httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.CLOSE);

        ctx.write(httpResponse);
        var input = new ConsumerPollChunkedInput(
                objectMapper, pollResponse, bearer.getSerializeTo(), responseChunkBytes);
        var future = ctx.write(new HttpChunkedInput(input), promise.unvoid());
        future.addListener(result -> {
            if (!result.isSuccess()) {
                logger.error("Failed to write poll response.", result.cause());
                ctx.close();
            } else {
                if (logger.isDebugEnabled() && bearer.getSerializeTo() == Serde.BINARY)
                    logger.debug("Poll response written: {} messages, {} of {} bytes.",
                            pollResponse.size(), input.progress(),
                            ConsumerResponseSerializer.calculatePollBinarySize(pollResponse));
                if (!bearer.isConnectionKeepAlive())
                    ctx.close();
            }
        });
    }

    private FullHttpResponse createHttpResponse(ChannelHandlerContext ctx, ConsumerResponseBearer bearer)
            throws IOException {
        FullHttpResponse httpResponse;
        if (bearer.getResponse() == null) {
            httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, bearer.getStatus());
        } else {
            if (bearer.getSerializeTo() == Serde.JSON) {
                var content = ConsumerResponseSerializer.serializeJson(objectMapper, ctx.alloc(), bearer.getResponse());
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, bearer.getStatus(), content);
                httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_JSON_CHARSET_UTF8);
            } else if (bearer.getSerializeTo() == Serde.BINARY) {
                var content = ConsumerResponseSerializer.serializeBinary(ctx.alloc(), bearer.getResponse());
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, bearer.getStatus(), content);
                httpResponse.headers().set(HttpUtils.ASCII_CONTENT_TYPE, HttpUtils.ASCII_APPLICATION_OCTET_STREAM);
            } else {
                throw new IllegalStateException("Unexpected serde: " + bearer.getSerializeTo());
            }
        }
        httpResponse.headers().setInt(HttpUtils.ASCII_CONTENT_LENGTH, httpResponse.content().readableBytes());
        if (!bearer.isConnectionKeepAlive()) {
            httpResponse.headers().set(HttpUtils.ASCII_CONNECTION, HttpHeaderValues.CLOSE);
        }
        return httpResponse;
    }
}
