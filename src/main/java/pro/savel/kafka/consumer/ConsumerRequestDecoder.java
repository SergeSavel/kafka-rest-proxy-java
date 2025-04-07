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

package pro.savel.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.common.exceptions.DeserializeJsonException;
import pro.savel.kafka.consumer.requests.*;

import java.util.UUID;
import java.util.regex.Pattern;

@ChannelHandler.Sharable
public class ConsumerRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/consumer";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestDecoder.class);
    private static final String REGEX_CONSUMER =
            "^/consumer(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(/[a-z]+)?)?$";

    private static final Pattern PATTERN_CONSUMER = Pattern.compile(REGEX_CONSUMER);

    private final ObjectMapper objectMapper;

    public ConsumerRequestDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (DeserializeJsonException e) {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid request content.");
            } catch (Exception e) {
                String message = "An unexpected error occurred while decoding consumer request.";
                logger.error(message, e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, httpRequest.protocolVersion(), message);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws DeserializeJsonException {

        var producerMatcher = PATTERN_CONSUMER.matcher(httpRequest.uri());
        if (producerMatcher.matches()) {
            if (producerMatcher.group(1) == null) {
                decodeRoot(ctx, httpRequest);
                return;
            }
            var consumerId = UUID.fromString(producerMatcher.group(2));
            var pathMethod = producerMatcher.group(3);
            if (pathMethod == null) {
                decodeConsumerRoot(ctx, httpRequest, consumerId);
                return;
            }
//            if ("/produce".equals(pathMethod)) {
//                decodeProducerProduce(ctx, httpRequest, producerId);
//                return;
//            }
            if ("/touch".equals(pathMethod)) {
                decodeConsumerTouch(ctx, httpRequest, consumerId);
                return;
            }
        }
        HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListConsumersRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeCreateConsumerRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.DELETE) {
            decodeRemoveConsumerRequest(ctx, httpRequest, null);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeConsumerRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID consumerId) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeGetConsumerRequest(ctx, httpRequest, consumerId);
        } else if (httpRequest.method() == HttpMethod.DELETE) {
            decodeRemoveConsumerRequest(ctx, httpRequest, consumerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeConsumerTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID consumerId) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.POST || httpRequest.method() == HttpMethod.PUT) {
            decodeTouchConsumerRequest(ctx, httpRequest, consumerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeListConsumersRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new ListConsumersRequest();
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeGetConsumerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID consumerId) {
        var request = new GetConsumerRequest(consumerId);
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeCreateConsumerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        CreateConsumerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), CreateConsumerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeRemoveConsumerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID consumerId) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        RemoveConsumerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), RemoveConsumerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        if (consumerId != null) {
            if (request.getId() == null) {
                request.setId(consumerId);
            }
            if (!consumerId.equals(request.getId())) {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid Consumer Id.");
                return;
            }
        }
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeTouchConsumerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID consumerId) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        TouchConsumerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), TouchConsumerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        request.setId(consumerId);
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

}
