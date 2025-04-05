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
import pro.savel.kafka.producer.requests.*;

import java.util.UUID;
import java.util.regex.Pattern;

@ChannelHandler.Sharable
public class ProducerRequestDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ProducerRequestDecoder.class);

    public static final String URI_PREFIX = "/producer";

    private static final String REGEX_PRODUCER =
            "^/producer(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(/[a-z]+)?)?$";

    private static final Pattern PATTERN_PRODUCER = Pattern.compile(REGEX_PRODUCER);

    private final ObjectMapper objectMapper;

    public ProducerRequestDecoder(ObjectMapper objectMapper) {
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
                String message = "An unexpected error occurred while decoding producer request.";
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

        var producerMatcher = PATTERN_PRODUCER.matcher(httpRequest.uri());
        if (producerMatcher.matches()) {
            if (producerMatcher.group(1) == null) {
                decodeRoot(ctx, httpRequest);
                return;
            }
            var producerId = UUID.fromString(producerMatcher.group(2));
            var pathMethod = producerMatcher.group(3);
            if (pathMethod == null) {
                decodeProducerRoot(ctx, httpRequest, producerId);
                return;
            }
            if ("/produce".equals(pathMethod)) {
                decodeProducerProduce(ctx, httpRequest, producerId);
                return;
            }
            if ("/touch".equals(pathMethod)) {
                decodeProducerTouch(ctx, httpRequest, producerId);
                return;
            }
        }
        HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListProducersRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeCreateProducerRequest(ctx, httpRequest);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeProducerRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeGetProducerRequest(ctx, httpRequest, producerId);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeProduceRequest(ctx, httpRequest, producerId);
        } else if (httpRequest.method() == HttpMethod.DELETE) {
            decodeRemoveProducerRequest(ctx, httpRequest, producerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeProducerTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.POST || httpRequest.method() == HttpMethod.PUT) {
            decodeTouchProducerRequest(ctx, httpRequest, producerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeProducerProduce(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeProduceRequest(ctx, httpRequest, producerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeListProducersRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new ListProducersRequest();
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeGetProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerUuid) {
        var request = new GetProducerRequest(producerUuid);
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeCreateProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        CreateProducerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), CreateProducerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeTouchProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        TouchProducerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), TouchProducerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        request.setId(producerId);
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeRemoveProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {

        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        RemoveProducerRequest request;
        if (HttpUtils.isJson(contentType)) {
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), RemoveProducerRequest.class);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        request.setId(producerId);
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeProduceRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) throws DeserializeJsonException {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        ProduceRequest request;
        if (HttpUtils.isJson(contentType)) {
            var stringRequest = JsonUtils.parseJson(objectMapper, httpRequest.content(), ProduceStringRequest.class);
            request = ProducerMapper.mapProduceRequest(stringRequest, producerId);
        } else {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }
}
