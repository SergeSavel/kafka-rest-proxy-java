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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.producer.requests.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.regex.Pattern;

public class ProducerRequestDecoder {

    public static final String URI_PREFIX = "/producer";

    private static final String REGEX_PRODUCER =
            "^/producer(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(/[a-z]+)?)?$";

    private static final Pattern PATTERN_PRODUCER = Pattern.compile(REGEX_PRODUCER);

    private final ObjectMapper objectMapper;

    public ProducerRequestDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListProducersRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeCreateProducerRequest(ctx, httpRequest);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    public void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
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

    private void decodeProducerRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {
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

    private void decodeProducerTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {
        if (httpRequest.method() == HttpMethod.POST || httpRequest.method() == HttpMethod.PUT) {
            decodeTouchProducerRequest(ctx, httpRequest, producerId);
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

    private void decodeCreateProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        CreateProducerRequest request;
        try {
            if (HttpUtils.isJson(contentType)) {
                request = parseJson(httpRequest.content(), CreateProducerRequest.class);
            } else {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
                return;
            }
        } catch (IOException e) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid content");
            return;
        }
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeTouchProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {

        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }

        TouchProducerRequest request;
        try {
            if (HttpUtils.isJson(contentType)) {
                request = parseJson(httpRequest.content(), TouchProducerRequest.class);
            } else {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
                return;
            }
        } catch (IOException e) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid content");
            return;
        }
        request.setId(producerId);

        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeRemoveProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {

        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }

        RemoveProducerRequest request;
        try {
            if (HttpUtils.isJson(contentType)) {
                request = parseJson(httpRequest.content(), RemoveProducerRequest.class);
            } else {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
                return;
            }
        } catch (IOException e) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid content");
            return;
        }
        request.setId(producerId);

        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decodeProducerProduce(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeProduceRequest(ctx, httpRequest, producerId);
        } else {
            HttpUtils.writeMethodNotAllowedAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private <T> T parseJson(ByteBuf byteBuf, Class<T> clazz) throws IOException {
        T result;
        try (var inputStream = new ByteBufInputStream(byteBuf)) {
            result = objectMapper.readValue((InputStream) inputStream, clazz);
        }
        return result;
    }

    private void decodeProduceRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {
        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        ProduceRequest request;
        try {
            if (HttpUtils.isJson(contentType)) {
                var stringRequest = parseJson(httpRequest.content(), ProduceStringRequest.class);
                request = ProducerMapper.mapProduceRequest(stringRequest, producerId);
            } else {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
                return;
            }
        } catch (IOException e) {
            HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), "Invalid content");
            return;
        }

        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }
}
