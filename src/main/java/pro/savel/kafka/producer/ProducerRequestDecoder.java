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
    private static final int URI_PREFIX_LENGTH = URI_PREFIX.length();

    private static final String REGEX_UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    //private static final Pattern PATTERN_ROOT = Pattern.compile("^/?$");
    private static final Pattern PATTERN_PRODUCER = Pattern.compile("^/(" + REGEX_UUID + ")");

    private final ObjectMapper objectMapper;

    public ProducerRequestDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {

        var uri = httpRequest.uri();
        assert uri.startsWith(URI_PREFIX) : "URI must start with '" + URI_PREFIX + "'";
        uri = uri.substring(URI_PREFIX_LENGTH);

        if (uri.isEmpty() || "/".equals(uri)) {
            decodeRoot(ctx, httpRequest);
            return;
        }

        var producerMatcher = PATTERN_PRODUCER.matcher(uri);
        if (producerMatcher.find()) {
            uri = uri.substring(producerMatcher.group().length());
            var producerId = UUID.fromString(producerMatcher.group(1));

            if (uri.isEmpty() || "/".equals(uri)) {
                decodeProducerRoot(ctx, httpRequest, producerId);
                return;
            }

            if ("/touch".equals(uri)) {
                decodeProducerTouch(ctx, httpRequest, producerId);
                return;
            }
        }

        HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
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

    private void decodeProducerRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest, UUID producerId) {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeGetProducerRequest(ctx, httpRequest, producerId);
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

    private <T> T parseJson(ByteBuf byteBuf, Class<T> clazz) throws IOException {
        T result;
        try (var inputStream = new ByteBufInputStream(byteBuf)) {
            result = objectMapper.readValue((InputStream) inputStream, clazz);
        }
        return result;
    }
}
