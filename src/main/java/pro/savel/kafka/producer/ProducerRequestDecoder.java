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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.producer.contract.CreateProducerRequest;
import pro.savel.kafka.producer.contract.ListProducersRequest;

import java.io.IOException;
import java.io.InputStream;

public class ProducerRequestDecoder {
    public static final String URI_PREFIX = "/producer";
    private static final int URI_PREFIX_LENGTH = URI_PREFIX.length();

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws IOException {
        var uri = httpRequest.uri();
        assert uri.startsWith(URI_PREFIX) : "URI must start with '" + URI_PREFIX + "'";
        uri = uri.substring(URI_PREFIX_LENGTH);
        if (uri.isEmpty() || "/".equals(uri)) {
            decodeRoot(ctx, httpRequest);
        } else {
            HttpUtils.writeNotFound(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws IOException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListProducersRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeCreateProducerRequest(ctx, httpRequest);
        } else {
            HttpUtils.writeMethodNotAllowed(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeListProducersRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {

        var request = new ListProducersRequest();

        var bearer = new RequestBearer(httpRequest, request);
        ctx.writeAndFlush(bearer);
    }

    private void decodeCreateProducerRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws IOException {

        var contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null) {
            HttpUtils.writeBadRequest(ctx, httpRequest.protocolVersion(), "Missing 'Content-Type' header");
            return;
        }
        CreateProducerRequest request;
        if (HttpUtils.isJson(contentType)) {
            try (var inputStream = new ByteBufInputStream(httpRequest.content())) {
                request = objectMapper.readValue((InputStream) inputStream, CreateProducerRequest.class);
            }
        } else {
            HttpUtils.writeBadRequest(ctx, httpRequest.protocolVersion(), "Invalid 'Content-Type' header");
            return;
        }

        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }
}
