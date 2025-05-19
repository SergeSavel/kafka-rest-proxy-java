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

package pro.savel.kafka.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.admin.requests.*;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;

@ChannelHandler.Sharable
public class AdminRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/admin";
    private static final Logger logger = LoggerFactory.getLogger(AdminRequestDecoder.class);
    private final ObjectMapper objectMapper;

    public AdminRequestDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private static void passBearer(ChannelHandlerContext ctx, FullHttpRequest httpRequest, AdminRequest request) {
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding admin request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        var pathMethod = httpRequest.uri().substring(URI_PREFIX.length());
        switch (pathMethod) {
            case "" -> decodeRoot(ctx, httpRequest);
            case "/topic" -> decodeTopic(ctx, httpRequest);
            case "/topics" -> decodeTopics(ctx, httpRequest);
            case "/broker-config" -> decodeBrokerConfig(ctx, httpRequest);
            case "/cluster" -> decodeCluster(ctx, httpRequest);
            case "/touch" -> decodeTouch(ctx, httpRequest);
            default -> HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListRequest(ctx, httpRequest);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeJsonRequest(ctx, httpRequest, AdminCreateRequest.class);
        } else if (httpRequest.method() == HttpMethod.DELETE) {
            decodeJsonRequest(ctx, httpRequest, AdminRemoveRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeJsonRequest(ctx, httpRequest, AdminTouchRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeCluster(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeJsonRequest(ctx, httpRequest, AdminDescribeClusterRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeJsonRequest(ctx, httpRequest, AdminListTopicsRequest.class);
        } else if (httpRequest.method() == HttpMethod.POST) {
            decodeJsonRequest(ctx, httpRequest, AdminCreateTopicRequest.class);
        } else if (httpRequest.method() == HttpMethod.DELETE) {
            decodeJsonRequest(ctx, httpRequest, AdminDeleteTopicRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeTopic(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeJsonRequest(ctx, httpRequest, AdminDescribeTopicRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeBrokerConfig(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeJsonRequest(ctx, httpRequest, AdminDescribeBrokerConfigRequest.class);
        } else {
            throw new BadRequestException("Unsupported HTTP method.");
        }
    }

    private void decodeListRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new AdminListRequest();
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private <T extends AdminRequest> void decodeJsonRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, Class<T> clazz) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        T request;
        if (HttpUtils.isJson(contentType))
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), clazz);
        else
            throw new BadRequestException("Invalid Content-Type header in request.");
        passBearer(ctx, httpRequest, request);
    }

}
