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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.consumer.requests.*;

@ChannelHandler.Sharable
public class ConsumerRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/consumer";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestDecoder.class);

    private final ObjectMapper objectMapper;

    public ConsumerRequestDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding consumer request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, httpRequest.protocolVersion(), Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private static void passBearer(ChannelHandlerContext ctx, FullHttpRequest httpRequest, ConsumerRequest request) {
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        var pathMethod = httpRequest.uri().substring(URI_PREFIX.length());
        switch (pathMethod) {
            case "" -> decodeRoot(ctx, httpRequest);
            case "/poll" -> decodePoll(ctx, httpRequest);
            case "/commit" -> decodeCommit(ctx, httpRequest);
            case "/position" -> decodeSeek(ctx, httpRequest);
            case "/assignment" -> decodeAssign(ctx, httpRequest);
            case "/subscription" -> decodeSubscribe(ctx, httpRequest);
            case "/partitions" -> decodePartitions(ctx, httpRequest);
            case "/topics" -> decodeTopics(ctx, httpRequest);
            case "/beginning" -> decodeBeginning(ctx, httpRequest);
            case "/end" -> decodeEnd(ctx, httpRequest);
            case "/touch" -> decodeTouch(ctx, httpRequest);
            default -> HttpUtils.writeNotFoundAndClose(ctx, httpRequest.protocolVersion());
        }
    }

    private void decodeRoot(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeListRequest(ctx, httpRequest);
        else if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerCreateRequest.class);
        else if (httpRequest.method() == HttpMethod.DELETE)
            decodeJsonRequest(ctx, httpRequest, ConsumerRemoveRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerTouchRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeAssign(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetAssignmentRequest.class);
        else if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerAssignRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeSeek(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetPositionRequest.class);
        else if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerSeekRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeSubscribe(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetSubscriptionRequest.class);
        else if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerSubscribeRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetTopicsRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodePartitions(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetPartitionsRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeBeginning(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetBeginningOffsetsRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeEnd(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeJsonRequest(ctx, httpRequest, ConsumerGetEndOffsetsRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodePoll(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerPollRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeCommit(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeJsonRequest(ctx, httpRequest, ConsumerCommitRequest.class);
        else
            throw new BadRequestException("Unsupported HTTP method.");
    }

    private void decodeListRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new ConsumerListRequest();
        passBearer(ctx, httpRequest, request);
    }

    private <T extends ConsumerRequest> void decodeJsonRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, Class<T> clazz) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        T request;
        if (HttpUtils.isJson(contentType))
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), clazz);
        else
            throw new BadRequestException("Invalid Content-Type header in request.");
        passBearer(ctx, httpRequest, request);
    }
}
