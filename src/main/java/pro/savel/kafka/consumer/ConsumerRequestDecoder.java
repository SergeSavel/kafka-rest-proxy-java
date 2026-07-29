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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.MethodNotAllowedException;
import pro.savel.kafka.consumer.requests.*;

import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
public class ConsumerRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/consumer";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestDecoder.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;

    public ConsumerRequestDecoder(ObjectMapper objectMapper, ValidatorFactory validatorFactory) {
        this.objectMapper = objectMapper;
        this.validator = validatorFactory == null ? null : validatorFactory.getValidator();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (MethodNotAllowedException e) {
                HttpUtils.writeMethodNotAllowedAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding consumer request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
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

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        var decoder = new QueryStringDecoder(httpRequest.uri(), StandardCharsets.UTF_8, true);
        var pathMethod = decoder.path().substring(URI_PREFIX.length());
        switch (pathMethod) {
            case "/poll" -> decodePoll(ctx, httpRequest);
            case "/commit" -> decodeCommit(ctx, httpRequest);
            case "/seek" -> decodeSeek(ctx, httpRequest);
            case "/seek-to-beginning" -> decodeSeekToBeginning(ctx, httpRequest);
            case "/seek-to-end" -> decodeSeekToEnd(ctx, httpRequest);
            case "/get-position" -> decodeGetPosition(ctx, httpRequest);
            case "/assign" -> decodeAssign(ctx, httpRequest);
            case "/get-assignment" -> decodeGetAssignment(ctx, httpRequest);
            case "/subscribe" -> decodeSubscribe(ctx, httpRequest);
            case "/unsubscribe" -> decodeUnsubscribe(ctx, httpRequest);
            case "/get-subscription" -> decodeGetSubscription(ctx, httpRequest);
            case "/get-partitions" -> decodeGetPartitions(ctx, httpRequest);
            case "/list-partitions" -> decodeListPartitions(ctx, httpRequest);
            case "/list-topics" -> decodeListTopics(ctx, httpRequest);
            case "/get-group-metadata" -> decodeGetGroupMetadata(ctx, httpRequest);
            case "/get-committed" -> decodeGetCommitted(ctx, httpRequest);
            case "/get-beginning-offsets" -> decodeGetBeginningOffsets(ctx, httpRequest);
            case "/get-end-offsets" -> decodeGetEndOffsets(ctx, httpRequest);
            case "/touch" -> decodeTouch(ctx, httpRequest);
            case "/create" -> decodeCreate(ctx, httpRequest);
            case "/release" -> decodeRelease(ctx, httpRequest);
            case "" -> decodeList(ctx, httpRequest);
            default -> HttpUtils.writeNotFoundAndClose(ctx);
        }
    }

    private void decodeList(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.GET)
            decodeListRequest(ctx, httpRequest);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeCreate(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerCreateRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeRelease(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerReleaseRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerTouchRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeAssign(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerAssignRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetAssignment(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetAssignmentRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeSeek(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerSeekRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeSeekToBeginning(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerSeekToBeginningRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeSeekToEnd(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerSeekToEndRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetPosition(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetPositionRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeSubscribe(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerSubscribeRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeUnsubscribe(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerUnsubscribeRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetSubscription(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetSubscriptionRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeListTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerListTopicsRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetPartitions(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetPartitionsRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    @Deprecated
    private void decodeListPartitions(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerListPartitionsRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetGroupMetadata(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetGroupMetadataRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetCommitted(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetCommittedRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetBeginningOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetBeginningOffsetsRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeGetEndOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerGetEndOffsetsRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodePoll(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerPollRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeCommit(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST)
            decodeRequest(ctx, httpRequest, ConsumerCommitRequest.class);
        else
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeListRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new ConsumerListRequest();
        passBearer(ctx, httpRequest, request);
    }

    private <T extends ConsumerRequest> void decodeRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, Class<T> clazz) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        T request;
        if (HttpUtils.isJson(contentType))
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), clazz);
        else
            throw new BadRequestException("Invalid Content-Type header in request.");
        if (validator != null) {
            var violations = validator.validate(request);
            if (!violations.isEmpty()) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineConstraintViolationMessage(violations));
                return;
            }
        }
        passBearer(ctx, httpRequest, request);
    }
}
