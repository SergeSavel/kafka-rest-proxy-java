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
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
public class ConsumerRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/consumer";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestDecoder.class);

    @SuppressWarnings("deprecation")
    private static final Map<String, Class<? extends ConsumerRequest>> REQUEST_TYPES = Map.ofEntries(
            Map.entry("/create", ConsumerCreateRequest.class),
            Map.entry("/release", ConsumerReleaseRequest.class),
            Map.entry("/touch", ConsumerTouchRequest.class),
            Map.entry("/poll", ConsumerPollRequest.class),
            Map.entry("/commit", ConsumerCommitRequest.class),
            Map.entry("/seek", ConsumerSeekRequest.class),
            Map.entry("/seek-to-beginning", ConsumerSeekToBeginningRequest.class),
            Map.entry("/seek-to-end", ConsumerSeekToEndRequest.class),
            Map.entry("/get-position", ConsumerGetPositionRequest.class),
            Map.entry("/assign", ConsumerAssignRequest.class),
            Map.entry("/get-assignment", ConsumerGetAssignmentRequest.class),
            Map.entry("/subscribe", ConsumerSubscribeRequest.class),
            Map.entry("/unsubscribe", ConsumerUnsubscribeRequest.class),
            Map.entry("/get-subscription", ConsumerGetSubscriptionRequest.class),
            Map.entry("/get-partitions", ConsumerGetPartitionsRequest.class),
            Map.entry("/list-partitions", ConsumerListPartitionsRequest.class), // deprecated
            Map.entry("/list-topics", ConsumerListTopicsRequest.class),
            Map.entry("/get-group-metadata", ConsumerGetGroupMetadataRequest.class),
            Map.entry("/get-committed", ConsumerGetCommittedRequest.class),
            Map.entry("/get-beginning-offsets", ConsumerGetBeginningOffsetsRequest.class),
            Map.entry("/get-end-offsets", ConsumerGetEndOffsetsRequest.class)
    );

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final long maxPollTimeoutMs;

    /**
     * @param maxPollTimeoutSeconds the longest poll a client may request. Derived from the read
     *                              timeout: a poll holds the connection with reading suspended, so
     *                              ClientReadTimeoutHandler exempts it from that timeout, and this
     *                              cap is what replaces the bound it would otherwise impose.
     */
    public ConsumerRequestDecoder(ObjectMapper objectMapper, ValidatorFactory validatorFactory, int maxPollTimeoutSeconds) {
        if (maxPollTimeoutSeconds <= 0)
            throw new IllegalArgumentException("maxPollTimeoutSeconds must be greater than 0");
        this.objectMapper = objectMapper;
        this.validator = validatorFactory == null ? null : validatorFactory.getValidator();
        this.maxPollTimeoutMs = TimeUnit.SECONDS.toMillis(maxPollTimeoutSeconds);
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
        if (pathMethod.isEmpty()) {
            requireMethod(httpRequest, HttpMethod.GET);
            decodeListRequest(ctx, httpRequest);
            return;
        }
        var requestType = REQUEST_TYPES.get(pathMethod);
        if (requestType == null) {
            HttpUtils.writeNotFoundAndClose(ctx);
            return;
        }
        requireMethod(httpRequest, HttpMethod.POST);
        decodeRequest(ctx, httpRequest, requestType);
    }

    private static void requireMethod(FullHttpRequest httpRequest, HttpMethod method) throws MethodNotAllowedException {
        if (httpRequest.method() != method)
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
        if (request instanceof ConsumerPollRequest pollRequest && pollRequest.getTimeout() > maxPollTimeoutMs)
            throw new BadRequestException("Poll timeout must not exceed " + maxPollTimeoutMs + " ms.");
        passBearer(ctx, httpRequest, request);
    }
}
