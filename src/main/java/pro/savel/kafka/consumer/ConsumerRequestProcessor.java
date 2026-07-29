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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.*;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.consumer.requests.*;
import pro.savel.kafka.consumer.responses.*;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

@ChannelHandler.Sharable
public class ConsumerRequestProcessor extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestProcessor.class);

    private final ConsumerProvider provider = new ConsumerProvider();
    private final BlockingTaskExecutor blockingTaskExecutor;

    public ConsumerRequestProcessor(BlockingTaskExecutor blockingTaskExecutor) {
        this.blockingTaskExecutor = blockingTaskExecutor;
    }

//region Overrides

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && bearer.request() instanceof ConsumerRequest) {
            try {
                processRequest(ctx, bearer);
            } catch (Exception e) {
                if (!handleError(ctx, e)) {
                    logger.error("An unexpected error occurred while processing consumer request.", e);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void close() {
        provider.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("An error occurred while processing consumer request.", cause);
        ctx.close();
    }

//endregion

    private void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var requestClass = requestBearer.request().getClass();
        if (requestClass == ConsumerPollRequest.class)
            processPoll(ctx, requestBearer);
        else if (requestClass == ConsumerCommitRequest.class)
            processCommit(ctx, requestBearer);
        else if (requestClass == ConsumerSeekRequest.class)
            processSeek(ctx, requestBearer);
        else if (requestClass == ConsumerSeekToBeginningRequest.class)
            processSeekToBeginning(ctx, requestBearer);
        else if (requestClass == ConsumerSeekToEndRequest.class)
            processSeekToEnd(ctx, requestBearer);
        else if (requestClass == ConsumerGetPartitionsRequest.class)
            processGetPartitions(ctx, requestBearer);
        else if (requestClass == ConsumerListPartitionsRequest.class)
            processListPartitions(ctx, requestBearer);
        else if (requestClass == ConsumerAssignRequest.class)
            processAssign(ctx, requestBearer);
        else if (requestClass == ConsumerSubscribeRequest.class)
            processSubscribe(ctx, requestBearer);
        else if (requestClass == ConsumerGetBeginningOffsetsRequest.class)
            processGetBeginningOffsets(ctx, requestBearer);
        else if (requestClass == ConsumerGetEndOffsetsRequest.class)
            processGetEndOffsets(ctx, requestBearer);
        else if (requestClass == ConsumerListTopicsRequest.class)
            processListTopics(ctx, requestBearer);
        else if (requestClass == ConsumerGetPositionRequest.class)
            processGetPosition(ctx, requestBearer);
        else if (requestClass == ConsumerGetAssignmentRequest.class)
            processGetAssignment(ctx, requestBearer);
        else if (requestClass == ConsumerGetSubscriptionRequest.class)
            processGetSubscription(ctx, requestBearer);
        else if (requestClass == ConsumerCreateRequest.class)
            processCreate(ctx, requestBearer);
        else if (requestClass == ConsumerReleaseRequest.class)
            processRemove(ctx, requestBearer);
        else if (requestClass == ConsumerListRequest.class)
            processList(ctx, requestBearer);
        else if (requestClass == ConsumerTouchRequest.class)
            processTouch(ctx, requestBearer);
        else
            throw new RuntimeException("Unexpected consumer request type: " + requestClass.getName());
    }

//region Management

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = ConsumerListResponse.of(wrappers);
        var responseBearer = new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerCreateRequest) requestBearer.request();
        var owner = ctx.channel().attr(NettyAttributes.USERNAME).get();
        execute(ctx, () -> {
            var wrapper = provider.createConsumer(request.getName(), request.getConfig(), request.getExpirationTimeout(), owner);
            var response = ConsumerCreateResponse.of(wrapper);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        });
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerReleaseRequest) requestBearer.request();
        execute(ctx, () -> {
            provider.removeConsumer(request.getConsumerId(), request.getToken());
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerTouchRequest) requestBearer.request();
        var wrapper = provider.getConsumer(request.getConsumerId(), request.getToken());
        wrapper.touch();
        var responseBearer = new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

//endregion

//region Consumer

    private void processPoll(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerPollRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var records = consumer.poll(Duration.ofMillis(request.getTimeout()));
            var response = ConsumerResponseMapper.mapPollResponse(records);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processCommit(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerCommitRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.commitSync();
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processAssign(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerAssignRequest) requestBearer.request();
        var assignment = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.assign(assignment);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processGetAssignment(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetAssignmentRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var assignment = consumer.assignment();
            var response = ConsumerAssignmentResponse.of(assignment);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processSeek(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekRequest) requestBearer.request();
        var topicPartition = new TopicPartition(request.getTopic(), request.getPartition());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seek(topicPartition, request.getOffset());
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processSeekToBeginning(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekToBeginningRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seekToBeginning(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processSeekToEnd(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekToEndRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seekToEnd(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processSubscribe(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSubscribeRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            if (request.getTopics() != null)
                consumer.subscribe(request.getTopics());
            else if (request.getPattern() != null)
                consumer.subscribe(new SubscriptionPattern(request.getPattern()));
            else
                throw new IllegalArgumentException("Topic list or pattern must be specified");
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        });
    }

    private void processGetSubscription(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetSubscriptionRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var subscription = consumer.subscription();
            var response = ConsumerSubscriptionResponse.of(subscription);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processGetPosition(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetPositionRequest) requestBearer.request();
        var topicPartition = new TopicPartition(request.getTopic(), request.getPartition());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var position = consumer.position(topicPartition);
            var response = ConsumerResponseMapper.mapPositionResponse(position);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processGetPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetPartitionsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var partitions = consumer.partitionsFor(request.getTopic());
            var response = ConsumerPartitionsResponse.of(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    @Deprecated
    private void processListPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerListPartitionsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var partitions = consumer.partitionsFor(request.getTopic());
            var response = ConsumerResponseMapper.mapPartitionsResponse(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processGetBeginningOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetBeginningOffsetsRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var offsets = consumer.beginningOffsets(partitions);
            var response = ConsumerOffsetsResponse.of(offsets);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processGetEndOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetEndOffsetsRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var offsets = consumer.endOffsets(partitions);
            var response = ConsumerOffsetsResponse.of(offsets);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerListTopicsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var topics = consumer.listTopics();
            if (request.getPattern() != null) {
                Pattern pattern;
                try {
                    pattern = Pattern.compile(request.getPattern());
                } catch (PatternSyntaxException e) {
                    throw new BadRequestException("Invalid pattern.", e);
                }
                topics = topics.entrySet().stream()
                        .filter(e -> pattern.matcher(e.getKey()).matches())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            var response = ConsumerTopicsResponse.of(topics);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        });
    }

//endregion

    private org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> getConsumer(String id, String token) {
        var wrapper = provider.getConsumer(id, token);
        wrapper.touch();
        return wrapper.getConsumer();
    }

    private void execute(ChannelHandlerContext ctx, Supplier<ConsumerResponseBearer> operation) {
        blockingTaskExecutor.execute(ctx, operation::get, (response, error) -> {
            if (error == null) {
                ctx.writeAndFlush(response);
            } else if (!handleError(ctx, error)) {
                logger.error("An unexpected error occurred while processing consumer request.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            }
        });
    }

    private static boolean handleError(ChannelHandlerContext ctx, Throwable error) {
        var handled = true;
        if (error instanceof java.util.concurrent.CompletionException && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (error instanceof org.apache.kafka.common.errors.TimeoutException && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (error instanceof InvalidOffsetException e)
            HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(e));
        else if (!CommonErrors.handle(ctx, error))
            handled = false;
        return handled;
    }
}
