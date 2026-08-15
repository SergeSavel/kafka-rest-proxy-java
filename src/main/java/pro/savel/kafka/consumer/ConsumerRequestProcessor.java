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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.TopicPartition;
import pro.savel.kafka.common.*;
import pro.savel.kafka.consumer.requests.*;
import pro.savel.kafka.consumer.responses.*;

import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class ConsumerRequestProcessor extends AbstractRequestProcessor {

    private final ConsumerProvider provider;

    public ConsumerRequestProcessor(BlockingTaskExecutor blockingTaskExecutor, ConsumerProvider provider) {
        super("consumer", ConsumerRequest.class, blockingTaskExecutor);
        this.provider = provider;
    }

    @Override
    protected boolean handleSpecificError(ChannelHandlerContext ctx, Throwable error) {
        if (error instanceof InvalidOffsetException e) {
            HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(e));
            return true;
        }
        return false;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        switch (requestBearer.request()) {
            case ConsumerPollRequest ignored -> processPoll(ctx, requestBearer);
            case ConsumerCommitRequest ignored -> processCommit(ctx, requestBearer);
            case ConsumerSeekRequest ignored -> processSeek(ctx, requestBearer);
            case ConsumerSeekToBeginningRequest ignored -> processSeekToBeginning(ctx, requestBearer);
            case ConsumerSeekToEndRequest ignored -> processSeekToEnd(ctx, requestBearer);
            case ConsumerGetPartitionsRequest ignored -> processGetPartitions(ctx, requestBearer);
            case ConsumerListPartitionsRequest ignored -> processListPartitions(ctx, requestBearer);
            case ConsumerAssignRequest ignored -> processAssign(ctx, requestBearer);
            case ConsumerSubscribeRequest ignored -> processSubscribe(ctx, requestBearer);
            case ConsumerUnsubscribeRequest ignored -> processUnsubscribe(ctx, requestBearer);
            case ConsumerGetGroupMetadataRequest ignored -> processGetGroupMetadata(ctx, requestBearer);
            case ConsumerGetCommittedRequest ignored -> processGetCommitted(ctx, requestBearer);
            case ConsumerGetBeginningOffsetsRequest ignored -> processGetBeginningOffsets(ctx, requestBearer);
            case ConsumerGetEndOffsetsRequest ignored -> processGetEndOffsets(ctx, requestBearer);
            case ConsumerListTopicsRequest ignored -> processListTopics(ctx, requestBearer);
            case ConsumerGetPositionRequest ignored -> processGetPosition(ctx, requestBearer);
            case ConsumerGetAssignmentRequest ignored -> processGetAssignment(ctx, requestBearer);
            case ConsumerGetSubscriptionRequest ignored -> processGetSubscription(ctx, requestBearer);
            case ConsumerCreateRequest ignored -> processCreate(ctx, requestBearer);
            case ConsumerReleaseRequest ignored -> processRemove(ctx, requestBearer);
            case ConsumerListRequest ignored -> processList(ctx, requestBearer);
            case ConsumerTouchRequest ignored -> processTouch(ctx, requestBearer);
            default ->
                    throw new RuntimeException("Unexpected consumer request type: " + requestBearer.request().getClass().getName());
        }
    }

    // region Management

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
            var wrapper = provider.createConsumer(request.getName(), request.getConfig(),
                    request.getExpirationTimeout(), owner);
            var response = ConsumerCreateResponse.of(wrapper);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        }, ctx::writeAndFlush);
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerReleaseRequest) requestBearer.request();
        execute(ctx, () -> {
            provider.removeConsumer(request.getConsumerId(), request.getToken());
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerTouchRequest) requestBearer.request();
        var wrapper = provider.getConsumer(request.getConsumerId(), request.getToken());
        wrapper.touch();
        var responseBearer = new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    // endregion

    // region Consumer

    private void processPoll(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerPollRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var records = consumer.poll(Duration.ofMillis(request.resolveTimeoutMs()));
            var response = ConsumerPollResponse.of(records);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processCommit(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerCommitRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.commitSync();
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processAssign(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerAssignRequest) requestBearer.request();
        var assignment = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.assign(assignment);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processGetAssignment(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetAssignmentRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var assignment = consumer.assignment();
            var response = ConsumerAssignmentResponse.of(assignment);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processSeek(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekRequest) requestBearer.request();
        var topicPartition = new TopicPartition(request.getTopic(), request.getPartition());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seek(topicPartition, request.getOffset());
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processSeekToBeginning(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekToBeginningRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seekToBeginning(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processSeekToEnd(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerSeekToEndRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.seekToEnd(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
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
        }, ctx::writeAndFlush);
    }

    private void processUnsubscribe(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerUnsubscribeRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            consumer.unsubscribe();
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        }, ctx::writeAndFlush);
    }

    private void processGetSubscription(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetSubscriptionRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var subscription = consumer.subscription();
            var response = ConsumerSubscriptionResponse.of(subscription);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processGetPosition(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetPositionRequest) requestBearer.request();
        var topicPartition = new TopicPartition(request.getTopic(), request.getPartition());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var position = consumer.position(topicPartition);
            var response = ConsumerPositionResponse.of(position);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processGetPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetPartitionsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var partitions = consumer.partitionsFor(request.getTopic());
            var response = ConsumerPartitionsResponse.of(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    @Deprecated
    private void processListPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerListPartitionsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var partitions = consumer.partitionsFor(request.getTopic());
            var response = ConsumerResponseMapper.mapPartitionsResponse(partitions);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processGetGroupMetadata(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetGroupMetadataRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        var metadata = consumer.groupMetadata();
        var response = ConsumerGroupMetadataResponse.of(metadata);
        ctx.writeAndFlush(new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response));
    }

    private void processGetCommitted(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetCommittedRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var committed = consumer.committed(partitions);
            var response = ConsumerCommittedResponse.of(committed);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processGetBeginningOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetBeginningOffsetsRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var offsets = consumer.beginningOffsets(partitions);
            var response = ConsumerOffsetsResponse.of(offsets);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processGetEndOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerGetEndOffsetsRequest) requestBearer.request();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        execute(ctx, () -> {
            var offsets = consumer.endOffsets(partitions);
            var response = ConsumerOffsetsResponse.of(offsets);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ConsumerListTopicsRequest) requestBearer.request();
        var consumer = getConsumer(request.getConsumerId(), request.getToken());
        final Pattern pattern;
        try {
            pattern = request.getPattern() == null ? null : Pattern.compile(request.getPattern());
        } catch (PatternSyntaxException e) {
            HttpUtils.writeBadRequestAndClose(ctx, "Invalid pattern: " + e.getMessage());
            return;
        }
        execute(ctx, () -> {
            var topics = consumer.listTopics();
            if (pattern != null) {
                topics = topics.entrySet().stream()
                        .filter(e -> pattern.matcher(e.getKey()).matches())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            var response = ConsumerTopicsResponse.of(topics);
            return new ConsumerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        }, ctx::writeAndFlush);
    }

    // endregion

    private org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> getConsumer(String id, String token) {
        var wrapper = provider.getConsumer(id, token);
        wrapper.touch();
        return wrapper.getConsumer();
    }
}
