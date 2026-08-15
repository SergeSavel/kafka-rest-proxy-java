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

package pro.savel.kafka.producer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pro.savel.kafka.common.*;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.ProducerCreateResponse;
import pro.savel.kafka.producer.responses.ProducerListResponse;
import pro.savel.kafka.producer.responses.ProducerPartitionsResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

public class ProducerRequestProcessor extends AbstractRequestProcessor {

    private final ProducerProvider provider;

    public ProducerRequestProcessor(BlockingTaskExecutor blockingTaskExecutor, ProducerProvider provider) {
        super("producer", ProducerRequest.class, blockingTaskExecutor);
        this.provider = provider;
    }

    @Override
    protected void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        switch (requestBearer.request()) {
            case ProducerSendRequest ignored -> processSend(ctx, requestBearer);
            case ProducerGetPartitionsRequest ignored -> processGetPartitions(ctx, requestBearer);
            case ProducerCreateRequest ignored -> processCreate(ctx, requestBearer);
            case ProducerRemoveRequest ignored -> processRemove(ctx, requestBearer);
            case ProducerTouchRequest ignored -> processTouch(ctx, requestBearer);
            case ProducerBeginTransactionRequest ignored -> processBeginTransaction(ctx, requestBearer);
            case ProducerCommitTransactionRequest ignored -> processCommitTransaction(ctx, requestBearer);
            case ProducerAbortTransactionRequest ignored -> processAbortTransaction(ctx, requestBearer);
            case ProducerListRequest ignored -> processList(ctx, requestBearer);
            default ->
                    throw new RuntimeException("Unexpected producer request type: " + requestBearer.request().getClass().getName());
        }
    }

//region Management

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = ProducerListResponse.of(wrappers);
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerCreateRequest) requestBearer.request();
        var owner = ctx.channel().attr(NettyAttributes.USERNAME).get();
        execute(ctx,
                () -> provider.createProducer(request.getName(), request.getConfig(), request.getExpirationTimeout(), owner),
                wrapper -> {
                    var response = ProducerCreateResponse.of(wrapper);
                    ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response));
                });
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerRemoveRequest) requestBearer.request();
        execute(ctx, () -> {
            provider.removeProducer(request.getProducerId(), request.getToken());
            return null;
        }, ignored -> ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null)));
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerTouchRequest) requestBearer.request();
        var wrapper = provider.getProducer(request.getProducerId(), request.getToken());
        wrapper.touch();
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

//endregion

//region Producer

    private void processSend(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerSendRequest) requestBearer.request();
        var producer = getProducer(request.getProducerId(), request.getToken());
        var record = new ProducerRecord<>(request.getTopic(), request.getPartition(), request.getKey(), request.getValue());
        var headers = request.getHeaders();
        if (headers != null)
            headers.forEach(header -> record.headers().add(header.getKey(), header.getValue()));
        Callback callback = (RecordMetadata metadata, Exception exception) -> ensureResponse(ctx, () -> {
            if (exception != null) {
                if (!handleError(ctx, exception)) {
                    logger.error("An unexpected error occurred while processing producer send.", exception);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(exception));
                }
            } else {
                var response = ProducerSendResponse.of(metadata);
                ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response));
            }
        });
        producer.send(record, callback);
    }

    private void processGetPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerGetPartitionsRequest) requestBearer.request();
        var producer = getProducer(request.getProducerId(), request.getToken());
        execute(ctx, () -> producer.partitionsFor(request.getTopic()), partitions -> {
            var response = ProducerPartitionsResponse.of(partitions);
            ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.OK, response));
        });
    }

    private void processBeginTransaction(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerBeginTransactionRequest) requestBearer.request();
        var producer = getProducer(request.getProducerId(), request.getToken());
        execute(ctx, () -> {
            producer.beginTransaction();
            return null;
        }, ignored -> ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null)));
    }

    private void processCommitTransaction(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerCommitTransactionRequest) requestBearer.request();
        var producer = getProducer(request.getProducerId(), request.getToken());
        execute(ctx, () -> {
            producer.commitTransaction();
            return null;
        }, ignored -> ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null)));
    }

    private void processAbortTransaction(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerAbortTransactionRequest) requestBearer.request();
        var producer = getProducer(request.getProducerId(), request.getToken());
        execute(ctx, () -> {
            producer.abortTransaction();
            return null;
        }, ignored -> ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null)));
    }

//endregion

    private Producer<byte[], byte[]> getProducer(String id, String token) {
        var wrapper = provider.getProducer(id, token);
        wrapper.touch();
        return wrapper.getProducer();
    }
}
