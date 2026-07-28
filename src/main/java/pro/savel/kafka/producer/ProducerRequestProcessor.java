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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.*;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.ProducerCreateResponse;
import pro.savel.kafka.producer.responses.ProducerListResponse;
import pro.savel.kafka.producer.responses.ProducerPartitionsResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@ChannelHandler.Sharable
public class ProducerRequestProcessor extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ProducerRequestProcessor.class);

    private final ProducerProvider provider = new ProducerProvider();
    private final BlockingTaskExecutor blockingTaskExecutor;

    public ProducerRequestProcessor(BlockingTaskExecutor blockingTaskExecutor) {
        this.blockingTaskExecutor = blockingTaskExecutor;
    }

//region Overrides

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && bearer.request() instanceof ProducerRequest) {
            try {
                processRequest(ctx, bearer);
            } catch (Exception e) {
                if (!handleError(ctx, e)) {
                    logger.error("An unexpected error occurred while processing producer request.", e);
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("An error occurred while processing producer request.", cause);
        ctx.close();
    }

    @Override
    public void close() {
        provider.close();
    }

//endregion

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var requestClass = requestBearer.request().getClass();
        if (requestClass == ProducerSendRequest.class)
            processSend(ctx, requestBearer);
        else if (requestClass == ProducerGetPartitionsRequest.class)
            processGetPartitions(ctx, requestBearer);
        else if (requestClass == ProducerCreateRequest.class)
            processCreate(ctx, requestBearer);
        else if (requestClass == ProducerRemoveRequest.class)
            processRemove(ctx, requestBearer);
        else if (requestClass == ProducerTouchRequest.class)
            processTouch(ctx, requestBearer);
        else if (requestClass == ProducerListRequest.class)
            processList(ctx, requestBearer);
        else
            throw new RuntimeException("Unexpected producer request type: " + requestClass.getName());
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
            headers.forEach((key, value) -> record.headers().add(key, value));
        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                if (!handleError(ctx, exception)) {
                    logger.error("An unexpected error occurred while processing producer send.", exception);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(exception));
                }
            } else {
                var response = ProducerSendResponse.of(metadata);
                ctx.writeAndFlush(new ProducerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response));
            }
        };
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

//endregion

    private Producer<byte[], byte[]> getProducer(String id, String token) {
        var wrapper = provider.getProducer(id, token);
        wrapper.touch();
        return wrapper.getProducer();
    }

    private <T> void execute(
            ChannelHandlerContext ctx,
            Callable<T> operation,
            Consumer<T> completion) {
        blockingTaskExecutor.execute(ctx, operation, (result, error) -> {
            if (error == null) {
                completion.accept(result);
            } else if (!handleError(ctx, error)) {
                logger.error("An unexpected error occurred while processing producer request.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            }
        });
    }

    private static boolean handleError(ChannelHandlerContext ctx, Throwable error) {
        var handled = true;
        if ((error instanceof java.util.concurrent.CompletionException || error instanceof ExecutionException)
                && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (error instanceof org.apache.kafka.common.errors.TimeoutException && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (!CommonErrors.handle(ctx, error))
            handled = false;
        return handled;
    }
}
