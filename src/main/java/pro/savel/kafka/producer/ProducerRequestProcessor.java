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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;
import pro.savel.kafka.common.exceptions.UnauthenticatedException;
import pro.savel.kafka.common.exceptions.UnauthorizedException;
import pro.savel.kafka.producer.requests.*;

import java.util.List;

@ChannelHandler.Sharable
public class ProducerRequestProcessor extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ProducerRequestProcessor.class);

    private final ProducerProvider provider = new ProducerProvider();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && bearer.request() instanceof ProducerRequest) {
            try {
                processRequest(ctx, bearer);
            } catch (NotFoundException e) {
                HttpUtils.writeNotFoundAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (UnauthenticatedException e) {
                HttpUtils.writeUnauthorizedAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (UnauthorizedException e) {
                HttpUtils.writeForbiddenAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while processing producer request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
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

    private static List<PartitionInfo> getPartitions(ProducerWrapper wrapper, ProducerGetPartitionsRequest request) throws UnauthenticatedException, UnauthorizedException {
        var producer = wrapper.getProducer();
        try {
            return producer.partitionsFor(request.getTopic());
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to get partitions.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to get partitions.", e);
        }
    }

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = ProducerResponseMapper.mapListResponse(wrappers);
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (ProducerCreateRequest) requestBearer.request();
        var wrapper = provider.createProducer(request.getName(), request.getConfig(), request.getExpirationTimeout());
        var response = ProducerResponseMapper.mapCreateResponse(wrapper);
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) throws BadRequestException {
        var request = (ProducerRemoveRequest) requestBearer.request();
        provider.removeProducer(request.getProducerId(), request.getToken());
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (ProducerTouchRequest) requestBearer.request();
        var wrapper = provider.getProducer(request.getProducerId(), request.getToken());
        wrapper.touch();
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processSend(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException, UnauthenticatedException, UnauthorizedException {
        var request = (ProducerSendRequest) requestBearer.request();
        var wrapper = provider.getProducer(request.getProducerId(), request.getToken());
        wrapper.touch();
        var callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Produce request completed.");
                }
                if (exception == null) {
                    var response = ProducerResponseMapper.mapSendResponse(metadata);
                    var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
                    ctx.writeAndFlush(responseBearer);
                } else {
                    if (exception instanceof InvalidTopicException || exception instanceof UnknownTopicOrPartitionException)
                        HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), exception.getMessage());
                    else if (exception instanceof SaslAuthenticationException)
                        HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), exception.getMessage());
                    else {
                        logger.error("Unable to produce message.", exception);
                        HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), exception.getMessage());
                    }
                }
            }
        };
        if (logger.isDebugEnabled()) {
            logger.debug("Starting produce request processing.");
        }
        var producer = wrapper.getProducer();
        var record = new ProducerRecord<>(request.getTopic(), request.getPartition(), request.getKey(), request.getValue());
        request.getHeaders().forEach((key, value) -> record.headers().add(key, value));
        try {
            producer.send(record, callback);
        } catch (AuthenticationException e) {
            throw new UnauthenticatedException("Unable to produce message.", e);
        } catch (AuthorizationException e) {
            throw new UnauthorizedException("Unable to produce message.", e);
        }
    }

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException, UnauthenticatedException, UnauthorizedException {
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

    private void processGetPartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException, UnauthenticatedException, UnauthorizedException {
        var request = (ProducerGetPartitionsRequest) requestBearer.request();
        var wrapper = provider.getProducer(request.getProducerId(), request.getToken());
        wrapper.touch();
        var partitions = getPartitions(wrapper, request);
        var response = ProducerResponseMapper.mapPartitionsResponse(partitions);
        var responseBearer = new ProducerResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }
}
