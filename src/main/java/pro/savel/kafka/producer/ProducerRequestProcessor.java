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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.common.contract.ResponseBearer;
import pro.savel.kafka.common.exceptions.InstanceNotFoundException;
import pro.savel.kafka.common.exceptions.InvalidTokenException;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.DeliveryResult;
import pro.savel.kafka.producer.responses.ProducerList;

public class ProducerRequestProcessor implements AutoCloseable {

    private final ProducerProvider provider = new ProducerProvider();

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) throws InstanceNotFoundException, InvalidTokenException {
        var bearerRequest = requestBearer.request();

        if (bearerRequest instanceof ProduceRequest) {
            processProduce(ctx, requestBearer);
            return;
        }
        if (bearerRequest instanceof CreateProducerRequest) {
            processCreateProducer(ctx, requestBearer);
            return;
        }
        if (bearerRequest instanceof RemoveProducerRequest) {
            processRemoveProducer(ctx, requestBearer);
            return;
        }
        if (bearerRequest instanceof GetProducerRequest) {
            processGetProducer(ctx, requestBearer);
            return;
        }
        if (bearerRequest instanceof TouchProducerRequest) {
            processTouchProducer(ctx, requestBearer);
            return;
        }
        if (bearerRequest instanceof ListProducersRequest) {
            processListProducers(ctx, requestBearer);
            return;
        }
        throw new RuntimeException("Unexpected producer request type: " + bearerRequest.getClass().getName());
    }

    private void processListProducers(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var response = new ProducerList();
        var wrappers = provider.getItems();
        wrappers.forEach(wrapper -> response.add(wrapper.id()));
        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.fireChannelRead(responseBearer);
    }

    private void processGetProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) throws InstanceNotFoundException {
        var request = (GetProducerRequest) requestBearer.request();
        ProducerWrapper wrapper;
        wrapper = provider.getItem(request.id());
        var response = ProducerResponseMapper.mapProducer(wrapper);
        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.fireChannelRead(responseBearer);
    }

    private void processCreateProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (CreateProducerRequest) requestBearer.request();
        var wrapper = provider.createProducer(request.name(), request.config(), request.expirationTimeout());
        var response = ProducerResponseMapper.mapProducerWithToken(wrapper);
        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        ctx.fireChannelRead(responseBearer);
    }

    private void processRemoveProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) throws InstanceNotFoundException, InvalidTokenException {
        var request = (RemoveProducerRequest) requestBearer.request();
        ProducerWrapper wrapper;
        wrapper = provider.getItem(request.getId(), request.getToken());
        provider.removeItem(wrapper.id());
        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT);
        ctx.fireChannelRead(responseBearer);
    }

    private void processTouchProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) throws InstanceNotFoundException, InvalidTokenException {
        var request = (TouchProducerRequest) requestBearer.request();
        ProducerWrapper wrapper;
        wrapper = provider.getItem(request.getId(), request.getToken());
        wrapper.touch();
        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT);
        ctx.fireChannelRead(responseBearer);
    }

    private void processProduce(ChannelHandlerContext ctx, RequestBearer requestBearer) throws InstanceNotFoundException, InvalidTokenException {
        var request = (ProduceRequest) requestBearer.request();
        ProducerWrapper wrapper;
        wrapper = provider.getItem(request.getId(), request.getToken());
        var callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    var response = new DeliveryResult();
                    response.setTopic(metadata.topic());
                    response.setPartition(metadata.partition());
                    response.setOffset(metadata.offset());
                    response.setTimestamp(metadata.timestamp());
                    var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
                    ctx.fireChannelRead(responseBearer);
                } else {
                    if (exception instanceof InvalidTopicException || exception instanceof UnknownTopicOrPartitionException) {
                        HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), exception.getMessage());
                    } else {
                        HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), exception.getMessage());
                    }
                }
            }
        };
        wrapper.produce(request, callback);
    }

    @Override
    public void close() {
        provider.close();
    }
}
