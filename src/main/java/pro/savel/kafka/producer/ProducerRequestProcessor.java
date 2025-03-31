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
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.common.contract.ResponseBearer;
import pro.savel.kafka.common.exceptions.InstanceNotFoundException;
import pro.savel.kafka.common.exceptions.InvalidTokenException;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.ProducerList;

public class ProducerRequestProcessor implements AutoCloseable {

    private final ProducerProvider provider = new ProducerProvider();

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var bearerRequest = requestBearer.request();

        if (bearerRequest instanceof ListProducersRequest) {
            processListProducers(ctx, requestBearer);
        } else if (bearerRequest instanceof GetProducerRequest) {
            processGetProducer(ctx, requestBearer);
        } else if (bearerRequest instanceof CreateProducerRequest) {
            processCreateProducer(ctx, requestBearer);
        } else if (bearerRequest instanceof RemoveProducerRequest) {
            processRemoveProducer(ctx, requestBearer);
        } else if (bearerRequest instanceof TouchProducerRequest) {
            processTouchProducer(ctx, requestBearer);
        } else {
            throw new RuntimeException("Unsupported request type: " + bearerRequest.getClass().getName());
        }
    }

    private void processListProducers(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        //var request = (ListProducersRequest) requestBearer.request();

        var response = new ProducerList();
        var wrappers = provider.getItems();
        wrappers.forEach(wrapper -> response.add(wrapper.id()));

        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.fireChannelRead(responseBearer);
    }

    private void processGetProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (GetProducerRequest) requestBearer.request();

        ProducerWrapper wrapper;
        try {
            wrapper = provider.getItem(request.id());
        } catch (InstanceNotFoundException e) {
            HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion());
            return;
        }
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

    private void processRemoveProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (RemoveProducerRequest) requestBearer.request();

        ProducerWrapper wrapper;
        try {
            wrapper = provider.getItem(request.id(), request.token());
        } catch (InstanceNotFoundException e) {
            HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion());
            return;
        } catch (InvalidTokenException e) {
            HttpUtils.writeForbiddenAndClose(ctx, requestBearer.protocolVersion(), e.getMessage());
            return;
        }

        provider.removeItem(wrapper.id());

        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT);
        ctx.fireChannelRead(responseBearer);
    }

    private void processTouchProducer(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (TouchProducerRequest) requestBearer.request();

        ProducerWrapper wrapper;
        try {
            wrapper = provider.getItem(request.id(), request.token());
        } catch (InstanceNotFoundException e) {
            HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion());
            return;
        } catch (InvalidTokenException e) {
            HttpUtils.writeForbiddenAndClose(ctx, requestBearer.protocolVersion(), e.getMessage());
            return;
        }

        wrapper.touch();

        var responseBearer = new ResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT);
        ctx.fireChannelRead(responseBearer);
    }

    @Override
    public void close() {
        provider.close();
    }
}
