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

package pro.savel.kafka;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.contract.RequestBearer;
import pro.savel.kafka.common.exceptions.InstanceNotFoundException;
import pro.savel.kafka.common.exceptions.InvalidTokenException;
import pro.savel.kafka.producer.ProducerRequestProcessor;
import pro.savel.kafka.producer.requests.ProducerRequest;

@ChannelHandler.Sharable
public class RequestProcessor extends SimpleChannelInboundHandler<RequestBearer> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RequestProcessor.class);

    private final ProducerRequestProcessor producerProcessor = new ProducerRequestProcessor();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        try {
            if (requestBearer.request() instanceof ProducerRequest) {
                producerProcessor.processRequest(ctx, requestBearer);
                return;
            }
        } catch (InstanceNotFoundException e) {
            HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion());
            return;
        } catch (InvalidTokenException e) {
            HttpUtils.writeForbiddenAndClose(ctx, requestBearer.protocolVersion(), e.getMessage());
            return;
        }
        throw new RuntimeException("Unexpected request type: " + requestBearer.request().getClass().getName());
    }

    @Override
    public void close() {
        producerProcessor.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("An error occurred while processing the request.", cause);
        ctx.close();
    }
}
