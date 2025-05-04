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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import pro.savel.kafka.admin.AdminRequestDecoder;
import pro.savel.kafka.admin.AdminRequestProcessor;
import pro.savel.kafka.admin.AdminResponseEncoder;
import pro.savel.kafka.consumer.ConsumerRequestDecoder;
import pro.savel.kafka.consumer.ConsumerRequestProcessor;
import pro.savel.kafka.consumer.ConsumerResponseEncoder;
import pro.savel.kafka.producer.ProducerRequestDecoder;
import pro.savel.kafka.producer.ProducerRequestProcessor;
import pro.savel.kafka.producer.ProducerResponseEncoder;

class ServerInitializer extends ChannelInitializer<SocketChannel> implements AutoCloseable {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ProducerRequestDecoder producerRequestDecoder = new ProducerRequestDecoder(objectMapper);
    private final ConsumerRequestDecoder consumerRequestDecoder = new ConsumerRequestDecoder(objectMapper);
    private final AdminRequestDecoder adminRequestDecoder = new AdminRequestDecoder(objectMapper);
    private final DefaultRequestDecoder defaultRequestDecoder = new DefaultRequestDecoder();

    private final ProducerRequestProcessor producerRequestProcessor = new ProducerRequestProcessor();
    private final ConsumerRequestProcessor consumerRequestProcessor = new ConsumerRequestProcessor();
    private final AdminRequestProcessor adminRequestProcessor = new AdminRequestProcessor();

    private final ProducerResponseEncoder producerResponseEncoder = new ProducerResponseEncoder(objectMapper);
    private final ConsumerResponseEncoder consumerResponseEncoder = new ConsumerResponseEncoder(objectMapper);
    private final AdminResponseEncoder adminResponseEncoder = new AdminResponseEncoder(objectMapper);

    private final DefaultInboundHandler defaultInboundHandler = new DefaultInboundHandler();

    static {
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(32 * 1024 * 1024));
        pipeline.addLast(producerRequestDecoder);
        pipeline.addLast(consumerRequestDecoder);
        pipeline.addLast(adminRequestDecoder);
        pipeline.addLast(defaultRequestDecoder);
        pipeline.addLast(producerResponseEncoder);
        pipeline.addLast(consumerResponseEncoder);
        pipeline.addLast(adminResponseEncoder);
        pipeline.addLast(producerRequestProcessor);
        pipeline.addLast(consumerRequestProcessor);
        pipeline.addLast(adminRequestProcessor);
        pipeline.addLast(defaultInboundHandler);
    }

    @Override
    public void close() {
        producerRequestProcessor.close();
        consumerRequestProcessor.close();
        adminRequestProcessor.close();
    }
}