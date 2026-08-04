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

package pro.savel.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import pro.savel.kafka.admin.AdminRequestDecoder;
import pro.savel.kafka.admin.AdminRequestProcessor;
import pro.savel.kafka.admin.AdminResponseEncoder;
import pro.savel.kafka.common.BlockingTaskExecutor;
import pro.savel.kafka.consumer.ConsumerRequestDecoder;
import pro.savel.kafka.consumer.ConsumerRequestProcessor;
import pro.savel.kafka.consumer.ConsumerResponseEncoder;
import pro.savel.kafka.producer.ProducerRequestDecoder;
import pro.savel.kafka.producer.ProducerRequestProcessor;
import pro.savel.kafka.producer.ProducerResponseEncoder;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ServerInitializer extends ChannelInitializer<SocketChannel> implements AutoCloseable {

    private static final long CLIENT_SHUTDOWN_TIMEOUT_SECONDS = 40;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    private static final ValidatorFactory validatorFactory = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory();

    private final HealthRequestDecoder healthRequestDecoder = new HealthRequestDecoder();
    private final BasicAuthenticationHandler basicAuthenticationHandler = new BasicAuthenticationHandler(objectMapper);
    private final HttpVersionHandler httpVersionHandler = new HttpVersionHandler();

    private final ProducerRequestDecoder producerRequestDecoder = new ProducerRequestDecoder(objectMapper, validatorFactory);
    private final ConsumerRequestDecoder consumerRequestDecoder = new ConsumerRequestDecoder(objectMapper, validatorFactory);
    private final AdminRequestDecoder adminRequestDecoder = new AdminRequestDecoder(objectMapper, validatorFactory);
    private final VersionRequestDecoder versionRequestDecoder = new VersionRequestDecoder();
    private final DefaultRequestDecoder defaultRequestDecoder = new DefaultRequestDecoder();

    private final BlockingTaskExecutor blockingTaskExecutor = new BlockingTaskExecutor();
    private final ProducerRequestProcessor producerRequestProcessor = new ProducerRequestProcessor(blockingTaskExecutor);
    private final ConsumerRequestProcessor consumerRequestProcessor = new ConsumerRequestProcessor(blockingTaskExecutor);
    private final AdminRequestProcessor adminRequestProcessor = new AdminRequestProcessor(blockingTaskExecutor);

    private final ProducerResponseEncoder producerResponseEncoder = new ProducerResponseEncoder(objectMapper);
    private final ConsumerResponseEncoder consumerResponseEncoder = new ConsumerResponseEncoder(objectMapper);
    private final AdminResponseEncoder adminResponseEncoder = new AdminResponseEncoder(objectMapper);

    private final DefaultInboundHandler defaultInboundHandler = new DefaultInboundHandler();
    private final ServerConfig config;

    ServerInitializer(ServerConfig config) {
        this.config = config;
    }

    public void initialize() {
        basicAuthenticationHandler.initialize();
    }

    @Override
    protected void initChannel(SocketChannel channel) {

        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(httpVersionHandler);
        pipeline.addLast(new ReadTimeoutHandler(config.readTimeoutSeconds(), TimeUnit.SECONDS));
        pipeline.addLast(new WriteTimeoutHandler(config.writeTimeoutSeconds(), TimeUnit.SECONDS));
        pipeline.addLast(new JsonRequestSizeLimitHandler(config.maxJsonRequestBytes()));
        pipeline.addLast(new HttpObjectAggregator(config.maxRequestBytes()));
        pipeline.addLast(new HttpRequestFlowControlHandler());
        pipeline.addLast(healthRequestDecoder);
        pipeline.addLast(versionRequestDecoder);
        pipeline.addLast(basicAuthenticationHandler);
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
        blockingTaskExecutor.close();

        var closeExecutor = Executors.newVirtualThreadPerTaskExecutor();
        closeExecutor.submit(producerRequestProcessor::close);
        closeExecutor.submit(consumerRequestProcessor::close);
        closeExecutor.submit(adminRequestProcessor::close);
        closeExecutor.shutdown();
        try {
            if (!closeExecutor.awaitTermination(CLIENT_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                closeExecutor.shutdownNow();
        } catch (InterruptedException e) {
            closeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
