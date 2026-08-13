// Copyright 2026 Sergey Savelev (serge@savel.pro)
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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.BlockingTaskExecutor;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.SynchronousBlockingTaskExecutor;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.ProducerCreateResponse;
import pro.savel.kafka.producer.responses.ProducerListResponse;
import pro.savel.kafka.producer.responses.ProducerPartitionsResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProducerRequestProcessorTest {

    @SuppressWarnings("unchecked")
    private final ProducerProvider provider = new ProducerProvider(config -> mock(Producer.class));
    private final ProducerRequestProcessor processor =
            new ProducerRequestProcessor(new SynchronousBlockingTaskExecutor(), provider);
    private final EmbeddedChannel channel = new EmbeddedChannel(processor);

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        provider.close();
    }

    private static RequestBearer bearer(ProducerRequest request) {
        return new RequestBearer(request, Serde.JSON, true);
    }

    private ProducerWrapper addWrapper() {
        return provider.createProducer("test-producer", new Properties(), 60_000, null);
    }

    //region Management

    @Test
    void processCreate_success_returnsCreatedWithIdAndToken() {
        var request = new ProducerCreateRequest();
        request.setName("my-producer");
        request.setConfig(new Properties());
        request.setExpirationTimeout(60_000);

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.CREATED, response.getStatus());
        var body = (ProducerCreateResponse) response.getResponse();
        assertNotNull(body.getId());
        assertNotNull(body.getToken());
        assertEquals(1, provider.getItems().size());
    }

    @Test
    void processCreate_emptyScramPassword_returnsBadRequest() {
        var request = new ProducerCreateRequest();
        request.setName("my-producer");
        var config = new Properties();
        config.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        config.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"\";");
        request.setConfig(config);
        request.setExpirationTimeout(60_000);

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
        assertEquals(0, provider.getItems().size());
    }

    @Test
    void processCreate_kafkaRejectsConfig_returnsBadRequest() {
        // The real client constructor is what raises ConfigException on a malformed property, and
        // the shared provider hands back a mock instead - so this test needs its own failing one.
        var rejectingProvider = new ProducerProvider(config -> {
            throw new ConfigException("bootstrap.servers", "not-a-broker");
        });
        var rejectingChannel = new EmbeddedChannel(
                new ProducerRequestProcessor(new SynchronousBlockingTaskExecutor(), rejectingProvider));
        var request = new ProducerCreateRequest();
        request.setName("my-producer");
        request.setConfig(new Properties());
        request.setExpirationTimeout(60_000);

        rejectingChannel.writeInbound(bearer(request));

        FullHttpResponse response = rejectingChannel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
        assertEquals(0, rejectingProvider.getItems().size());
        rejectingChannel.finishAndReleaseAll();
        rejectingProvider.close();
    }

    @Test
    void processRemove_validToken_removesAndReturnsNoContent() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();

        var request = new ProducerRemoveRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        assertTrue(provider.getItems().isEmpty());
        verify(producer).close(any());
    }

    @Test
    void processRemove_invalidToken_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new ProducerRemoveRequest();
        request.setProducerId(wrapper.getId());
        request.setToken("wrong-token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        assertEquals(1, provider.getItems().size());
        response.release();
    }

    @Test
    void processRemove_unknownId_isNoOpAndReturnsNoContent() {
        var request = new ProducerRemoveRequest();
        request.setProducerId("does-not-exist");
        request.setToken("any-token");

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processTouch_updatesExpirationAndReturnsNoContent() throws InterruptedException {
        var wrapper = addWrapper();
        var expiresBefore = wrapper.getExpiresAt();
        Thread.sleep(5);

        var request = new ProducerTouchRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        assertTrue(wrapper.getExpiresAt() > expiresBefore);
    }

    @Test
    void processTouch_unknownId_returnsNotFound() {
        var request = new ProducerTouchRequest();
        request.setProducerId("does-not-exist");
        request.setToken("token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processList_returnsAllProducers() {
        addWrapper();
        addWrapper();

        channel.writeInbound(bearer(new ProducerListRequest()));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ProducerListResponse) response.getResponse();
        assertEquals(2, body.size());
    }

    //endregion

    //region Producer operations

    @Test
    void processSend_success_returnsCreatedWithMetadata() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            var metadata = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0L, 5, 7);
            callback.onCompletion(metadata, null);
            return null;
        }).when(producer).send(any(), any());

        var request = new ProducerSendRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");
        request.setValue("payload".getBytes());

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.CREATED, response.getStatus());
        var body = (ProducerSendResponse) response.getResponse();
        assertEquals("topic", body.getTopic());
        assertEquals(5, body.getSerializedKeySize());
        assertEquals(7, body.getSerializedValueSize());
    }

    @Test
    void processSend_kafkaError_mapsToHttpStatus() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, new AuthorizationException("not allowed"));
            return null;
        }).when(producer).send(any(), any());

        var request = new ProducerSendRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
        response.release();
    }

    @Test
    void processSend_duplicateHeaderKeys_bothReachTheRecord() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        var sentRecord = new AtomicReference<ProducerRecord<byte[], byte[]>>();
        doAnswer(invocation -> {
            sentRecord.set(invocation.getArgument(0));
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0L, 0, 0), null);
            return null;
        }).when(producer).send(any(), any());

        var request = new ProducerSendRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");
        request.setHeaders(List.of(
                new ProducerSendRequest.Header("dup", "v1".getBytes(StandardCharsets.UTF_8)),
                new ProducerSendRequest.Header("dup", "v2".getBytes(StandardCharsets.UTF_8))));

        channel.writeInbound(bearer(request));

        var values = new ArrayList<byte[]>();
        sentRecord.get().headers().headers("dup").forEach(header -> values.add(header.value()));
        assertEquals(2, values.size(), "a repeated header key must not collapse on the way to Kafka");
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), values.get(0));
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), values.get(1));
    }

    @Test
    void processGetPartitions_success_returnsPartitions() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        var node = new org.apache.kafka.common.Node(1, "host", 9092);
        when(producer.partitionsFor("topic")).thenReturn(List.of(
                new PartitionInfo("topic", 0, node, new org.apache.kafka.common.Node[0], new org.apache.kafka.common.Node[0]),
                new PartitionInfo("topic", 1, node, new org.apache.kafka.common.Node[0], new org.apache.kafka.common.Node[0])));

        var request = new ProducerGetPartitionsRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ProducerPartitionsResponse) response.getResponse();
        assertEquals(2, body.size());
    }

    @Test
    void processBeginTransaction_success_returnsNoContent() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();

        var request = new ProducerBeginTransactionRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(producer).beginTransaction();
    }

    @Test
    void processCommitTransaction_success_returnsNoContent() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();

        var request = new ProducerCommitTransactionRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ProducerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(producer).commitTransaction();
    }

    @Test
    void processAbortTransaction_producerThrowsIllegalState_returnsConflict() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        doThrow(new IllegalStateException("no active transaction")).when(producer).abortTransaction();

        var request = new ProducerAbortTransactionRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.CONFLICT, response.status());
        response.release();
    }

    //endregion

    @Test
    void processRequest_unknownRequestType_returnsInternalServerError() {
        channel.writeInbound(bearer(new UnknownProducerRequest()));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
        response.release();
    }

    private record UnknownProducerRequest() implements ProducerRequest {
    }

    //region Response guarantee

    @Test
    void processSend_callbackThrows_returnsInternalServerErrorInsteadOfHanging() {
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        var callbackRef = new AtomicReference<Callback>();
        doAnswer(invocation -> {
            callbackRef.set(invocation.getArgument(1));
            return null;
        }).when(producer).send(any(), any());
        var metadata = mock(RecordMetadata.class);
        when(metadata.topic()).thenThrow(new RuntimeException("response construction failed"));

        var request = new ProducerSendRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");
        request.setValue("payload".getBytes());

        channel.writeInbound(bearer(request));
        // Kafka completes the send on its own sender thread, long after channelRead returned, so a
        // throw here has no pipeline exception handling to fall back on.
        callbackRef.get().onCompletion(metadata, null);

        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response, "a response must be written even when the callback throws");
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
        response.release();
    }

    @Test
    void execute_completionThrows_returnsInternalServerErrorInsteadOfHanging() {
        var executor = new DeferredBlockingTaskExecutor();
        var deferredChannel = new EmbeddedChannel(new ProducerRequestProcessor(executor, provider));
        var wrapper = addWrapper();
        var producer = wrapper.getProducer();
        var partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.partition()).thenThrow(new RuntimeException("response construction failed"));
        when(producer.partitionsFor("topic")).thenReturn(List.of(partitionInfo));

        var request = new ProducerGetPartitionsRequest();
        request.setProducerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");

        deferredChannel.writeInbound(bearer(request));
        executor.runCompletions();

        FullHttpResponse response = deferredChannel.readOutbound();
        assertNotNull(response, "a response must be written even when the completion throws");
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
        response.release();
        deferredChannel.finishAndReleaseAll();
    }

    /**
     * Runs the operation inline but hands the completion back through a queue, the way the real
     * executor hands it to the event loop. Unlike {@link SynchronousBlockingTaskExecutor} this keeps
     * a throwing completion from unwinding back into channelRead, which is what makes the guard in
     * {@code execute} observable at all.
     */
    private static class DeferredBlockingTaskExecutor extends BlockingTaskExecutor {

        private final Queue<Runnable> completions = new ArrayDeque<>();

        @Override
        public <T> void execute(ChannelHandlerContext ctx, Callable<T> operation, BiConsumer<T, Throwable> completion) {
            T result = null;
            Throwable error = null;
            try {
                result = operation.call();
            } catch (Throwable e) {
                error = e;
            }
            var result_ = result;
            var error_ = error;
            completions.add(() -> completion.accept(result_, error_));
        }

        void runCompletions() {
            Runnable next;
            while ((next = completions.poll()) != null)
                next.run();
        }
    }

    //endregion
}
