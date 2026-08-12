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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.SynchronousBlockingTaskExecutor;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.producer.requests.*;
import pro.savel.kafka.producer.responses.ProducerCreateResponse;
import pro.savel.kafka.producer.responses.ProducerListResponse;
import pro.savel.kafka.producer.responses.ProducerPartitionsResponse;
import pro.savel.kafka.producer.responses.ProducerSendResponse;

import java.util.List;
import java.util.Properties;

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
}
