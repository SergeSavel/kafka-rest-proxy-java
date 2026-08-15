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

package pro.savel.kafka.consumer;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.SynchronousBlockingTaskExecutor;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.consumer.requests.*;
import pro.savel.kafka.consumer.responses.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumerRequestProcessorTest {

    @SuppressWarnings("unchecked")
    private final ConsumerProvider provider = new ConsumerProvider(config -> mock(Consumer.class));
    private final ConsumerRequestProcessor processor =
            new ConsumerRequestProcessor(new SynchronousBlockingTaskExecutor(), provider);
    private final EmbeddedChannel channel = new EmbeddedChannel(processor);

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        provider.close();
    }

    private static RequestBearer bearer(ConsumerRequest request) {
        return new RequestBearer(request, Serde.JSON, true);
    }

    private ConsumerWrapper addWrapper() {
        return provider.createConsumer("test-consumer", new Properties(), 60_000, null);
    }

    private static pro.savel.kafka.common.contract.TopicPartition partition(String topic, int partition) {
        return pro.savel.kafka.common.contract.TopicPartition.of(new TopicPartition(topic, partition));
    }

    //region Management

    @Test
    void processCreate_success_returnsCreatedWithIdAndToken() {
        var request = new ConsumerCreateRequest();
        request.setName("my-consumer");
        request.setConfig(new Properties());
        request.setExpirationTimeout(60_000);

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.CREATED, response.getStatus());
        var body = (ConsumerCreateResponse) response.getResponse();
        assertNotNull(body.getId());
        assertNotNull(body.getToken());
        assertEquals(1, provider.getItems().size());
    }

    @Test
    void processCreate_emptyScramPassword_returnsBadRequest() {
        var request = new ConsumerCreateRequest();
        request.setName("my-consumer");
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
    void processRemove_validToken_removesAndReturnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerReleaseRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        assertTrue(provider.getItems().isEmpty());
        verify(consumer).wakeup();
    }

    @Test
    void processRemove_invalidToken_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new ConsumerReleaseRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken("wrong-token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processRemove_unknownId_isNoOpAndReturnsNoContent() {
        var request = new ConsumerReleaseRequest();
        request.setConsumerId("does-not-exist");
        request.setToken("any-token");

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processTouch_updatesExpirationAndReturnsNoContent() throws InterruptedException {
        var wrapper = addWrapper();
        var expiresBefore = wrapper.getExpiresAt();
        Thread.sleep(5);

        var request = new ConsumerTouchRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        assertTrue(wrapper.getExpiresAt() > expiresBefore);
    }

    @Test
    void processTouch_unknownId_returnsNotFound() {
        var request = new ConsumerTouchRequest();
        request.setConsumerId("does-not-exist");
        request.setToken("token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processList_returnsAllConsumers() {
        addWrapper();
        addWrapper();

        channel.writeInbound(bearer(new ConsumerListRequest()));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerListResponse) response.getResponse();
        assertEquals(2, body.size());
    }

    //endregion

    //region Consumer operations

    @Test
    void processPoll_success_returnsMessages() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var tp = new TopicPartition("topic", 0);
        var record = new ConsumerRecord<byte[], byte[]>("topic", 0, 0L, "key".getBytes(), "value".getBytes());
        var records = new ConsumerRecords<byte[], byte[]>(Map.of(tp, List.of(record)));
        when(consumer.poll(any())).thenReturn(records);

        var request = new ConsumerPollRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTimeout(1000L);

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerPollResponse) response.getResponse();
        assertEquals(1, body.size());
        assertEquals("topic", body.get(0).getTopic());
    }

    @Test
    void processCommit_success_returnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerCommitRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).commitSync();
    }

    @Test
    void processCommit_invalidOffsetException_returnsConflict() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        doThrow(new OffsetOutOfRangeException(Map.of())).when(consumer).commitSync();

        var request = new ConsumerCommitRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.CONFLICT, response.status());
        response.release();
    }

    @Test
    void processSeek_success_seeksAndReturnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerSeekRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");
        request.setPartition(2);
        request.setOffset(42);

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).seek(new TopicPartition("topic", 2), 42L);
    }

    @Test
    void processSeekToBeginning_success_returnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerSeekToBeginningRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).seekToBeginning(Set.of(new TopicPartition("topic", 0)));
    }

    @Test
    void processSeekToEnd_success_returnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerSeekToEndRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).seekToEnd(Set.of(new TopicPartition("topic", 0)));
    }

    @Test
    void processAssign_success_returnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerAssignRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).assign(Set.of(new TopicPartition("topic", 0)));
    }

    @Test
    void processGetAssignment_success_returnsAssignment() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        when(consumer.assignment()).thenReturn(Set.of(new TopicPartition("topic", 0)));

        var request = new ConsumerGetAssignmentRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerAssignmentResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processSubscribe_withTopics_subscribesAndReturnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerSubscribeRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopics(List.of("topic-a", "topic-b"));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).subscribe(List.of("topic-a", "topic-b"));
    }

    @Test
    void processSubscribe_withPattern_subscribesAndReturnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerSubscribeRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPattern("topic-.*");

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).subscribe(new SubscriptionPattern("topic-.*"));
    }

    @Test
    void processSubscribe_neitherTopicsNorPattern_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new ConsumerSubscribeRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processUnsubscribe_success_returnsNoContent() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();

        var request = new ConsumerUnsubscribeRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        verify(consumer).unsubscribe();
    }

    @Test
    void processGetSubscription_success_returnsSubscription() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        when(consumer.subscription()).thenReturn(Set.of("topic-a"));

        var request = new ConsumerGetSubscriptionRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerSubscriptionResponse) response.getResponse();
        assertEquals(List.of("topic-a"), body);
    }

    @Test
    void processGetPosition_success_returnsOffset() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        when(consumer.position(new TopicPartition("topic", 0))).thenReturn(123L);

        var request = new ConsumerGetPositionRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");
        request.setPartition(0);

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerPositionResponse) response.getResponse();
        assertEquals(123L, body.getOffset());
    }

    @Test
    void processGetPartitions_success_returnsPartitions() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var node = new Node(1, "host", 9092);
        when(consumer.partitionsFor("topic")).thenReturn(List.of(
                new PartitionInfo("topic", 0, node, new Node[0], new Node[0])));

        var request = new ConsumerGetPartitionsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopic("topic");

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerPartitionsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processListTopics_noPattern_returnsAllTopics() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var node = new Node(1, "host", 9092);
        when(consumer.listTopics()).thenReturn(Map.of(
                "topic-a", List.of(new PartitionInfo("topic-a", 0, node, new Node[0], new Node[0])),
                "topic-b", List.of(new PartitionInfo("topic-b", 0, node, new Node[0], new Node[0]))));

        var request = new ConsumerListTopicsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerTopicsResponse) response.getResponse();
        assertEquals(2, body.size());
    }

    @Test
    void processListTopics_withPattern_filtersTopics() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var node = new Node(1, "host", 9092);
        when(consumer.listTopics()).thenReturn(Map.of(
                "topic-a", List.of(new PartitionInfo("topic-a", 0, node, new Node[0], new Node[0])),
                "other", List.of(new PartitionInfo("other", 0, node, new Node[0], new Node[0]))));

        var request = new ConsumerListTopicsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPattern("topic-.*");

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerTopicsResponse) response.getResponse();
        assertEquals(1, body.size());
        assertEquals("topic-a", body.get(0).getTopic());
    }

    @Test
    void processListTopics_invalidPattern_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new ConsumerListTopicsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPattern("[");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processGetGroupMetadata_success_returnsMetadata() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("group-1", 3, "member-1", java.util.Optional.empty()));

        var request = new ConsumerGetGroupMetadataRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerGroupMetadataResponse) response.getResponse();
        assertEquals("group-1", body.getGroupId());
        assertEquals(3, body.getGenerationId());
    }

    @Test
    void processGetCommitted_success_returnsCommittedOffsets() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var tp = new TopicPartition("topic", 0);
        when(consumer.committed(Set.of(tp))).thenReturn(Map.of(tp, new OffsetAndMetadata(10L)));

        var request = new ConsumerGetCommittedRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerCommittedResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processGetBeginningOffsets_success_returnsOffsets() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var tp = new TopicPartition("topic", 0);
        when(consumer.beginningOffsets(Set.of(tp))).thenReturn(Map.of(tp, 0L));

        var request = new ConsumerGetBeginningOffsetsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerOffsetsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processGetEndOffsets_success_returnsOffsets() {
        var wrapper = addWrapper();
        var consumer = wrapper.getConsumer();
        var tp = new TopicPartition("topic", 0);
        when(consumer.endOffsets(Set.of(tp))).thenReturn(Map.of(tp, 100L));

        var request = new ConsumerGetEndOffsetsRequest();
        request.setConsumerId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic", 0)));

        channel.writeInbound(bearer(request));

        ConsumerResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (ConsumerOffsetsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    //endregion

    @Test
    void processRequest_unknownRequestType_returnsInternalServerError() {
        channel.writeInbound(bearer(new UnknownConsumerRequest()));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
        response.release();
    }

    private record UnknownConsumerRequest() implements ConsumerRequest {
    }
}
