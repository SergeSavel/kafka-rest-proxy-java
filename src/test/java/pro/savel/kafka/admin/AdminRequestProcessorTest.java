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

package pro.savel.kafka.admin;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.admin.data.AdminAclBinding;
import pro.savel.kafka.admin.requests.AdminRequest;
import pro.savel.kafka.admin.requests.acls.AdminCreateAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDeleteAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDescribeAclsRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeClusterRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeFeaturesRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeLogDirsRequest;
import pro.savel.kafka.admin.requests.config.AdminAlterGroupConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminAlterTopicConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDeleteGroupConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDeleteTopicConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeGroupConfigsRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeTopicConfigsRequest;
import pro.savel.kafka.admin.requests.group.*;
import pro.savel.kafka.admin.requests.management.AdminCreateRequest;
import pro.savel.kafka.admin.requests.management.AdminListRequest;
import pro.savel.kafka.admin.requests.management.AdminRemoveRequest;
import pro.savel.kafka.admin.requests.management.AdminTouchRequest;
import pro.savel.kafka.admin.requests.offset.AdminListEarliestOffsetsRequest;
import pro.savel.kafka.admin.requests.offset.AdminListTimestampOffsetsRequest;
import pro.savel.kafka.admin.requests.producer.AdminAbortTransactionRequest;
import pro.savel.kafka.admin.requests.producer.AdminDescribeProducersRequest;
import pro.savel.kafka.admin.requests.topic.*;
import pro.savel.kafka.admin.responses.*;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.SynchronousBlockingTaskExecutor;
import pro.savel.kafka.common.contract.Serde;
import pro.savel.kafka.common.contract.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdminRequestProcessorTest {

    private final AdminProvider provider = new AdminProvider(config -> mock(Admin.class));
    private final AdminRequestProcessor processor =
            new AdminRequestProcessor(new SynchronousBlockingTaskExecutor(), provider);
    private final EmbeddedChannel channel = new EmbeddedChannel(processor);

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        provider.close();
    }

    private static RequestBearer bearer(AdminRequest request) {
        return new RequestBearer(request, Serde.JSON, true);
    }

    private AdminWrapper addWrapper() {
        return provider.createAdmin("test-admin", new Properties(), 60_000, null);
    }

    private static TopicPartition partition(String topic, int partition) {
        return TopicPartition.of(new org.apache.kafka.common.TopicPartition(topic, partition));
    }

    private static <T> KafkaFuture<T> failed(Throwable error) {
        var future = new org.apache.kafka.common.internals.KafkaFutureImpl<T>();
        future.completeExceptionally(error);
        return future;
    }

    // AdminDescribeLogDirsRequest only exposes @Getter (populated via Jackson field access in production).
    private static void setField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    //region Management

    @Test
    void processCreate_success_returnsCreatedWithIdAndToken() {
        var request = new AdminCreateRequest();
        request.setName("my-admin");
        request.setConfig(new Properties());
        request.setExpirationTimeout(60_000);

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.CREATED, response.getStatus());
        var body = (AdminCreateResponse) response.getResponse();
        assertNotNull(body.getId());
        assertNotNull(body.getToken());
        assertEquals(1, provider.getItems().size());
    }

    @Test
    void processRemove_validToken_removesAndReturnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();

        var request = new AdminRemoveRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
        assertTrue(provider.getItems().isEmpty());
        verify(admin).close(any());
    }

    @Test
    void processRemove_invalidToken_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new AdminRemoveRequest();
        request.setAdminId(wrapper.getId());
        request.setToken("wrong-token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processTouch_unknownId_returnsNotFound() {
        var request = new AdminTouchRequest();
        request.setAdminId("does-not-exist");
        request.setToken("token");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processTouch_validId_returnsNoContent() {
        var wrapper = addWrapper();

        var request = new AdminTouchRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processList_returnsAllAdmins() {
        addWrapper();
        addWrapper();

        channel.writeInbound(bearer(new AdminListRequest()));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListResponse) response.getResponse();
        assertEquals(2, body.size());
    }

    //endregion

    //region Cluster

    @Test
    void processDescribeCluster_success_returnsClusterInfo() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeClusterResult.class);
        var node = new Node(1, "host", 9092);
        when(result.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(node)));
        when(result.clusterId()).thenReturn(KafkaFuture.completedFuture("cluster-1"));
        when(result.controller()).thenReturn(KafkaFuture.completedFuture(node));
        when(result.authorizedOperations()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(admin.describeCluster()).thenReturn(result);

        var request = new AdminDescribeClusterRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeClusterResponse) response.getResponse();
        assertEquals("cluster-1", body.getClusterId());
    }

    @Test
    void processDescribeFeatures_success_returnsMetadata() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeFeaturesResult.class);
        var metadata = mock(FeatureMetadata.class);
        when(metadata.supportedFeatures()).thenReturn(Map.of("group.version", new SupportedVersionRange((short) 0, (short) 1)));
        when(metadata.finalizedFeatures()).thenReturn(Map.of("group.version", new FinalizedVersionRange((short) 1, (short) 1)));
        when(metadata.finalizedFeaturesEpoch()).thenReturn(Optional.of(42L));
        when(result.featureMetadata()).thenReturn(KafkaFuture.completedFuture(metadata));
        when(admin.describeFeatures()).thenReturn(result);

        var request = new AdminDescribeFeaturesRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeFeaturesResponse) response.getResponse();
        assertEquals(42L, body.getFinalizedFeaturesEpoch());
        assertEquals(1, body.getSupportedFeatures().size());
        assertEquals("group.version", body.getSupportedFeatures().get(0).getName());
        assertEquals(1, body.getSupportedFeatures().get(0).getMaxVersion());
        assertEquals(1, body.getFinalizedFeatures().size());
        assertEquals("group.version", body.getFinalizedFeatures().get(0).getName());
    }

    @Test
    void processDescribeLogDirs_success_returnsDescriptions() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeLogDirsResult.class);
        when(result.allDescriptions()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeLogDirs(anyCollection())).thenReturn(result);

        var request = new AdminDescribeLogDirsRequest();
        setField(request, "adminId", wrapper.getId());
        setField(request, "token", wrapper.getToken());
        setField(request, "brokerIds", List.of(1));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        assertTrue(((AdminDescribeLogDirsResponse) response.getResponse()).isEmpty());
    }

    //endregion

    //region Topics

    @Test
    void processListTopics_success_returnsListings() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(ListTopicsResult.class);
        when(result.listings()).thenReturn(KafkaFuture.completedFuture(
                List.of(new TopicListing("topic-a", Uuid.randomUuid(), false))));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(result);

        var request = new AdminListTopicsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListTopicsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processListTopics_invalidPattern_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new AdminListTopicsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPattern("[");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processDescribeTopic_success_returnsDescription() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeTopicsResult.class);
        var description = new TopicDescription("topic-a", false, List.of(), Set.of(), Uuid.randomUuid());
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("topic-a", description)));
        when(admin.describeTopics(any(TopicCollection.class), any(DescribeTopicsOptions.class))).thenReturn(result);

        var request = new AdminDescribeTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeTopicResponse) response.getResponse();
        assertEquals("topic-a", body.getName());
    }

    @Test
    void processDescribeTopic_multipleResultEntries_writesOnlyOneResponse() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeTopicsResult.class);
        var descriptionA = new TopicDescription("topic-a", false, List.of(), Set.of(), Uuid.randomUuid());
        var descriptionB = new TopicDescription("topic-b", false, List.of(), Set.of(), Uuid.randomUuid());
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("topic-a", descriptionA, "topic-b", descriptionB)));
        when(admin.describeTopics(any(TopicCollection.class), any(DescribeTopicsOptions.class))).thenReturn(result);

        var request = new AdminDescribeTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        assertNull(channel.readOutbound(), "only one response should be written per request");
    }

    @Test
    void processDescribeTopic_notFound_returnsNotFound() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeTopicsResult.class);
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeTopics(any(TopicCollection.class), any(DescribeTopicsOptions.class))).thenReturn(result);

        var request = new AdminDescribeTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("missing");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processDescribeTopic_byId_success_returnsDescription() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeTopicsResult.class);
        var topicId = Uuid.randomUuid();
        var description = new TopicDescription("topic-a", false, List.of(), Set.of(), topicId);
        when(result.allTopicIds()).thenReturn(KafkaFuture.completedFuture(Map.of(topicId, description)));
        when(admin.describeTopics(any(TopicCollection.class), any(DescribeTopicsOptions.class))).thenReturn(result);

        var request = new AdminDescribeTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicId(topicId.toString());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeTopicResponse) response.getResponse();
        assertEquals("topic-a", body.getName());
    }

    @Test
    void processDescribeTopic_byId_notFound_returnsNotFound() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeTopicsResult.class);
        when(result.allTopicIds()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeTopics(any(TopicCollection.class), any(DescribeTopicsOptions.class))).thenReturn(result);

        var request = new AdminDescribeTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicId(Uuid.randomUuid().toString());

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processCreateTopic_success_returnsTopicInfo() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(result.values()).thenReturn(Map.of("topic-a", KafkaFuture.completedFuture(null)));
        when(result.topicId("topic-a")).thenReturn(KafkaFuture.completedFuture(Uuid.randomUuid()));
        when(result.numPartitions("topic-a")).thenReturn(KafkaFuture.completedFuture(3));
        when(result.replicationFactor("topic-a")).thenReturn(KafkaFuture.completedFuture(1));
        when(admin.createTopics(anyCollection(), any(CreateTopicsOptions.class))).thenReturn(result);

        var request = new AdminCreateTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");
        request.setNumPartitions(3);
        request.setReplicationFactor((short) 1);

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminCreateTopicResponse) response.getResponse();
        assertEquals(3, body.getNumPartitions());
    }

    @Test
    void processCreateTopic_topicExists_returnsConflict() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed(new org.apache.kafka.common.errors.TopicExistsException("already exists")));
        when(admin.createTopics(anyCollection(), any(CreateTopicsOptions.class))).thenReturn(result);

        var request = new AdminCreateTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.CONFLICT, response.status());
        response.release();
    }

    @Test
    void processDeleteTopic_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteTopics(any(TopicCollection.class))).thenReturn(result);

        var request = new AdminDeleteTopicRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processDeleteTopics_success_returnsResults() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(result.topicNameValues()).thenReturn(Map.of("topic-a", KafkaFuture.completedFuture(null)));
        when(admin.deleteTopics(any(TopicCollection.class))).thenReturn(result);

        var request = new AdminDeleteTopicsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicNames(List.of("topic-a"));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDeleteTopicsResponse) response.getResponse();
        assertEquals(1, body.size());
        assertTrue(body.get(0).isSuccess());
    }

    @Test
    void processCreatePartitions_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(CreatePartitionsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createPartitions(anyMap())).thenReturn(result);

        var request = new AdminCreatePartitionsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");
        request.setIncreaseTo(6);

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    //endregion

    //region Configs

    @Test
    void processDescribeTopicConfigs_success_returnsEntries() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeConfigsResult.class);
        var resource = new org.apache.kafka.common.config.ConfigResource(
                org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "topic-a");
        var config = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        when(admin.describeConfigs(anyCollection())).thenReturn(result);

        var request = new AdminDescribeTopicConfigsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminConfigResponse) response.getResponse();
        assertEquals(1, body.size());
        assertEquals("retention.ms", body.get(0).getName());
    }

    @Test
    void processDescribeGroupConfigs_success_returnsEntries() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeConfigsResult.class);
        var resource = new org.apache.kafka.common.config.ConfigResource(
                org.apache.kafka.common.config.ConfigResource.Type.GROUP, "group-a");
        var config = new Config(List.of(new ConfigEntry("session.timeout.ms", "30000")));
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        when(admin.describeConfigs(anyCollection())).thenReturn(result);

        var request = new AdminDescribeGroupConfigsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-a");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminConfigResponse) response.getResponse();
        assertEquals(1, body.size());
        assertEquals("session.timeout.ms", body.get(0).getName());
    }

    @Test
    void processAlterTopicConfig_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);

        var request = new AdminAlterTopicConfigRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");
        request.setConfigName("retention.ms");
        request.setNewValue("60000");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    @Test
    void processAlterGroupConfig_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);

        var request = new AdminAlterGroupConfigRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-a");
        request.setConfigName("session.timeout.ms");
        request.setNewValue("30000");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    @Test
    void processDeleteTopicConfig_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);

        var request = new AdminDeleteTopicConfigRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setTopicName("topic-a");
        request.setConfigName("retention.ms");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    @Test
    void processDeleteGroupConfig_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);

        var request = new AdminDeleteGroupConfigRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-a");
        request.setConfigName("session.timeout.ms");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    //endregion

    //region SCRAM

    @Test
    void processDescribeUserScramCredentials_success_returnsDescriptions() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeUserScramCredentialsResult.class);
        var description = new UserScramCredentialsDescription("user-1",
                List.of(new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096)));
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of("user-1", description)));
        when(admin.describeUserScramCredentials(any())).thenReturn(result);

        var request = new pro.savel.kafka.admin.requests.scram.AdminDescribeUserScramCredentialsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeUserScramCredentialsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processUpsertUserScramCredentials_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterUserScramCredentialsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterUserScramCredentials(anyList())).thenReturn(result);

        var request = new pro.savel.kafka.admin.requests.scram.AdminUpsertUserScramCredentialsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setUser("user-1");
        request.setMechanism("SCRAM-SHA-256");
        request.setPassword("secret");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    @Test
    void processDeleteUserScramCredentials_success_returnsOk() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterUserScramCredentialsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterUserScramCredentials(anyList())).thenReturn(result);

        var request = new pro.savel.kafka.admin.requests.scram.AdminDeleteUserScramCredentialsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setUser("user-1");
        request.setMechanism("SCRAM-SHA-256");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    //endregion

    //region ACLs

    @Test
    void processDescribeAcls_success_returnsBindings() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeAclsResult.class);
        var binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "topic-a", PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        when(result.values()).thenReturn(KafkaFuture.completedFuture(List.of(binding)));
        when(admin.describeAcls(any())).thenReturn(result);

        var request = new AdminDescribeAclsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeAclsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processCreateAcls_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(CreateAclsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createAcls(anyCollection())).thenReturn(result);

        var request = new AdminCreateAclsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        var acl = new AdminAclBinding();
        var pattern = new AdminAclBinding.ResourcePattern();
        pattern.setResourceType("TOPIC");
        pattern.setName("topic-a");
        pattern.setPatternType("LITERAL");
        var entry = new AdminAclBinding.AccessControlEntry();
        entry.setPrincipal("User:alice");
        entry.setHost("*");
        entry.setOperation("READ");
        entry.setPermissionType("ALLOW");
        acl.setPattern(pattern);
        acl.setEntry(entry);
        request.setAcls(List.of(acl));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processDeleteAcls_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DeleteAclsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(admin.deleteAcls(anyCollection())).thenReturn(result);

        var request = new AdminDeleteAclsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        var filter = new pro.savel.kafka.admin.data.AdminAclBindingFilter();
        var patternFilter = new pro.savel.kafka.admin.data.AdminAclBindingFilter.ResourcePatternFilter();
        patternFilter.setResourceType("TOPIC");
        patternFilter.setPatternType("LITERAL");
        filter.setPatternFilter(patternFilter);
        request.setFilters(List.of(filter));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    //endregion

    //region Producers

    @Test
    void processDescribeProducers_success_returnsStates() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeProducersResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeProducers(anyCollection())).thenReturn(result);

        var request = new AdminDescribeProducersRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic-a", 0)));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
    }

    @Test
    void processAbortTransaction_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AbortTransactionResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.abortTransaction(any())).thenReturn(result);

        var request = new AdminAbortTransactionRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartition(partition("topic-a", 0));
        request.setProducerId(1L);
        request.setProducerEpoch((short) 0);
        request.setCoordinatorEpoch(0);

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    //endregion

    //region Groups

    @Test
    void processListGroups_success_returnsListings() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(List.of(
                new GroupListing("group-1", java.util.Optional.of(GroupType.CLASSIC), "protocol",
                        java.util.Optional.of(GroupState.STABLE)))));
        when(admin.listGroups(any(ListGroupsOptions.class))).thenReturn(result);

        var request = new AdminListGroupsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListGroupsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processListGroups_invalidGroupType_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new AdminListGroupsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setWithTypes(List.of("not-a-real-type"));

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processListGroups_invalidGroupState_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new AdminListGroupsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setInStates(List.of("not-a-real-state"));

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    @Test
    void processDescribeConsumerGroup_success_returnsDescription() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeConsumerGroupsResult.class);
        var description = new ConsumerGroupDescription("group-1", false, List.of(), "assignor",
                org.apache.kafka.common.ConsumerGroupState.STABLE, new Node(1, "host", 9092));
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of("group-1", description)));
        when(admin.describeConsumerGroups(anyCollection(), any(DescribeConsumerGroupsOptions.class))).thenReturn(result);

        var request = new AdminDescribeConsumerGroupRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminDescribeConsumerGroupResponse) response.getResponse();
        assertEquals("group-1", body.getGroupId());
    }

    @Test
    void processDescribeConsumerGroup_notFound_returnsNotFound() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DescribeConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeConsumerGroups(anyCollection(), any(DescribeConsumerGroupsOptions.class))).thenReturn(result);

        var request = new AdminDescribeConsumerGroupRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("missing");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        response.release();
    }

    @Test
    void processDeleteConsumerGroup_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DeleteConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteConsumerGroups(anyCollection())).thenReturn(result);

        var request = new AdminDeleteConsumerGroupRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processListConsumerGroupOffsets_success_returnsOffsets() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(ListConsumerGroupOffsetsResult.class);
        var tp = new org.apache.kafka.common.TopicPartition("topic-a", 0);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(
                Map.of("group-1", Map.of(tp, new OffsetAndMetadata(10L)))));
        when(admin.listConsumerGroupOffsets(anyString(), any(ListConsumerGroupOffsetsOptions.class))).thenReturn(result);

        var request = new AdminListConsumerGroupOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListConsumerGroupOffsetsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processAlterConsumerGroupOffsets_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(AlterConsumerGroupOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap())).thenReturn(result);

        var request = new AdminAlterConsumerGroupOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");
        var offset = new AdminAlterConsumerGroupOffsetsRequest.TopicPartitionOffsetMetadata();
        offset.setTopic("topic-a");
        offset.setPartition(0);
        offset.setOffset(10);
        request.setOffsets(List.of(offset));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processDeleteConsumerGroupOffsets_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(DeleteConsumerGroupOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteConsumerGroupOffsets(anyString(), anySet())).thenReturn(result);

        var request = new AdminDeleteConsumerGroupOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");
        request.setPartitions(Set.of(partition("topic-a", 0)));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void processRemoveMembersFromConsumerGroup_success_returnsNoContent() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(RemoveMembersFromConsumerGroupResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.removeMembersFromConsumerGroup(anyString(), any(RemoveMembersFromConsumerGroupOptions.class)))
                .thenReturn(result);

        var request = new AdminRemoveMembersFromConsumerGroupRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setGroupId("group-1");
        request.setMembers(List.of("member-1"));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.getStatus());
    }

    //endregion

    //region Offsets

    @Test
    void processListEarliestOffsets_success_returnsOffsets() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(ListOffsetsResult.class);
        var tp = new org.apache.kafka.common.TopicPartition("topic-a", 0);
        var info = new ListOffsetsResult.ListOffsetsResultInfo(0L, -1L, java.util.Optional.empty());
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp, info)));
        when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(result);

        var request = new AdminListEarliestOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic-a", 0)));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListOffsetsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processListOffsets_duplicatePartitions_areDeduplicated() {
        var wrapper = addWrapper();
        var admin = wrapper.getAdmin();
        var result = mock(ListOffsetsResult.class);
        var tp = new org.apache.kafka.common.TopicPartition("topic-a", 0);
        var info = new ListOffsetsResult.ListOffsetsResultInfo(0L, -1L, java.util.Optional.empty());
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp, info)));
        when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(result);

        var request = new AdminListEarliestOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(List.of(partition("topic-a", 0), partition("topic-a", 0)));

        channel.writeInbound(bearer(request));

        AdminResponseBearer response = channel.readOutbound();
        assertEquals(HttpResponseStatus.OK, response.getStatus());
        var body = (AdminListOffsetsResponse) response.getResponse();
        assertEquals(1, body.size());
    }

    @Test
    void processListTimestampOffsets_invalidIsolationLevel_returnsBadRequest() {
        var wrapper = addWrapper();

        var request = new AdminListTimestampOffsetsRequest();
        request.setAdminId(wrapper.getId());
        request.setToken(wrapper.getToken());
        request.setPartitions(Set.of(partition("topic-a", 0)));
        request.setTimestamp(123456789L);
        request.setIsolationLevel("not-a-real-level");

        channel.writeInbound(bearer(request));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();
    }

    //endregion

    @Test
    void processRequest_unknownRequestType_returnsInternalServerError() {
        channel.writeInbound(bearer(new UnknownAdminRequest()));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
        response.release();
    }

    private record UnknownAdminRequest() implements AdminRequest {
    }
}
