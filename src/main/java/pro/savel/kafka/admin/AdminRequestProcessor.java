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

package pro.savel.kafka.admin;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import pro.savel.kafka.admin.requests.AdminRequest;
import pro.savel.kafka.admin.requests.acls.AdminCreateAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDeleteAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDescribeAclsRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeClusterRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeFeaturesRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeLogDirsRequest;
import pro.savel.kafka.admin.requests.cluster.AdminUpdateFeatureRequest;
import pro.savel.kafka.admin.requests.config.AdminAlterGroupConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminAlterTopicConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDeleteGroupConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDeleteTopicConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeBrokerConfigsRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeGroupConfigsRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeTopicConfigsRequest;
import pro.savel.kafka.admin.requests.group.*;
import pro.savel.kafka.admin.requests.management.AdminCreateRequest;
import pro.savel.kafka.admin.requests.management.AdminListRequest;
import pro.savel.kafka.admin.requests.management.AdminRemoveRequest;
import pro.savel.kafka.admin.requests.management.AdminTouchRequest;
import pro.savel.kafka.admin.requests.offset.*;
import pro.savel.kafka.admin.requests.producer.AdminAbortTransactionRequest;
import pro.savel.kafka.admin.requests.producer.AdminDescribeProducersRequest;
import pro.savel.kafka.admin.requests.scram.AdminDeleteUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.scram.AdminDescribeUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.scram.AdminUpsertUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.topic.*;
import pro.savel.kafka.admin.responses.*;
import pro.savel.kafka.common.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class AdminRequestProcessor extends AbstractRequestProcessor {

    private final AdminProvider provider;

    public AdminRequestProcessor(BlockingTaskExecutor blockingTaskExecutor, AdminProvider provider) {
        super("admin", AdminRequest.class, blockingTaskExecutor);
        this.provider = provider;
    }

    @Override
    protected void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var requestClass = requestBearer.request().getClass();
        if (requestClass == AdminDescribeTopicRequest.class)
            processDescribeTopic(ctx, requestBearer);
        else if (requestClass == AdminCreateTopicRequest.class)
            processCreateTopic(ctx, requestBearer);
        else if (requestClass == AdminCreateTopicsRequest.class)
            processCreateTopics(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicRequest.class)
            processDeleteTopic(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicsRequest.class)
            processDeleteTopics(ctx, requestBearer);
        else if (requestClass == AdminDeleteRecordsRequest.class)
            processDeleteRecords(ctx, requestBearer);
        else if (requestClass == AdminListTopicsRequest.class)
            processListTopics(ctx, requestBearer);
        else if (requestClass == AdminDescribeTopicConfigsRequest.class)
            processDescribeTopicConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeBrokerConfigsRequest.class)
            processDescribeBrokerConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeGroupConfigsRequest.class)
            processDescribeGroupConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeClusterRequest.class)
            processDescribeCluster(ctx, requestBearer);
        else if (requestClass == AdminDescribeFeaturesRequest.class)
            processDescribeFeatures(ctx, requestBearer);
        else if (requestClass == AdminDescribeLogDirsRequest.class)
            processDescribeLogDirs(ctx, requestBearer);
        else if (requestClass == AdminUpdateFeatureRequest.class)
            processUpdateFeature(ctx, requestBearer);
        else if (requestClass == AdminCreateRequest.class)
            processCreate(ctx, requestBearer);
        else if (requestClass == AdminRemoveRequest.class)
            processRemove(ctx, requestBearer);
        else if (requestClass == AdminTouchRequest.class)
            processTouch(ctx, requestBearer);
        else if (requestClass == AdminListRequest.class)
            processList(ctx, requestBearer);
        else if (requestClass == AdminAlterTopicConfigRequest.class)
            processAlterTopicConfig(ctx, requestBearer);
        else if (requestClass == AdminAlterGroupConfigRequest.class)
            processAlterGroupConfig(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicConfigRequest.class)
            processDeleteTopicConfig(ctx, requestBearer);
        else if (requestClass == AdminDeleteGroupConfigRequest.class)
            processDeleteGroupConfig(ctx, requestBearer);
        else if (requestClass == AdminDescribeUserScramCredentialsRequest.class)
            processDescribeUserScramCredentials(ctx, requestBearer);
        else if (requestClass == AdminUpsertUserScramCredentialsRequest.class)
            processUpsertUserScramCredentials(ctx, requestBearer);
        else if (requestClass == AdminDeleteUserScramCredentialsRequest.class)
            processDeleteUserScramCredentials(ctx, requestBearer);
        else if (requestClass == AdminDescribeAclsRequest.class)
            processDescribeAcls(ctx, requestBearer);
        else if (requestClass == AdminCreateAclsRequest.class)
            processCreateAcls(ctx, requestBearer);
        else if (requestClass == AdminDeleteAclsRequest.class)
            processDeleteAcls(ctx, requestBearer);
        else if (requestClass == AdminCreatePartitionsRequest.class)
            processCreatePartitions(ctx, requestBearer);
        else if (requestClass == AdminDescribeProducersRequest.class)
            processDescribeProducers(ctx, requestBearer);
        else if (requestClass == AdminAbortTransactionRequest.class)
            processAbortTransaction(ctx, requestBearer);
        else if (requestClass == AdminListGroupsRequest.class)
            processListGroups(ctx, requestBearer);
        else if (requestClass == AdminDescribeClassicGroupRequest.class)
            processDescribeClassicGroup(ctx, requestBearer);
        else if (requestClass == AdminDescribeConsumerGroupRequest.class)
            processDescribeConsumerGroup(ctx, requestBearer);
        else if (requestClass == AdminDescribeShareGroupRequest.class)
            processDescribeShareGroup(ctx, requestBearer);
        else if (requestClass == AdminDescribeStreamsGroupRequest.class)
            processDescribeStreamsGroup(ctx, requestBearer);
        else if (requestClass == AdminListConsumerGroupOffsetsRequest.class)
            processListConsumerGroupOffsets(ctx, requestBearer);
        else if (requestClass == AdminAlterConsumerGroupOffsetsRequest.class)
            processAlterConsumerGroupOffsets(ctx, requestBearer);
        else if (requestClass == AdminDeleteConsumerGroupOffsetsRequest.class)
            processDeleteConsumerGroupOffsets(ctx, requestBearer);
        else if (requestClass == AdminRemoveMembersFromConsumerGroupRequest.class)
            processRemoveMembersFromConsumerGroup(ctx, requestBearer);
        else if (requestClass == AdminDeleteConsumerGroupRequest.class)
            processDeleteConsumerGroup(ctx, requestBearer);
        else if (requestClass == AdminDeleteConsumerGroupsRequest.class)
            processDeleteConsumerGroups(ctx, requestBearer);
        else if (requestClass == AdminDeleteShareGroupRequest.class)
            processDeleteShareGroup(ctx, requestBearer);
        else if (requestClass == AdminDeleteShareGroupsRequest.class)
            processDeleteShareGroups(ctx, requestBearer);
        else if (requestClass == AdminDeleteStreamsGroupRequest.class)
            processDeleteStreamsGroup(ctx, requestBearer);
        else if (requestClass == AdminDeleteStreamsGroupsRequest.class)
            processDeleteStreamsGroups(ctx, requestBearer);
        else if (requestClass == AdminListEarliestOffsetsRequest.class)
            processListEarliestOffsetsRequest(ctx, requestBearer);
        else if (requestClass == AdminListEarliestLocalOffsetsRequest.class)
            processListEarliestLocalOffsetsRequest(ctx, requestBearer);
        else if (requestClass == AdminListLatestOffsetsRequest.class)
            processListLatestOffsetsRequest(ctx, requestBearer);
        else if (requestClass == AdminListLatestTieredOffsetsRequest.class)
            processListLatestTieredOffsetsRequest(ctx, requestBearer);
        else if (requestClass == AdminListMaxTimestampOffsetsRequest.class)
            processListMaxTimestampOffsetsRequest(ctx, requestBearer);
        else if (requestClass == AdminListTimestampOffsetsRequest.class)
            processListTimestampOffsetsRequest(ctx, requestBearer);
        else
            throw new RuntimeException("Unexpected admin request type: " + requestClass.getName());
    }

    // region Management

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = AdminListResponse.of(wrappers);
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateRequest) requestBearer.request();
        var owner = ctx.channel().attr(NettyAttributes.USERNAME).get();
        execute(ctx,
                () -> provider.createAdmin(request.getName(), request.getConfig(), request.getExpirationTimeout(),
                        owner),
                wrapper -> {
                    var response = AdminCreateResponse.of(wrapper);
                    var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
                    ctx.writeAndFlush(responseBearer);
                });
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminRemoveRequest) requestBearer.request();
        execute(ctx, () -> {
            provider.removeAdmin(request.getAdminId(), request.getToken());
            return null;
        }, ignored -> {
            var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
            ctx.writeAndFlush(responseBearer);
        });
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminTouchRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    // endregion

    // region Cluster

    private void processDescribeCluster(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeClusterRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeClusterOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeCluster(options);
        var nodesFuture = describeResult.nodes().toCompletionStage().toCompletableFuture();
        var clusterIdFuture = describeResult.clusterId().toCompletionStage().toCompletableFuture();
        var controllerFuture = describeResult.controller().toCompletionStage().toCompletableFuture();
        var aclFuture = describeResult.authorizedOperations().toCompletionStage().toCompletableFuture();
        whenComplete(CompletableFuture.allOf(nodesFuture, clusterIdFuture, controllerFuture, aclFuture), ctx,
                (ignored, error) -> {
                    if (error == null) {
                        var response = AdminDescribeClusterResponse.of(
                                clusterIdFuture.join(),
                                controllerFuture.join(),
                                nodesFuture.join(),
                                aclFuture.join());
                        ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    } else if (!handleError(ctx, error)) {
                        logger.error("Unable to get cluster description.", error);
                        HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
                    }
                });
    }

    private void processDescribeFeatures(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeFeaturesRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeFeaturesOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeFeatures(options);
        respondWith(describeResult.featureMetadata(), ctx, requestBearer, HttpResponseStatus.OK,
                "describe features", AdminDescribeFeaturesResponse::of);
    }

    private void processDescribeLogDirs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeLogDirsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var brokers = request.getBrokerIds();
        var options = new DescribeLogDirsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeLogDirs(brokers, options);
        respondWith(describeResult.allDescriptions(), ctx, requestBearer, HttpResponseStatus.OK,
                "get log dir descriptions", AdminDescribeLogDirsResponse::of);
    }

    private void processUpdateFeature(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminUpdateFeatureRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        FeatureUpdate.UpgradeType upgradeType;
        try {
            upgradeType = FeatureUpdate.UpgradeType.valueOf(request.getUpgradeType());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported upgrade type: " + request.getUpgradeType());
        }
        if (upgradeType == FeatureUpdate.UpgradeType.UNKNOWN)
            throw new IllegalArgumentException("Unsupported upgrade type: " + request.getUpgradeType());
        var featureUpdate = new FeatureUpdate(request.getVersionLevel(), upgradeType);
        var options = new UpdateFeaturesOptions();
        if (request.getValidateOnly() != null)
            options.validateOnly(request.getValidateOnly());
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var updateFeatures = Collections.singletonMap(request.getFeatureName(), featureUpdate);
        var updateResult = admin.updateFeatures(updateFeatures, options);
        respondWith(updateResult.all(), ctx, requestBearer, HttpResponseStatus.OK, "update feature", ignore -> null);
    }

    // endregion

    // region Topics

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListTopicsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        final Pattern pattern;
        try {
            pattern = request.getPattern() == null ? null : Pattern.compile(request.getPattern());
        } catch (PatternSyntaxException e) {
            HttpUtils.writeBadRequestAndClose(ctx, "Invalid pattern: " + e.getMessage());
            return;
        }
        var options = new ListTopicsOptions();
        var includeInternal = request.getIncludeInternal();
        if (includeInternal != null)
            options.listInternal(includeInternal);
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var topicsResult = admin.listTopics(options);
        respondWith(topicsResult.listings(), ctx, requestBearer, HttpResponseStatus.OK, "get topic listings",
                listings -> {
                    if (pattern == null)
                        return AdminListTopicsResponse.of(listings);
                    return AdminListTopicsResponse.of(listings.stream()
                            .filter(t -> pattern.matcher(t.name()).matches())
                            .toList());
                });
    }

    private void processDescribeTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeTopicRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeTopicsOptions();
        var includeAuthorizedOperations = request.getIncludeAuthorizedOperations();
        if (includeAuthorizedOperations != null)
            options = options.includeAuthorizedOperations(includeAuthorizedOperations);
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        org.apache.kafka.common.TopicCollection topicCollection;
        if (request.getTopicId() != null)
            topicCollection = org.apache.kafka.common.TopicCollection
                    .ofTopicIds(Collections.singleton(Uuid.fromString(request.getTopicId())));
        else if (request.getTopicName() != null)
            topicCollection = org.apache.kafka.common.TopicCollection
                    .ofTopicNames(Collections.singleton(request.getTopicName()));
        else
            throw new IllegalArgumentException("Topic id or name must be specified");
        var describeResult = admin.describeTopics(topicCollection, options);
        KafkaFuture<? extends Map<?, TopicDescription>> descriptions =
                request.getTopicId() != null ? describeResult.allTopicIds() : describeResult.allTopicNames();
        respondWithOrNotFound(descriptions, ctx, requestBearer, "get topic description", "Topic not found.",
                topicDescriptions -> topicDescriptions.isEmpty() ? null
                        : AdminDescribeTopicResponse.of(topicDescriptions.values().iterator().next()));
    }

    private void processCreateTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateTopicRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var newTopic = new NewTopic(request.getTopicName(), Optional.ofNullable(request.getNumPartitions()),
                Optional.ofNullable(request.getReplicationFactor()));
        var options = new CreateTopicsOptions();
        if (request.getValidateOnly() != null)
            options.validateOnly(request.getValidateOnly());
        if (request.getRetryOnQuotaViolation() != null)
            options.retryOnQuotaViolation(request.getRetryOnQuotaViolation());
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var createResult = admin.createTopics(Collections.singleton(newTopic), options);
        whenComplete(createResult.all(), ctx, (topics, error) -> {
            if (error == null) {
                for (var topicName : createResult.values().keySet()) {
                    var response = AdminCreateTopicResponse.of(
                            createResult.topicId(topicName),
                            createResult.numPartitions(topicName),
                            createResult.replicationFactor(topicName)
                    );
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    return;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to create topic.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processCreateTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateTopicsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var topicsSource = request.getTopics();
        var newTopics = new ArrayList<NewTopic>(topicsSource.size());
        topicsSource.forEach(topicSpec -> newTopics.add(new NewTopic(topicSpec.getTopicName(),
                Optional.ofNullable(topicSpec.getNumPartitions()),
                Optional.ofNullable(topicSpec.getReplicationFactor()))));
        var options = new CreateTopicsOptions();
        if (request.getValidateOnly() != null)
            options.validateOnly(request.getValidateOnly());
        if (request.getRetryOnQuotaViolation() != null)
            options.retryOnQuotaViolation(request.getRetryOnQuotaViolation());
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var createResult = admin.createTopics(newTopics, options);
        whenComplete(createResult.all(), ctx, (ignore1, ignore2) -> {
            var response = AdminCreateTopicsResponse.of(createResult);
            ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
        });
    }

    private void processDeleteTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        TopicCollection topics;
        if (request.getTopicId() != null) {
            var topicUuids = Collections.singleton(Uuid.fromString(request.getTopicId()));
            topics = TopicCollection.ofTopicIds(topicUuids);
        } else if (request.getTopicName() != null) {
            var topicNames = Collections.singleton(request.getTopicName());
            topics = TopicCollection.ofTopicNames(topicNames);
        } else
            throw new IllegalArgumentException("Topic id or name must be specified");
        var options = new DeleteTopicsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteTopics(topics, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT, "delete topic", ignore -> null);
    }

    private void processDeleteTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        TopicCollection topics;
        if (request.getTopicIds() != null) {
            var topicUuids = new ArrayList<Uuid>(request.getTopicIds().size());
            request.getTopicIds().forEach(topicId -> topicUuids.add(Uuid.fromString(topicId)));
            topics = TopicCollection.ofTopicIds(topicUuids);
        } else if (request.getTopicNames() != null) {
            topics = TopicCollection.ofTopicNames(request.getTopicNames());
        } else
            throw new IllegalArgumentException("Topic ids or names must be specified");
        var options = new DeleteTopicsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteTopics(topics, options);
        whenComplete(deleteResult.all(), ctx, (ignore1, ignore2) -> {
            AdminDeleteTopicsResponse response;
            if (request.getTopicIds() != null)
                response = AdminDeleteTopicsResponse.ofUuids(deleteResult.topicIdValues());
            else
                response = AdminDeleteTopicsResponse.ofNames(deleteResult.topicNameValues());
            ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
        });
    }

    private void processDeleteRecords(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteRecordsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var topicPartition = new TopicPartition(request.getTopic(), request.getPartition());
        var options = new DeleteRecordsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var records = Collections.singletonMap(topicPartition, RecordsToDelete.beforeOffset(request.getBeforeOffset()));
        var deleteResult = admin.deleteRecords(records, options);
        respondWith(deleteResult.lowWatermarks().get(topicPartition), ctx, requestBearer, HttpResponseStatus.OK,
                "delete records", AdminDeleteRecordsResponse::of);
    }

    private void processCreatePartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreatePartitionsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var newPartitions = NewPartitions.increaseTo(request.getIncreaseTo());
        var options = new CreatePartitionsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var createResult = admin.createPartitions(Collections.singletonMap(request.getTopicName(), newPartitions), options);
        respondWith(createResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT, "create partitions", ignore -> null);
    }

    // endregion

    // region Configs

    private void processDescribeBrokerConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeBrokerConfigsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(request.getBrokerId()));
        processDescribeConfigs(ctx, requestBearer, admin, resource, "Broker not found.", request.getTimeoutMs());
    }

    private void processDescribeTopicConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeTopicConfigsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        processDescribeConfigs(ctx, requestBearer, admin, resource, "Topic not found.", request.getTimeoutMs());
    }

    private void processDescribeGroupConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeGroupConfigsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var resource = new ConfigResource(ConfigResource.Type.GROUP, request.getGroupId());
        processDescribeConfigs(ctx, requestBearer, admin, resource, "Group not found.", request.getTimeoutMs());
    }

    private void processDescribeConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin,
                                               ConfigResource resource, String notFoundMessage, Integer timeoutMs) {
        var options = new DescribeConfigsOptions();
        if (timeoutMs != null)
            options.timeoutMs(timeoutMs);
        var describeResult = admin.describeConfigs(Collections.singleton(resource), options);
        respondWithOrNotFound(describeResult.all(), ctx, requestBearer, "describe configs", notFoundMessage,
                configs -> configs.isEmpty() ? null
                        : AdminConfigResponse.of(configs.values().iterator().next()));
    }

    private void processAlterTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminAlterTopicConfigRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), request.getNewValue());
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        processIncrementalAlterConfig(ctx, requestBearer, admin, configResource, alterConfigOp, request.getTimeoutMs());
    }

    private void processAlterGroupConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminAlterGroupConfigRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var configResource = new ConfigResource(ConfigResource.Type.GROUP, request.getGroupId());
        var configEntry = new ConfigEntry(request.getConfigName(), request.getNewValue());
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        processIncrementalAlterConfig(ctx, requestBearer, admin, configResource, alterConfigOp, request.getTimeoutMs());
    }

    private void processDeleteTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicConfigRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), null);
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
        processIncrementalAlterConfig(ctx, requestBearer, admin, configResource, alterConfigOp, request.getTimeoutMs());
    }

    private void processDeleteGroupConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteGroupConfigRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var configResource = new ConfigResource(ConfigResource.Type.GROUP, request.getGroupId());
        var configEntry = new ConfigEntry(request.getConfigName(), null);
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
        processIncrementalAlterConfig(ctx, requestBearer, admin, configResource, alterConfigOp, request.getTimeoutMs());
    }

    private void processIncrementalAlterConfig(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin,
                                               ConfigResource resource, AlterConfigOp op, Integer timeoutMs) {
        Collection<AlterConfigOp> alterConfigOps = Collections.singleton(op);
        var configs = Collections.singletonMap(resource, alterConfigOps);
        processIncrementalAlterConfigs(ctx, requestBearer, admin, configs, timeoutMs);
    }

    private void processIncrementalAlterConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer,
                                                       Admin admin, Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                       Integer timeoutMs) {
        var options = new AlterConfigsOptions();
        if (timeoutMs != null)
            options.timeoutMs(timeoutMs);
        var alterConfigsResult = admin.incrementalAlterConfigs(configs, options);
        respondWith(alterConfigsResult.all(), ctx, requestBearer, HttpResponseStatus.OK, "alter config", ignore -> null);
    }

    // endregion

    // region User SCRAM credentials

    private void processDescribeUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeUserScramCredentialsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeUserScramCredentialsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeUserScramCredentials(request.getUsers(), options);
        respondWith(describeResult.all(), ctx, requestBearer, HttpResponseStatus.OK,
                "describe user SCRAM credentials", AdminDescribeUserScramCredentialsResponse::of);
    }

    private void processUpsertUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminUpsertUserScramCredentialsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var iterations = request.getIterations() == null ? 4096 : request.getIterations();
        var credentialInfo = new ScramCredentialInfo(ScramMechanism.fromMechanismName(request.getMechanism()),
                iterations);
        var alteration = new UserScramCredentialUpsertion(request.getUser(), credentialInfo, request.getPassword());
        processAlterUserScramCredentials(ctx, requestBearer, admin, alteration, request.getTimeoutMs());
    }

    private void processDeleteUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteUserScramCredentialsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var alteration = new UserScramCredentialDeletion(request.getUser(),
                ScramMechanism.fromMechanismName(request.getMechanism()));
        processAlterUserScramCredentials(ctx, requestBearer, admin, alteration, request.getTimeoutMs());
    }

    private void processAlterUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer,
                                                         Admin admin, UserScramCredentialAlteration alteration,
                                                         Integer timeoutMs) {
        var options = new AlterUserScramCredentialsOptions();
        if (timeoutMs != null)
            options.timeoutMs(timeoutMs);
        var alterationResult = admin.alterUserScramCredentials(Collections.singletonList(alteration), options);
        respondWith(alterationResult.all(), ctx, requestBearer, HttpResponseStatus.OK,
                "alter user SCRAM credentials", ignore -> null);
    }

    // endregion

    // region Acls

    private void processDescribeAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeAclsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var filter = AdminRequestMapper.mapAclBindingFilter(request.getFilter());
        var options = new DescribeAclsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeAcls(filter, options);
        respondWith(describeResult.values(), ctx, requestBearer, HttpResponseStatus.OK,
                "describe ACLs", AdminDescribeAclsResponse::of);
    }

    private void processCreateAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateAclsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var acls = AdminRequestMapper.mapAclBindings(request.getAcls());
        var options = new CreateAclsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var createAclsResult = admin.createAcls(acls, options);
        respondWith(createAclsResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT, "create ACLs", ignore -> null);
    }

    private void processDeleteAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteAclsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var filters = AdminRequestMapper.mapAclBindingFilters(request.getFilters());
        var options = new DeleteAclsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var createAclsResult = admin.deleteAcls(filters, options);
        respondWith(createAclsResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT, "delete ACLs", ignore -> null);
    }

    // endregion

    // region Producers

    private void processDescribeProducers(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeProducersRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var options = new DescribeProducersOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var describeResult = admin.describeProducers(partitions, options);
        respondWith(describeResult.all(), ctx, requestBearer, HttpResponseStatus.OK,
                "describe producers", AdminDescribeProducersResponse::of);
    }

    private void processAbortTransaction(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminAbortTransactionRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var partition = CommonRequestMapper.mapTopicPartition(request.getPartition());
        var spec = new AbortTransactionSpec(partition, request.getProducerId(), request.getProducerEpoch(), request.getCoordinatorEpoch());
        var options = new AbortTransactionOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        respondWith(admin.abortTransaction(spec, options).all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "abort transaction", ignore -> null);
    }

    // endregion

    // region Groups

    private void processListGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListGroupsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new ListGroupsOptions();
        if (request.getWithTypes() != null) {
            var groupTypes = new HashSet<GroupType>();
            for (String groupTypeName : request.getWithTypes()) {
                var groupType = GroupType.parse(groupTypeName);
                if (groupType == null || groupType == GroupType.UNKNOWN) {
                    HttpUtils.writeBadRequestAndClose(ctx, "Invalid group type: '" + groupTypeName + "'.");
                    return;
                }
                groupTypes.add(groupType);
            }
            options = options.withTypes(groupTypes);
        }
        if (request.getWithProtocolTypes() != null) {
            var protocolTypes = new HashSet<>(request.getWithProtocolTypes());
            options = options.withProtocolTypes(protocolTypes);
        }
        if (request.getInStates() != null) {
            var groupStates = new HashSet<GroupState>();
            for (String groupStateName : request.getInStates()) {
                var groupState = GroupState.parse(groupStateName);
                if (groupState == null || groupState == GroupState.UNKNOWN) {
                    HttpUtils.writeBadRequestAndClose(ctx, "Invalid group state: '" + groupStateName + "'.");
                    return;
                }
                groupStates.add(groupState);
            }
            options = options.inGroupStates(groupStates);
        }
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var listGroupsResult = admin.listGroups(options);
        respondWith(listGroupsResult.all(), ctx, requestBearer, HttpResponseStatus.OK,
                "get group listings", AdminListGroupsResponse::of);
    }

    private void processDescribeClassicGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeClassicGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeClassicGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeClassicGroups(groupIds, options);
        respondWithOrNotFound(describeResult.all(), ctx, requestBearer, "get classic group description",
                "Classic group not found.",
                descriptions -> descriptions.isEmpty() ? null
                        : AdminDescribeClassicGroupResponse.of(descriptions.values().iterator().next()));
    }

    private void processDescribeConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeConsumerGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeConsumerGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeConsumerGroups(groupIds, options);
        respondWithOrNotFound(describeResult.all(), ctx, requestBearer, "get consumer group description",
                "Consumer group not found.",
                descriptions -> descriptions.isEmpty() ? null
                        : AdminDescribeConsumerGroupResponse.of(descriptions.values().iterator().next()));
    }

    private void processDescribeShareGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeShareGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeShareGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeShareGroups(groupIds, options);
        respondWithOrNotFound(describeResult.all(), ctx, requestBearer, "get share group description",
                "Share group not found.",
                descriptions -> descriptions.isEmpty() ? null
                        : AdminDescribeShareGroupResponse.of(descriptions.values().iterator().next()));
    }

    private void processDescribeStreamsGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeStreamsGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var options = new DescribeStreamsGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeStreamsGroups(groupIds, options);
        respondWithOrNotFound(describeResult.all(), ctx, requestBearer, "get streams group description",
                "Streams group not found.",
                descriptions -> descriptions.isEmpty() ? null
                        : AdminDescribeStreamsGroupResponse.of(descriptions.values().iterator().next()));
    }

    private void processListConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListConsumerGroupOffsetsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupId = request.getGroupId();
        var options = new ListConsumerGroupOffsetsOptions();
        if (request.getRequireStable() != null)
            options = options.requireStable(request.getRequireStable());
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var listResult = admin.listConsumerGroupOffsets(groupId, options);
        respondWithOrNotFound(listResult.all(), ctx, requestBearer, "list consumer group offsets",
                "Consumer group not found.",
                offsets -> offsets.isEmpty() ? null
                        : AdminListConsumerGroupOffsetsResponse.of(offsets.values().iterator().next()));
    }

    private void processAlterConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminAlterConsumerGroupOffsetsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupId = request.getGroupId();
        var offsets = AdminRequestMapper.mapTopicPartitionOffsetMetadata(request.getOffsets());
        var options = new AlterConsumerGroupOffsetsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var alterResult = admin.alterConsumerGroupOffsets(groupId, offsets, options);
        respondWith(alterResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "alter consumer group offsets", ignore -> null);
    }

    private void processDeleteConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupOffsetsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupId = request.getGroupId();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var options = new DeleteConsumerGroupOffsetsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteConsumerGroupOffsets(groupId, partitions, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete consumer group offsets", ignore -> null);
    }

    private void processRemoveMembersFromConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminRemoveMembersFromConsumerGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupId = request.getGroupId();
        RemoveMembersFromConsumerGroupOptions options;
        if (request.getMembers() == null)
            options = new RemoveMembersFromConsumerGroupOptions();
        else {
            var members = request.getMembers().stream()
                    .distinct()
                    .map(MemberToRemove::new)
                    .toList();
            options = new RemoveMembersFromConsumerGroupOptions(members);
        }
        if (request.getReason() != null)
            options.reason(request.getReason());
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var removeResult = admin.removeMembersFromConsumerGroup(groupId, options);
        respondWith(removeResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "remove members from consumer group", ignore -> null);
    }

    private void processDeleteConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = Collections.singleton(request.getGroupId());
        var options = new DeleteConsumerGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteConsumerGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete consumer group", ignore -> null);
    }

    private void processDeleteConsumerGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = request.getGroupIds();
        var options = new DeleteConsumerGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteConsumerGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete consumer groups", ignore -> null);
    }

    private void processDeleteShareGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteShareGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = Collections.singleton(request.getGroupId());
        var options = new DeleteShareGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteShareGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete share group", ignore -> null);
    }

    private void processDeleteShareGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteShareGroupsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = request.getGroupIds();
        var options = new DeleteShareGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteShareGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete share groups", ignore -> null);
    }

    private void processDeleteStreamsGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteStreamsGroupRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = Collections.singleton(request.getGroupId());
        var options = new DeleteStreamsGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteStreamsGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete streams group", ignore -> null);
    }

    private void processDeleteStreamsGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteStreamsGroupsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var groupIds = request.getGroupIds();
        var options = new DeleteStreamsGroupsOptions();
        if (request.getTimeoutMs() != null)
            options.timeoutMs(request.getTimeoutMs());
        var deleteResult = admin.deleteStreamsGroups(groupIds, options);
        respondWith(deleteResult.all(), ctx, requestBearer, HttpResponseStatus.NO_CONTENT,
                "delete streams groups", ignore -> null);
    }

    // endregion

    // region Offsets

    private void processListEarliestOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var offsetSpec = OffsetSpec.earliest();
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListEarliestLocalOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var offsetSpec = OffsetSpec.earliestLocal();
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListLatestOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var offsetSpec = OffsetSpec.latest();
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListLatestTieredOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var offsetSpec = OffsetSpec.latestTiered();
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListMaxTimestampOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var offsetSpec = OffsetSpec.maxTimestamp();
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListTimestampOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListTimestampOffsetsRequest) requestBearer.request();
        var offsetSpec = OffsetSpec.forTimestamp(request.getTimestamp());
        processListOffsetsRequest(ctx, requestBearer, offsetSpec);
    }

    private void processListOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer,
                                           OffsetSpec offsetSpec) {
        var request = (AdminListOffsetsRequest) requestBearer.request();
        var admin = getAdmin(request.getAdminId(), request.getToken());
        var topicPartitionOffsets = request.getPartitions().stream()
                .collect(Collectors.toMap(CommonRequestMapper::mapTopicPartition, topicPartition -> offsetSpec,
                        (existing, duplicate) -> existing));
        var options = new ListOffsetsOptions();
        if (request.getIsolationLevel() != null)
            try {
                var isolationLevel = IsolationLevel.valueOf(request.getIsolationLevel());
                options = new ListOffsetsOptions(isolationLevel);
            } catch (IllegalArgumentException e) {
                HttpUtils.writeBadRequestAndClose(ctx, e.getMessage());
                return;
            }
        if (request.getTimeoutMs() != null)
            options = options.timeoutMs(request.getTimeoutMs());
        var listOffsetsResult = admin.listOffsets(topicPartitionOffsets, options);
        respondWith(listOffsetsResult.all(), ctx, requestBearer, HttpResponseStatus.OK,
                "list offsets", AdminListOffsetsResponse::of);
    }

    // endregion

    private Admin getAdmin(String id, String token) {
        var wrapper = provider.getAdmin(id, token);
        wrapper.touch();
        return wrapper.getAdmin();
    }

    /**
     * Registers the completion on the future under {@link #ensureResponse}: an exception thrown by
     * the completion itself would otherwise be swallowed by the future, leaving the client without
     * a response and the connection stuck with reading disabled.
     */
    private <T> void whenComplete(KafkaFuture<T> future, ChannelHandlerContext ctx, Completion<T> completion) {
        future.whenComplete((result, error) -> ensureResponse(ctx, () -> completion.complete(result, error)));
    }

    private <T> void whenComplete(CompletableFuture<T> future, ChannelHandlerContext ctx, Completion<T> completion) {
        future.whenComplete((result, error) -> ensureResponse(ctx, () -> completion.complete(result, error)));
    }

    /**
     * The common shape of an admin endpoint: map the completed Kafka result to a response and write
     * it, or report the failure. A null response means an empty body.
     */
    private <T> void respondWith(KafkaFuture<T> future, ChannelHandlerContext ctx, RequestBearer requestBearer,
                                 HttpResponseStatus status, String action, Function<T, AdminResponse> mapper) {
        whenComplete(future, ctx, (result, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, status, mapper.apply(result)));
            else
                handleCompletionError(ctx, action, error);
        });
    }

    /**
     * Same as {@link #respondWith} for the single-entity describes: a null response means the entity
     * is not there, answered with 404.
     */
    private <T> void respondWithOrNotFound(KafkaFuture<T> future, ChannelHandlerContext ctx, RequestBearer requestBearer,
                                           String action, String notFoundMessage, Function<T, AdminResponse> mapper) {
        whenComplete(future, ctx, (result, error) -> {
            if (error == null) {
                var response = mapper.apply(result);
                if (response == null)
                    HttpUtils.writeNotFoundAndClose(ctx, notFoundMessage);
                else
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else
                handleCompletionError(ctx, action, error);
        });
    }

    private void handleCompletionError(ChannelHandlerContext ctx, String action, Throwable error) {
        if (!handleError(ctx, error)) {
            logger.error("Unable to {}.", action, error);
            HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
        }
    }

    @FunctionalInterface
    private interface Completion<T> {
        void complete(T result, Throwable error) throws Exception;
    }
}
