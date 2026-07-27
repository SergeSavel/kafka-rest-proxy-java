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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.admin.requests.AdminRequest;
import pro.savel.kafka.admin.requests.acls.AdminCreateAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDeleteAclsRequest;
import pro.savel.kafka.admin.requests.acls.AdminDescribeAclsRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeClusterRequest;
import pro.savel.kafka.admin.requests.cluster.AdminDescribeLogDirsRequest;
import pro.savel.kafka.admin.requests.config.AdminDeleteTopicConfigRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeBrokerConfigsRequest;
import pro.savel.kafka.admin.requests.config.AdminDescribeTopicConfigsRequest;
import pro.savel.kafka.admin.requests.config.AdminSetTopicConfigRequest;
import pro.savel.kafka.admin.requests.group.*;
import pro.savel.kafka.admin.requests.management.AdminCreateRequest;
import pro.savel.kafka.admin.requests.management.AdminListRequest;
import pro.savel.kafka.admin.requests.management.AdminRemoveRequest;
import pro.savel.kafka.admin.requests.management.AdminTouchRequest;
import pro.savel.kafka.admin.requests.offset.*;
import pro.savel.kafka.admin.requests.producer.AdminDescribeProducersRequest;
import pro.savel.kafka.admin.requests.scram.AdminDeleteUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.scram.AdminDescribeUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.scram.AdminUpsertUserScramCredentialsRequest;
import pro.savel.kafka.admin.requests.topic.*;
import pro.savel.kafka.admin.responses.*;
import pro.savel.kafka.common.*;
import pro.savel.kafka.common.contract.Node;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

@ChannelHandler.Sharable
public class AdminRequestProcessor extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AdminRequestProcessor.class);

    private final AdminProvider provider = new AdminProvider();

//region Overrides

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && bearer.request() instanceof AdminRequest) {
            try {
                processRequest(ctx, bearer);
            } catch (Exception e) {
                if (!handleError(ctx, e)) {
                    logger.error("An unexpected error occurred while processing admin request.", e);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("An error occurred while processing admin request.", cause);
        ctx.close();
    }

    @Override
    public void close() {
        provider.close();
    }

//endregion

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var requestClass = requestBearer.request().getClass();
        if (requestClass == AdminDescribeTopicRequest.class)
            processDescribeTopic(ctx, requestBearer);
        else if (requestClass == AdminCreateTopicRequest.class)
            processCreateTopic(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicRequest.class)
            processDeleteTopic(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicsRequest.class)
            processDeleteTopics(ctx, requestBearer);
        else if (requestClass == AdminListTopicsRequest.class)
            processListTopics(ctx, requestBearer);
        else if (requestClass == AdminDescribeTopicConfigsRequest.class)
            processDescribeTopicConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeBrokerConfigsRequest.class)
            processDescribeBrokerConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeClusterRequest.class)
            processDescribeCluster(ctx, requestBearer);
        else if (requestClass == AdminDescribeLogDirsRequest.class)
            processDescribeLogDirs(ctx, requestBearer);
        else if (requestClass == AdminCreateRequest.class)
            processCreate(ctx, requestBearer);
        else if (requestClass == AdminRemoveRequest.class)
            processRemove(ctx, requestBearer);
        else if (requestClass == AdminTouchRequest.class)
            processTouch(ctx, requestBearer);
        else if (requestClass == AdminListRequest.class)
            processList(ctx, requestBearer);
        else if (requestClass == AdminSetTopicConfigRequest.class)
            processSetTopicConfig(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicConfigRequest.class)
            processDeleteTopicConfig(ctx, requestBearer);
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

//region Management

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = AdminResponseMapper.mapListResponse(wrappers);
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateRequest) requestBearer.request();
        var owner = ctx.channel().attr(NettyAttributes.USERNAME).get();
        var wrapper = provider.createAdmin(request.getName(), request.getConfig(), request.getExpirationTimeout(), owner);
        var response = AdminResponseMapper.mapCreateResponse(wrapper);
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminRemoveRequest) requestBearer.request();
        provider.removeAdmin(request.getAdminId(), request.getToken());
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminTouchRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

//endregion

//region Cluster

    private void processDescribeCluster(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeClusterRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeCluster();
        var nodesFuture = describeResult.nodes().toCompletionStage().toCompletableFuture();
        var clusterIdFuture = describeResult.clusterId().toCompletionStage().toCompletableFuture();
        var controllerFuture = describeResult.controller().toCompletionStage().toCompletableFuture();
        var aclFuture = describeResult.authorizedOperations().toCompletionStage().toCompletableFuture();
        CompletableFuture.allOf(nodesFuture, clusterIdFuture, controllerFuture, aclFuture)
            .whenComplete((ignored, error) -> {
                if (error == null) {
                    var response = new AdminDescribeClusterResponse();
                    response.setNodes(Node.of(nodesFuture.join()));
                    response.setClusterId(clusterIdFuture.join());
                    response.setController(Node.of(controllerFuture.join()));
                    response.setAuthorizedOperations(AdminResponseMapper.mapAclOperations(aclFuture.join()));
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                } else if (!handleError(ctx, error)) {
                    logger.error("Unable to get cluster description.", error);
                    HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
                }
            });
    }

    private void processDescribeLogDirs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeLogDirsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var brokers = request.getBrokerIds();
        var describeResult = admin.describeLogDirs(brokers);
        describeResult.allDescriptions().whenComplete((descriptions, error) -> {
            if (error == null) {
                var response = AdminDescribeLogDirsResponse.of(descriptions);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get log dir descriptions.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region Topics

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListTopicsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new ListTopicsOptions();
        var includeInternal = request.getIncludeInternal();
        if (includeInternal != null)
            options.listInternal(includeInternal);
        var topicsResult = admin.listTopics(options);
        topicsResult.listings().whenComplete((listings, error) -> {
            if (error == null) {
                if (request.getPattern() != null) {
                    Pattern pattern;
                    try {
                        pattern = Pattern.compile(request.getPattern());
                    } catch (PatternSyntaxException e) {
                        throw new BadRequestException("Invalid pattern.", e);
                    }
                    listings.removeIf(topicListing -> !pattern.matcher(topicListing.name()).matches());
                }
                var response = AdminListTopicsResponse.of(listings);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get topic listings.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDescribeTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new DescribeTopicsOptions();
        var includeAuthorizedOperations = request.getIncludeAuthorizedOperations();
        if (includeAuthorizedOperations != null)
            options = options.includeAuthorizedOperations(includeAuthorizedOperations);
        org.apache.kafka.common.TopicCollection topicCollection;
        if (request.getTopicId() != null)
            topicCollection = org.apache.kafka.common.TopicCollection.ofTopicIds(Collections.singleton(request.getTopicId()));
        else if (request.getTopicName() != null)
            topicCollection = org.apache.kafka.common.TopicCollection.ofTopicNames(Collections.singleton(request.getTopicName()));
        else
            throw new IllegalArgumentException("Topic name or id must be specified");
        var describeResult = admin.describeTopics(topicCollection, options);
        describeResult.allTopicNames().whenComplete((topicNames, error) -> {
            if (error == null) {
                if (topicNames.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Topic not found.");
                    return;
                }
                for (TopicDescription topicDescription : topicNames.values()) {
                    var response = AdminResponseMapper.mapDescribeTopicResponse(topicDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get topic description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processCreateTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var newTopic = new NewTopic(request.getTopicName(), Optional.ofNullable(request.getNumPartitions()), Optional.ofNullable(request.getReplicationFactor()));
        var createResult = admin.createTopics(Collections.singleton(newTopic));
        createResult.all().whenComplete((topics, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to create topic.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var topics = Collections.singleton(request.getTopicName());
        var deleteResult = admin.deleteTopics(topics);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete topic.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var topics = request.getTopicNames();
        var deleteResult = admin.deleteTopics(topics);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete topics.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processCreatePartitions(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreatePartitionsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var newPartitions = NewPartitions.increaseTo(request.getIncreaseTo());
        var createResult = admin.createPartitions(Collections.singletonMap(request.getTopicName(), newPartitions));
        createResult.all().whenComplete((topics, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to create partitions.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region Configs

    private void processDescribeBrokerConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeBrokerConfigsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(request.getBrokerId()));
        processDescribeConfigs(ctx, requestBearer, admin, resource);
    }

    private void processDescribeTopicConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeTopicConfigsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        processDescribeConfigs(ctx, requestBearer, admin, resource);
    }

    private static void processDescribeConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin, ConfigResource resource) {
        var describeResult = admin.describeConfigs(Collections.singleton(resource));
        describeResult.all().whenComplete((configs, error) -> {
            if (error == null) {
                if (configs.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Broker not found.");
                    return;
                }
                configs.values().forEach(config -> {
                    AdminConfigResponse response = AdminResponseMapper.mapConfigResponse(config);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                });
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get broker config description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processSetTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminSetTopicConfigRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), request.getNewValue());
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        Collection<AlterConfigOp> alterConfigOps = Collections.singleton(alterConfigOp);
        var configs = Collections.singletonMap(configResource, alterConfigOps);
        processIncrementalAlterConfigs(ctx, requestBearer, admin, configs);
    }

    private void processDeleteTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteTopicConfigRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), null);
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
        Collection<AlterConfigOp> alterConfigOps = Collections.singleton(alterConfigOp);
        var configs = Collections.singletonMap(configResource, alterConfigOps);
        processIncrementalAlterConfigs(ctx, requestBearer, admin, configs);
    }

    private static void processIncrementalAlterConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin, Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        var alterConfigsResult = admin.incrementalAlterConfigs(configs);
        alterConfigsResult.all().whenComplete((ignore, error) -> {
            if (error == null) {
                var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, null);
                ctx.writeAndFlush(responseBearer);
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to alter topic config.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region User SCRAM credentials

    private void processDescribeUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeUserScramCredentialsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeUserScramCredentials(request.getUsers());
        describeResult.all().whenComplete((descriptions, error) -> {
            if (error == null) {
                var response = AdminResponseMapper.mapDescribeUserScramCredentialsResponse(descriptions);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to describe user SCRAM credentials.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processUpsertUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminUpsertUserScramCredentialsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var iterations = request.getIterations() == null ? 4096 : request.getIterations();
        var credentialInfo = new ScramCredentialInfo(ScramMechanism.fromMechanismName(request.getMechanism()), iterations);
        var alteration = new UserScramCredentialUpsertion(request.getUser(), credentialInfo, request.getPassword());
        processAlterUserScramCredentials(ctx, requestBearer, admin, alteration);
    }

    private void processDeleteUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteUserScramCredentialsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var alteration = new UserScramCredentialDeletion(request.getUser(), ScramMechanism.fromMechanismName(request.getMechanism()));
        processAlterUserScramCredentials(ctx, requestBearer, admin, alteration);
    }

    private static void processAlterUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin, UserScramCredentialAlteration alteration) {
        var alterationResult = admin.alterUserScramCredentials(Collections.singletonList(alteration));
        alterationResult.all().whenComplete((ignore, error) -> {
            if (error == null) {
                var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, null);
                ctx.writeAndFlush(responseBearer);
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to alter user SCRAM credentials.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region Acls

    private void processDescribeAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeAclsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var filter = AdminRequestMapper.mapAclBindingFilter(request.getFilter());
        var describeResult = admin.describeAcls(filter);
        describeResult.values().whenComplete((aclBindings, error) -> {
            if (error == null) {
                var response = AdminResponseMapper.mapDescribeAclsResponse(aclBindings);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to describe ACLs.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processCreateAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateAclsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var acls = AdminRequestMapper.mapAclBindings(request.getAcls());
        var createAclsResult = admin.createAcls(acls);
        createAclsResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to create ACLs.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteAcls(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteAclsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var filters = AdminRequestMapper.mapAclBindingFilters(request.getFilters());
        var createAclsResult = admin.deleteAcls(filters);
        createAclsResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete ACLs.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region Producers

    private void processDescribeProducers(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeProducersRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var describeResult = admin.describeProducers(partitions);
        describeResult.all().whenComplete((producerStates, error) -> {
            if (error == null) {
                var response = AdminResponseMapper.mapDescribeProducerResponse(producerStates);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to describe producers.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

//endregion

//region Groups

    private void processListGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListGroupsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
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
        var listGroupsResult = admin.listGroups(options);
        listGroupsResult.all().whenComplete((groupListings, error) -> {
            if (error == null) {
                var response = AdminListGroupsResponse.of(groupListings);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get group listings.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDescribeClassicGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeClassicGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new DescribeClassicGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeClassicGroups(groupIds, options);
        describeResult.all().whenComplete((classicGroupDescriptions, error) -> {
            if (error == null) {
                if (classicGroupDescriptions.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Classic group not found.");
                    return;
                }
                for (var classicGroupDescription : classicGroupDescriptions.values()) {
                    var response = AdminDescribeClassicGroupResponse.of(classicGroupDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    break;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get classic group description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDescribeConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeConsumerGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new DescribeConsumerGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeConsumerGroups(groupIds, options);
        describeResult.all().whenComplete((consumerGroupDescriptions, error) -> {
            if (error == null) {
                if (consumerGroupDescriptions.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Consumer group not found.");
                    return;
                }
                for (var consumerGroupDescription : consumerGroupDescriptions.values()) {
                    var response = AdminDescribeConsumerGroupResponse.of(consumerGroupDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    break;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get consumer group description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDescribeShareGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeShareGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new DescribeShareGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeShareGroups(groupIds, options);
        describeResult.all().whenComplete((shareGroupDescriptions, error) -> {
            if (error == null) {
                if (shareGroupDescriptions.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Share group not found.");
                    return;
                }
                for (var shareGroupDescription : shareGroupDescriptions.values()) {
                    var response = AdminDescribeShareGroupResponse.of(shareGroupDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    break;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get share group description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDescribeStreamsGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDescribeStreamsGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var options = new DescribeStreamsGroupsOptions();
        if (request.getIncludeAuthorizedOperations() != null)
            options = options.includeAuthorizedOperations(request.getIncludeAuthorizedOperations());
        var groupIds = Collections.singleton(request.getGroupId());
        var describeResult = admin.describeStreamsGroups(groupIds, options);
        describeResult.all().whenComplete((streamsGroupDescriptions, error) -> {
            if (error == null) {
                if (streamsGroupDescriptions.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Streams group not found.");
                    return;
                }
                for (var streamsGroupDescription : streamsGroupDescriptions.values()) {
                    var response = AdminDescribeStreamsGroupResponse.of(streamsGroupDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    break;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to get streams group description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processListConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminListConsumerGroupOffsetsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupId = request.getGroupId();
        var options = new ListConsumerGroupOffsetsOptions();
        if (request.getRequireStable() != null)
            options = options.requireStable(request.getRequireStable());
        var listResult = admin.listConsumerGroupOffsets(groupId, options);
        listResult.all().whenComplete((offsets, error) -> {
            if (error == null) {
                if (offsets.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, "Consumer group not found.");
                    return;
                }
                for (var consumerGroupOffsets : offsets.values()) {
                    var response = AdminListConsumerGroupOffsetsResponse.of(consumerGroupOffsets);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                    break;
                }
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to list consumer group offsets.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processAlterConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminAlterConsumerGroupOffsetsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupId = request.getGroupId();
        var offsets = AdminRequestMapper.mapTopicPartitionOffsetMetadata(request.getOffsets());
        var alterResult = admin.alterConsumerGroupOffsets(groupId, offsets);
        alterResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to alter consumer group offsets.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteConsumerGroupOffsets(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupOffsetsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupId = request.getGroupId();
        var partitions = CommonRequestMapper.mapPartitions(request.getPartitions());
        var deleteResult = admin.deleteConsumerGroupOffsets(groupId, partitions);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete consumer group offsets.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processRemoveMembersFromConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminRemoveMembersFromConsumerGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
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
        var removeResult = admin.removeMembersFromConsumerGroup(groupId, options);
        removeResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to remove members from consumer group.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteConsumerGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = Collections.singleton(request.getGroupId());
        var deleteResult = admin.deleteConsumerGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete consumer group.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteConsumerGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteConsumerGroupsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = request.getGroupIds();
        var deleteResult = admin.deleteConsumerGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete consumer groups.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteShareGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteShareGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = Collections.singleton(request.getGroupId());
        var deleteResult = admin.deleteShareGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete share group.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteShareGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteShareGroupsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = request.getGroupIds();
        var deleteResult = admin.deleteShareGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete share groups.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteStreamsGroup(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteStreamsGroupRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = Collections.singleton(request.getGroupId());
        var deleteResult = admin.deleteStreamsGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete streams group.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

    private void processDeleteStreamsGroups(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminDeleteStreamsGroupsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var groupIds = request.getGroupIds();
        var deleteResult = admin.deleteStreamsGroups(groupIds);
        deleteResult.all().whenComplete((ignore, error) -> {
            if (error == null)
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            else if (!handleError(ctx, error)) {
                logger.error("Unable to delete streams groups.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

// endregion

//region Offsets

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

    private void processListOffsetsRequest(ChannelHandlerContext ctx, RequestBearer requestBearer, OffsetSpec offsetSpec) {
        var request = (AdminListOffsetsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var topicPartitionOffsets = request.getPartitions().stream()
                .collect(Collectors.toMap(CommonRequestMapper::mapTopicPartition, topicPartition -> offsetSpec));
        var options = new ListOffsetsOptions();
        if (request.getIsolationLevel() != null)
            try {
                var isolationLevel = IsolationLevel.valueOf(request.getIsolationLevel());
                options = new ListOffsetsOptions(isolationLevel);
            } catch (IllegalArgumentException e) {
                HttpUtils.writeBadRequestAndClose(ctx, e.getMessage());
                return;
            }
        var listOffsetsResult = admin.listOffsets(topicPartitionOffsets, options);
        listOffsetsResult.all().whenComplete((offsets, error) -> {
            if (error == null) {
                var response = AdminListOffsetsResponse.of(offsets);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (!handleError(ctx, error)) {
                logger.error("Unable to list offsets.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, error.getMessage());
            }
        });
    }

// endregion

    private static boolean handleError(ChannelHandlerContext ctx, Throwable error) {
        var handled = true;
        if (error instanceof java.util.concurrent.CompletionException && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (error instanceof org.apache.kafka.common.errors.TimeoutException && error.getCause() != null)
            handled = handleError(ctx, error.getCause());
        else if (!CommonErrors.handle(ctx, error))
            handled = false;
        return handled;
    }
}
