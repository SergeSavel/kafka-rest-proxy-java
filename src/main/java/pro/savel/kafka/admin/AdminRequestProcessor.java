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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.admin.requests.*;
import pro.savel.kafka.admin.responses.AdminConfigResponse;
import pro.savel.kafka.admin.responses.AdminDescribeClusterResponse;
import pro.savel.kafka.common.CommonMapper;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;
import pro.savel.kafka.common.exceptions.UnauthenticatedException;
import pro.savel.kafka.common.exceptions.UnauthorizedException;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@ChannelHandler.Sharable
public class AdminRequestProcessor extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AdminRequestProcessor.class);

    private final AdminProvider provider = new AdminProvider();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RequestBearer bearer && bearer.request() instanceof AdminRequest) {
            try {
                processRequest(ctx, bearer);
            } catch (NotFoundException e) {
                HttpUtils.writeNotFoundAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (UnauthenticatedException e) {
                HttpUtils.writeUnauthorizedAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (UnauthorizedException e) {
                HttpUtils.writeForbiddenAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while processing admin request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, bearer.protocolVersion(), Utils.combineErrorMessage(e));
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

    public void processRequest(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException, UnauthenticatedException, UnauthorizedException {
        var requestClass = requestBearer.request().getClass();
        if (requestClass == AdminDescribeTopicRequest.class)
            processDescribeTopic(ctx, requestBearer);
        else if (requestClass == AdminCreateTopicRequest.class)
            processCreateTopic(ctx, requestBearer);
        else if (requestClass == AdminDeleteTopicRequest.class)
            processDeleteTopic(ctx, requestBearer);
        else if (requestClass == AdminListTopicsRequest.class)
            processListTopics(ctx, requestBearer);
        else if (requestClass == AdminDescribeTopicConfigsRequest.class)
            processDescribeTopicConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeBrokerConfigsRequest.class)
            processDescribeBrokerConfigs(ctx, requestBearer);
        else if (requestClass == AdminDescribeClusterRequest.class)
            processDescribeCluster(ctx, requestBearer);
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
        else
            throw new RuntimeException("Unexpected admin request type: " + requestClass.getName());
    }

    private void processList(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var wrappers = provider.getItems();
        var response = AdminResponseMapper.mapListResponse(wrappers);
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processCreate(ChannelHandlerContext ctx, RequestBearer requestBearer) {
        var request = (AdminCreateRequest) requestBearer.request();
        var wrapper = provider.createAdmin(request.getName(), request.getConfig(), request.getExpirationTimeout());
        var response = AdminResponseMapper.mapCreateResponse(wrapper);
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.CREATED, response);
        ctx.writeAndFlush(responseBearer);
    }

    private void processRemove(ChannelHandlerContext ctx, RequestBearer requestBearer) throws BadRequestException {
        var request = (AdminRemoveRequest) requestBearer.request();
        provider.removeAdmin(request.getAdminId(), request.getToken());
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminTouchRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processDescribeCluster(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeClusterRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeCluster();
        var response = new AdminDescribeClusterResponse();
        var successCounter = new AtomicInteger(4);
        var errorCounter = new AtomicInteger(1);
        describeResult.nodes().whenComplete((nodesSource, error) -> {
            if (error == null) {
                response.setNodes(CommonMapper.mapNodes(nodesSource));
                if (successCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (errorCounter.decrementAndGet() == 0) {
                logger.error("Unable to get cluster description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.clusterId().whenComplete((clusterId, error) -> {
            if (error == null) {
                response.setClusterId(clusterId);
                if (successCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (errorCounter.decrementAndGet() == 0) {
                logger.error("Unable to get cluster description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.controller().whenComplete((controllerSource, error) -> {
            if (error == null) {
                response.setController(CommonMapper.mapNode(controllerSource));
                if (successCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (errorCounter.decrementAndGet() == 0) {
                logger.error("Unable to get cluster description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.authorizedOperations().whenComplete((aclOperationsSource, error) -> {
            if (error == null) {
                response.setAuthorizedOperations(AdminResponseMapper.mapAclOperations(aclOperationsSource));
                if (successCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (errorCounter.decrementAndGet() == 0) {
                logger.error("Unable to get cluster description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminListTopicsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var topicsResult = admin.listTopics();
        topicsResult.listings().whenComplete((listings, error) -> {
            if (error == null) {
                var response = AdminResponseMapper.mapListTopicsResponse(listings);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else {
                logger.error("Unable to get topic listings.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processDescribeTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeTopics(Collections.singleton(request.getTopic()));
        describeResult.allTopicNames().whenComplete((topicNames, error) -> {
            if (error == null) {
                if (topicNames.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion(), "Topic not found.");
                    return;
                }
                for (TopicDescription topicDescription : topicNames.values()) {
                    var response = AdminResponseMapper.mapDescribeTopicResponse(topicDescription);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                }
            } else {
                logger.error("Unable to get topic description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processDescribeBrokerConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeBrokerConfigsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(request.getBrokerId()));
        processDescribeConfigs(ctx, requestBearer, admin, resource);
    }

    private void processDescribeTopicConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeTopicConfigsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        processDescribeConfigs(ctx, requestBearer, admin, resource);
    }

    private void processDescribeConfigs(ChannelHandlerContext ctx, RequestBearer requestBearer, Admin admin, ConfigResource resource) {
        var describeResult = admin.describeConfigs(Collections.singleton(resource));
        describeResult.all().whenComplete((configs, error) -> {
            if (error == null) {
                if (configs.isEmpty()) {
                    HttpUtils.writeNotFoundAndClose(ctx, requestBearer.protocolVersion(), "Broker not found.");
                    return;
                }
                configs.values().forEach(config -> {
                    AdminConfigResponse response = AdminResponseMapper.mapConfigResponse(config);
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
                });
            } else {
                logger.error("Unable to get broker config description.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processDescribeUserScramCredentials(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeUserScramCredentialsRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeUserScramCredentials(request.getUsers());
        describeResult.all().whenComplete((descriptions, error) -> {
            if (error == null) {
                var response = AdminResponseMapper.mapDescribeUserScramCredentialsResponse(descriptions);
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else if (error instanceof ClusterAuthorizationException)
                HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof ResourceNotFoundException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof DuplicateResourceException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else {
                logger.error("Unable to describe user SCRAM credentials.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processSetTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminSetTopicConfigRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), request.getNewValue());
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        var alterConfigsResult = admin.incrementalAlterConfigs(Collections.singletonMap(configResource, Collections.singleton(alterConfigOp)));
        alterConfigsResult.all().whenComplete((ignore, error) -> {
            if (error == null) {
                var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, null);
                ctx.writeAndFlush(responseBearer);
            } else if (error instanceof ClusterAuthorizationException)
                HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof TopicAuthorizationException)
                HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof UnknownTopicOrPartitionException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof InvalidRequestException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof InvalidConfigurationException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else {
                logger.error("Unable to set topic config.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processDeleteTopicConfig(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDeleteTopicConfigRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, request.getTopicName());
        var configEntry = new ConfigEntry(request.getConfigName(), null);
        var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
        var alterConfigsResult = admin.incrementalAlterConfigs(Collections.singletonMap(configResource, Collections.singleton(alterConfigOp)));
        alterConfigsResult.all().whenComplete((ignore, error) -> {
            if (error == null) {
                var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, null);
                ctx.writeAndFlush(responseBearer);
            } else if (error instanceof ClusterAuthorizationException)
                HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof TopicAuthorizationException)
                HttpUtils.writeUnauthorizedAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof UnknownTopicOrPartitionException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof InvalidRequestException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else if (error instanceof InvalidConfigurationException)
                HttpUtils.writeBadRequestAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            else {
                logger.error("Unable to delete topic config.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processCreateTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminCreateTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var newTopic = new NewTopic(request.getTopicName(), Optional.ofNullable(request.getNumPartitions()), Optional.ofNullable(request.getReplicationFactor()));
        var createResult = admin.createTopics(Collections.singleton(newTopic));
        createResult.all().whenComplete((topics, error) -> {
            if (error == null) {
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.CREATED, null));
            } else {
                logger.error("Unable to create topic.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processDeleteTopic(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDeleteTopicRequest) requestBearer.request();
        var wrapper = provider.getAdmin(request.getAdminId(), request.getToken());
        wrapper.touch();
        var admin = wrapper.getAdmin();
        var deleteResult = admin.deleteTopics(Collections.singleton(request.getTopicName()));
        deleteResult.all().whenComplete((topics, error) -> {
            if (error == null) {
                ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null));
            } else {
                logger.error("Unable to delete topic.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }
}
