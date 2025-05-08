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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.savel.kafka.admin.requests.*;
import pro.savel.kafka.admin.responses.AdminDescribeClusterResponse;
import pro.savel.kafka.common.CommonMapper;
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.NotFoundException;
import pro.savel.kafka.common.exceptions.UnauthenticatedException;
import pro.savel.kafka.common.exceptions.UnauthorizedException;

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
        if (requestClass == AdminListTopicsRequest.class)
            processListTopics(ctx, requestBearer);
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
        provider.removeItem(request.getAdminId(), request.getToken());
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processTouch(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminTouchRequest) requestBearer.request();
        var wrapper = provider.getItem(request.getAdminId(), request.getToken());
        wrapper.touch();
        var responseBearer = new AdminResponseBearer(requestBearer, HttpResponseStatus.NO_CONTENT, null);
        ctx.writeAndFlush(responseBearer);
    }

    private void processDescribeCluster(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminDescribeClusterRequest) requestBearer.request();
        var wrapper = provider.getItem(request.getAdminId(), request.getToken());
        var admin = wrapper.getAdmin();
        var describeResult = admin.describeCluster();
        var response = new AdminDescribeClusterResponse();
        AtomicInteger asyncOpCounter = new AtomicInteger(4);
        describeResult.nodes().whenComplete((nodesSource, error) -> {
            if (error == null) {
                response.setNodes(CommonMapper.mapNodes(nodesSource));
                if (asyncOpCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else {
                logger.error("Unable to get cluster nodes.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.clusterId().whenComplete((clusterId, error) -> {
            if (error == null) {
                response.setClusterId(clusterId);
                if (asyncOpCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else {
                logger.error("Unable to get cluster identifier.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.controller().whenComplete((controllerSource, error) -> {
            if (error == null) {
                response.setController(CommonMapper.mapNode(controllerSource));
                if (asyncOpCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else {
                logger.error("Unable to get cluster controller.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
        describeResult.authorizedOperations().whenComplete((aclOperationsSource, error) -> {
            if (error == null) {
                response.setAuthorizedOperations(AdminResponseMapper.mapAclOperations(aclOperationsSource));
                if (asyncOpCounter.decrementAndGet() == 0)
                    ctx.writeAndFlush(new AdminResponseBearer(requestBearer, HttpResponseStatus.OK, response));
            } else {
                logger.error("Unable to get cluster authorized operations.", error);
                HttpUtils.writeInternalServerErrorAndClose(ctx, requestBearer.protocolVersion(), error.getMessage());
            }
        });
    }

    private void processListTopics(ChannelHandlerContext ctx, RequestBearer requestBearer) throws NotFoundException, BadRequestException {
        var request = (AdminListTopicsRequest) requestBearer.request();
        var wrapper = provider.getItem(request.getAdminId(), request.getToken());
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
}
