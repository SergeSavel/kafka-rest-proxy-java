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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import pro.savel.kafka.common.HttpUtils;
import pro.savel.kafka.common.JsonUtils;
import pro.savel.kafka.common.RequestBearer;
import pro.savel.kafka.common.Utils;
import pro.savel.kafka.common.exceptions.BadRequestException;
import pro.savel.kafka.common.exceptions.MethodNotAllowedException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@ChannelHandler.Sharable
public class AdminRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/admin";
    private static final Logger logger = LoggerFactory.getLogger(AdminRequestDecoder.class);

    private static final Map<String, Class<? extends AdminRequest>> REQUEST_TYPES = Map.<String, Class<? extends AdminRequest>>ofEntries(
            Map.entry("/create", AdminCreateRequest.class),
            Map.entry("/release", AdminRemoveRequest.class),
            Map.entry("/touch", AdminTouchRequest.class),
            Map.entry("/describe-cluster", AdminDescribeClusterRequest.class),
            Map.entry("/describe-features", AdminDescribeFeaturesRequest.class),
            Map.entry("/describe-log-dirs", AdminDescribeLogDirsRequest.class),
            Map.entry("/update-feature", AdminUpdateFeatureRequest.class),
            Map.entry("/list-topics", AdminListTopicsRequest.class),
            Map.entry("/describe-topic", AdminDescribeTopicRequest.class),
            Map.entry("/create-topic", AdminCreateTopicRequest.class),
            Map.entry("/create-topics", AdminCreateTopicsRequest.class),
            Map.entry("/delete-topic", AdminDeleteTopicRequest.class),
            Map.entry("/delete-topics", AdminDeleteTopicsRequest.class),
            Map.entry("/delete-records", AdminDeleteRecordsRequest.class),
            Map.entry("/create-partitions", AdminCreatePartitionsRequest.class),
            Map.entry("/describe-topic-configs", AdminDescribeTopicConfigsRequest.class),
            Map.entry("/describe-broker-configs", AdminDescribeBrokerConfigsRequest.class),
            Map.entry("/describe-group-configs", AdminDescribeGroupConfigsRequest.class),
            Map.entry("/set-topic-config", AdminAlterTopicConfigRequest.class), // deprecated alias
            Map.entry("/alter-topic-config", AdminAlterTopicConfigRequest.class),
            Map.entry("/alter-group-config", AdminAlterGroupConfigRequest.class),
            Map.entry("/delete-topic-config", AdminDeleteTopicConfigRequest.class),
            Map.entry("/delete-group-config", AdminDeleteGroupConfigRequest.class),
            Map.entry("/describe-user-scram-credentials", AdminDescribeUserScramCredentialsRequest.class),
            Map.entry("/upsert-user-scram-credentials", AdminUpsertUserScramCredentialsRequest.class),
            Map.entry("/delete-user-scram-credentials", AdminDeleteUserScramCredentialsRequest.class),
            Map.entry("/describe-acls", AdminDescribeAclsRequest.class),
            Map.entry("/create-acls", AdminCreateAclsRequest.class),
            Map.entry("/delete-acls", AdminDeleteAclsRequest.class),
            Map.entry("/describe-producers", AdminDescribeProducersRequest.class),
            Map.entry("/abort-transaction", AdminAbortTransactionRequest.class),
            Map.entry("/list-groups", AdminListGroupsRequest.class),
            Map.entry("/describe-classic-group", AdminDescribeClassicGroupRequest.class),
            Map.entry("/describe-consumer-group", AdminDescribeConsumerGroupRequest.class),
            Map.entry("/describe-share-group", AdminDescribeShareGroupRequest.class),
            Map.entry("/describe-streams-group", AdminDescribeStreamsGroupRequest.class),
            Map.entry("/list-consumer-group-offsets", AdminListConsumerGroupOffsetsRequest.class),
            Map.entry("/alter-consumer-group-offsets", AdminAlterConsumerGroupOffsetsRequest.class),
            Map.entry("/delete-consumer-group-offsets", AdminDeleteConsumerGroupOffsetsRequest.class),
            Map.entry("/remove-members-from-consumer-group", AdminRemoveMembersFromConsumerGroupRequest.class),
            Map.entry("/delete-consumer-group", AdminDeleteConsumerGroupRequest.class),
            Map.entry("/delete-consumer-groups", AdminDeleteConsumerGroupsRequest.class),
            Map.entry("/delete-share-group", AdminDeleteShareGroupRequest.class),
            Map.entry("/delete-share-groups", AdminDeleteShareGroupsRequest.class),
            Map.entry("/delete-streams-group", AdminDeleteStreamsGroupRequest.class),
            Map.entry("/delete-streams-groups", AdminDeleteStreamsGroupsRequest.class),
            Map.entry("/list-earliest-offsets", AdminListEarliestOffsetsRequest.class),
            Map.entry("/list-earliest-local-offsets", AdminListEarliestLocalOffsetsRequest.class),
            Map.entry("/list-latest-offsets", AdminListLatestOffsetsRequest.class),
            Map.entry("/list-latest-tiered-offsets", AdminListLatestTieredOffsetsRequest.class),
            Map.entry("/list-max-timestamp-offsets", AdminListMaxTimestampOffsetsRequest.class),
            Map.entry("/list-timestamp-offsets", AdminListTimestampOffsetsRequest.class)
    );

    private final ObjectMapper objectMapper;
    private final Validator validator;

    public AdminRequestDecoder(ObjectMapper objectMapper, ValidatorFactory validatorFactory) {
        this.objectMapper = objectMapper;
        this.validator = validatorFactory == null ? null : validatorFactory.getValidator();
    }

    private static void passBearer(ChannelHandlerContext ctx, FullHttpRequest httpRequest, AdminRequest request) {
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest httpRequest && httpRequest.uri().startsWith(URI_PREFIX)) {
            try {
                decode(ctx, httpRequest);
            } catch (BadRequestException e) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (MethodNotAllowedException e) {
                HttpUtils.writeMethodNotAllowedAndClose(ctx, Utils.combineErrorMessage(e));
            } catch (Exception e) {
                logger.error("An unexpected error occurred while decoding admin request.", e);
                HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(e));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void decode(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        var decoder = new QueryStringDecoder(httpRequest.uri(), StandardCharsets.UTF_8, true);
        var pathMethod = decoder.path().substring(URI_PREFIX.length());
        if (pathMethod.isEmpty()) {
            requireMethod(httpRequest, HttpMethod.GET);
            decodeListRequest(ctx, httpRequest);
            return;
        }
        var requestType = REQUEST_TYPES.get(pathMethod);
        if (requestType == null) {
            HttpUtils.writeNotFoundAndClose(ctx);
            return;
        }
        requireMethod(httpRequest, HttpMethod.POST);
        decodeRequest(ctx, httpRequest, requestType);
    }

    private static void requireMethod(FullHttpRequest httpRequest, HttpMethod method) throws MethodNotAllowedException {
        if (httpRequest.method() != method)
            throw new MethodNotAllowedException("Unsupported HTTP method.");
    }

    private void decodeListRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        var request = new AdminListRequest();
        var bearer = new RequestBearer(httpRequest, request);
        ctx.fireChannelRead(bearer);
    }

    private <T extends AdminRequest> void decodeRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest, Class<T> clazz) throws BadRequestException {
        var contentType = HttpUtils.getContentType(httpRequest);
        T request;
        if (HttpUtils.isJson(contentType))
            request = JsonUtils.parseJson(objectMapper, httpRequest.content(), clazz);
        else
            throw new BadRequestException("Invalid Content-Type header in request.");
        if (validator != null) {
            var violations = validator.validate(request);
            if (!violations.isEmpty()) {
                HttpUtils.writeBadRequestAndClose(ctx, Utils.combineConstraintViolationMessage(violations));
                return;
            }
        }
        passBearer(ctx, httpRequest, request);
    }

}
