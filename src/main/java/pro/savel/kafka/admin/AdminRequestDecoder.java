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

@ChannelHandler.Sharable
public class AdminRequestDecoder extends ChannelInboundHandlerAdapter {

    public static final String URI_PREFIX = "/admin";
    private static final Logger logger = LoggerFactory.getLogger(AdminRequestDecoder.class);
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
        switch (pathMethod) {
            case "/describe-topic" -> decodeDescribeTopic(ctx, httpRequest);
            case "/list-topics" -> decodeListTopics(ctx, httpRequest);
            case "/create-topic" -> decodeCreateTopic(ctx, httpRequest);
            case "/create-topics" -> decodeCreateTopics(ctx, httpRequest);
            case "/delete-topic" -> decodeDeleteTopic(ctx, httpRequest);
            case "/delete-topics" -> decodeDeleteTopics(ctx, httpRequest);
            case "/delete-records" -> decodeDeleteRecords(ctx, httpRequest);
            case "/describe-topic-configs" -> decodeDescribeTopicConfigs(ctx, httpRequest);
            case "/describe-broker-configs" -> decodeDescribeBrokerConfigs(ctx, httpRequest);
            case "/describe-group-configs" -> decodeDescribeGroupConfigs(ctx, httpRequest);
            case "/describe-cluster" -> decodeDescribeCluster(ctx, httpRequest);
            case "/describe-features" -> decodeDescribeFeatures(ctx, httpRequest);
            case "/describe-log-dirs" -> decodeDescribeLogDirs(ctx, httpRequest);
            case "/update-feature" -> decodeUpdateFeature(ctx, httpRequest);
            case "/touch" -> decodeTouch(ctx, httpRequest);
            case "/create" -> decodeCreate(ctx, httpRequest);
            case "/release" -> decodeRemove(ctx, httpRequest);
            case "/set-topic-config" -> decodeAlterTopicConfig(ctx, httpRequest); // deprecated
            case "/alter-topic-config" -> decodeAlterTopicConfig(ctx, httpRequest);
            case "/alter-group-config" -> decodeAlterGroupConfig(ctx, httpRequest);
            case "/delete-topic-config" -> decodeDeleteTopicConfig(ctx, httpRequest);
            case "/delete-group-config" -> decodeDeleteGroupConfig(ctx, httpRequest);
            case "/describe-user-scram-credentials" -> decodeDescribeUserScramCredentials(ctx, httpRequest);
            case "/upsert-user-scram-credentials" -> decodeUpsertUserScramCredentials(ctx, httpRequest);
            case "/delete-user-scram-credentials" -> decodeDeleteUserScramCredentials(ctx, httpRequest);
            case "/describe-acls" -> decodeDescribeAcls(ctx, httpRequest);
            case "/create-acls" -> decodeCreateAcls(ctx, httpRequest);
            case "/delete-acls" -> decodeDeleteAcls(ctx, httpRequest);
            case "/create-partitions" -> decodeCreatePartitions(ctx, httpRequest);
            case "/describe-producers" -> decodeDescribeProducers(ctx, httpRequest);
            case "/abort-transaction" -> decodeAbortTransaction(ctx, httpRequest);
            case "/list-groups" -> decodeListGroups(ctx, httpRequest);
            case "/describe-classic-group" -> decodeDescribeClassicGroup(ctx, httpRequest);
            case "/describe-consumer-group" -> decodeDescribeConsumerGroup(ctx, httpRequest);
            case "/describe-share-group" -> decodeDescribeShareGroup(ctx, httpRequest);
            case "/describe-streams-group" -> decodeDescribeStreamsGroup(ctx, httpRequest);
            case "/list-consumer-group-offsets" -> decodeListConsumerGroupOffsets(ctx, httpRequest);
            case "/alter-consumer-group-offsets" -> decodeAlterConsumerGroupOffsets(ctx, httpRequest);
            case "/delete-consumer-group-offsets" -> decodeDeleteConsumerGroupOffsets(ctx, httpRequest);
            case "/remove-members-from-consumer-group" -> decodeRemoveMembersFromConsumerGroup(ctx, httpRequest);
            case "/delete-consumer-group" -> decodeDeleteConsumerGroup(ctx, httpRequest);
            case "/delete-consumer-groups" -> decodeDeleteConsumerGroups(ctx, httpRequest);
            case "/delete-share-group" -> decodeDeleteShareGroup(ctx, httpRequest);
            case "/delete-share-groups" -> decodeDeleteShareGroups(ctx, httpRequest);
            case "/delete-streams-group" -> decodeDeleteStreamsGroup(ctx, httpRequest);
            case "/delete-streams-groups" -> decodeDeleteStreamsGroups(ctx, httpRequest);
            case "/list-earliest-offsets" -> decodeListEarliestOffsets(ctx, httpRequest);
            case "/list-earliest-local-offsets" -> decodeListEarliestLocalOffsets(ctx, httpRequest);
            case "/list-latest-offsets" -> decodeListLatestOffsets(ctx, httpRequest);
            case "/list-latest-tiered-offsets" -> decodeListLatestTieredOffsets(ctx, httpRequest);
            case "/list-max-timestamp-offsets" -> decodeListMaxTimestampOffsets(ctx, httpRequest);
            case "/list-timestamp-offsets" -> decodeListTimestampOffsets(ctx, httpRequest);
            case "" -> decodeList(ctx, httpRequest);
            default -> HttpUtils.writeNotFoundAndClose(ctx);
        }
    }

    private void decodeList(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.GET) {
            decodeListRequest(ctx, httpRequest);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeCreate(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminCreateRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeRemove(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminRemoveRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeTouch(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminTouchRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeCluster(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeClusterRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeFeatures(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeFeaturesRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeUpdateFeature(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminUpdateFeatureRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeLogDirs(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeLogDirsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListTopicsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeCreateTopic(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminCreateTopicRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeCreateTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminCreateTopicsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteTopic(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteTopicRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteTopics(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteTopicsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteRecords(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteRecordsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeTopic(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeTopicRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeBrokerConfigs(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeBrokerConfigsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeTopicConfigs(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeTopicConfigsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeGroupConfigs(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeGroupConfigsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeAlterTopicConfig(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminAlterTopicConfigRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeAlterGroupConfig(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminAlterGroupConfigRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteTopicConfig(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteTopicConfigRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteGroupConfig(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteGroupConfigRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeUserScramCredentials(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeUserScramCredentialsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeUpsertUserScramCredentials(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminUpsertUserScramCredentialsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteUserScramCredentials(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteUserScramCredentialsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeAcls(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeAclsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeCreateAcls(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminCreateAclsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteAcls(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteAclsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeCreatePartitions(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminCreatePartitionsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeProducers(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeProducersRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeAbortTransaction(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminAbortTransactionRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListGroups(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListGroupsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeClassicGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeClassicGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeConsumerGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeConsumerGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeShareGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeShareGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDescribeStreamsGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDescribeStreamsGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListConsumerGroupOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListConsumerGroupOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeAlterConsumerGroupOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminAlterConsumerGroupOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteConsumerGroupOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteConsumerGroupOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeRemoveMembersFromConsumerGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminRemoveMembersFromConsumerGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteConsumerGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteConsumerGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteConsumerGroups(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteConsumerGroupsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteShareGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteShareGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteShareGroups(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteShareGroupsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteStreamsGroup(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteStreamsGroupRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeDeleteStreamsGroups(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminDeleteStreamsGroupsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListEarliestOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListEarliestOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListEarliestLocalOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListEarliestLocalOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListLatestOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListLatestOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListLatestTieredOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListLatestTieredOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListMaxTimestampOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListMaxTimestampOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
    }

    private void decodeListTimestampOffsets(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws BadRequestException, MethodNotAllowedException {
        if (httpRequest.method() == HttpMethod.POST) {
            decodeRequest(ctx, httpRequest, AdminListTimestampOffsetsRequest.class);
        } else {
            throw new MethodNotAllowedException("Unsupported HTTP method.");
        }
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
