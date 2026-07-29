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

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import pro.savel.kafka.admin.data.AdminAclBinding;
import pro.savel.kafka.admin.responses.*;
import pro.savel.kafka.common.contract.PartitionInfo;

import java.util.*;

public class AdminResponseMapper {

    public static Set<String> mapAclOperations(Set<AclOperation> source) {
        if (source == null)
            return null;
        var result = new HashSet<String>(source.size());
        source.forEach(aclOperation -> result.add(mapAclOperation(aclOperation)));
        return result;
    }

    public static AdminDescribeTopicResponse mapDescribeTopicResponse(TopicDescription source) {
        if (source == null)
            return null;
        var result = new AdminDescribeTopicResponse();
        result.setId(source.topicId().toString());
        result.setName(source.name());
        result.setInternal(source.isInternal());
        result.setAuthorizedOperations(mapAclOperations(source.authorizedOperations()));
        result.setPartitions(PartitionInfo.of(source.partitions()));
        return result;
    }

    public static AdminDescribeUserScramCredentialsResponse mapDescribeUserScramCredentialsResponse(Map<String, UserScramCredentialsDescription> source) {
        if (source == null)
            return null;
        var sourceDescriptions = source.values();
        var result = new AdminDescribeUserScramCredentialsResponse(sourceDescriptions.size());
        sourceDescriptions.forEach(sourceDescription -> result.add(mapScramCredentialDescription(sourceDescription)));
        return result;
    }

    private static AdminDescribeUserScramCredentialsResponse.ScramCredentialDescription mapScramCredentialDescription(UserScramCredentialsDescription source) {
        if (source == null)
            return null;
        var result = new AdminDescribeUserScramCredentialsResponse.ScramCredentialDescription();
        result.setName(source.name());
        result.setCredentialInfos(mapScramCredentialInfos(source.credentialInfos()));
        return result;
    }

    private static ArrayList<AdminDescribeUserScramCredentialsResponse.ScramCredentialInfo> mapScramCredentialInfos(Collection<ScramCredentialInfo> source) {
        if (source == null)
            return null;
        var result = new ArrayList<AdminDescribeUserScramCredentialsResponse.ScramCredentialInfo>(source.size());
        source.forEach(sourceItem -> result.add(mapScramCredentialInfo(sourceItem)));
        return result;
    }

    private static AdminDescribeUserScramCredentialsResponse.ScramCredentialInfo mapScramCredentialInfo(ScramCredentialInfo source) {
        if (source == null)
            return null;
        var result = new AdminDescribeUserScramCredentialsResponse.ScramCredentialInfo();
        result.setScramMechanism(source.mechanism().mechanismName());
        result.setIterations(source.iterations());
        return result;
    }

    public static AdminDescribeAclsResponse mapDescribeAclsResponse(Collection<AclBinding> source) {
        if (source == null)
            return null;
        var result = new AdminDescribeAclsResponse(source.size());
        source.forEach(aclBinding -> result.add(mapAclBinding(aclBinding)));
        return result;
    }

    private static AdminAclBinding mapAclBinding(AclBinding source) {
        if (source == null)
            return null;
        var result = new AdminAclBinding();
        result.setPattern(mapResourcePattern(source.pattern()));
        result.setEntry(mapAccessControlEntry(source.entry()));
        return result;
    }

    private static AdminAclBinding.ResourcePattern mapResourcePattern(ResourcePattern source) {
        if (source == null)
            return null;
        var result = new AdminAclBinding.ResourcePattern();
        result.setResourceType(source.resourceType().name());
        result.setName(source.name());
        result.setPatternType(source.patternType().name());
        return result;
    }

    private static AdminAclBinding.AccessControlEntry mapAccessControlEntry(AccessControlEntry source) {
        if (source == null)
            return null;
        var result = new AdminAclBinding.AccessControlEntry();
        result.setPrincipal(source.principal());
        result.setHost(source.host());
        result.setOperation(mapAclOperation(source.operation()));
        result.setPermissionType(mapAclPermissionType(source.permissionType()));
        return result;
    }

    public static String mapAclOperation(AclOperation source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapAclPermissionType(AclPermissionType source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static AdminDescribeProducersResponse mapDescribeProducerResponse(Map<TopicPartition, DescribeProducersResult.PartitionProducerState> source) {
        if (source == null)
            return null;
        var result = new AdminDescribeProducersResponse(source.size());
        source.forEach((topicPartition, partitionProducerState) -> result.add(mapPartitionProducerState(topicPartition, partitionProducerState)));
        return result;
    }

    private static AdminDescribeProducersResponse.PartitionProducerState mapPartitionProducerState(TopicPartition topicPartition, DescribeProducersResult.PartitionProducerState partitionProducerState) {
        if (topicPartition == null || partitionProducerState == null)
            return null;
        var activeProducers = new ArrayList<AdminDescribeProducersResponse.ProducerState>(partitionProducerState.activeProducers().size());
        partitionProducerState.activeProducers().forEach(producerState -> activeProducers.add(mapProducerState(producerState)));
        var result = new AdminDescribeProducersResponse.PartitionProducerState();
        result.setTopic(topicPartition.topic());
        result.setPartition(topicPartition.partition());
        result.setActiveProducers(activeProducers);
        return result;
    }

    private static AdminDescribeProducersResponse.ProducerState mapProducerState(ProducerState source) {
        if (source == null)
            return null;
        Long currentTransactionStartOffset = source.currentTransactionStartOffset().isPresent() ? source.currentTransactionStartOffset().getAsLong() : null;
        Integer coordinatorEpoch = source.coordinatorEpoch().isPresent() ? source.coordinatorEpoch().getAsInt() : null;
        var result = new AdminDescribeProducersResponse.ProducerState();
        result.setProducerId(source.producerId());
        result.setProducerEpoch(source.producerEpoch());
        result.setLastSequence(source.lastSequence());
        result.setLastTimestamp(source.lastTimestamp());
        result.setCurrentTransactionStartOffset(currentTransactionStartOffset);
        result.setCoordinatorEpoch(coordinatorEpoch);
        return result;
    }

    public static String mapGroupType(GroupType source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapGroupState(GroupState source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static String mapGroupState(ClassicGroupState source) {
        if (source == null)
            return null;
        return source.name();
    }

    public static Collection<pro.savel.kafka.common.contract.TopicPartition> mapMemberAssignment(MemberAssignment source) {
        if (source == null)
            return null;
        return pro.savel.kafka.common.contract.TopicPartition.of(source.topicPartitions());
    }

    public static Collection<pro.savel.kafka.common.contract.TopicPartition> mapMemberAssignment(ShareMemberAssignment source) {
        if (source == null)
            return null;
        return pro.savel.kafka.common.contract.TopicPartition.of(source.topicPartitions());
    }
}
