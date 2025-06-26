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

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.acl.AclOperation;
import pro.savel.kafka.admin.responses.*;
import pro.savel.kafka.common.CommonMapper;
import pro.savel.kafka.common.contract.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class AdminResponseMapper {

    public static AdminListResponse mapListResponse(Collection<AdminWrapper> source) {
        if (source == null)
            return null;
        var result = new AdminListResponse(source.size());
        source.forEach(wrapper -> result.add(mapAdmin(wrapper)));
        return result;
    }

    private static AdminListResponse.Admin mapAdmin(AdminWrapper source) {
        if (source == null)
            return null;
        var result = new AdminListResponse.Admin();
        result.setId(source.getId());
        result.setName(source.getName());
        result.setUsername(source.getUsername());
        result.setExpiresAt(source.getExpiresAt());
        return result;
    }

    public static AdminCreateResponse mapCreateResponse(AdminWrapper source) {
        if (source == null)
            return null;
        var result = new AdminCreateResponse();
        result.setId(source.getId());
        result.setToken(source.getToken());
        return result;
    }

    public static AdminListTopicsResponse mapListTopicsResponse(Collection<TopicListing> source) {
        if (source == null)
            return null;
        var result = new AdminListTopicsResponse(source.size());
        source.forEach(listing -> result.add(mapTopicInfo(listing)));
        return result;
    }

    private static AdminListTopicsResponse.TopicInfo mapTopicInfo(TopicListing source) {
        if (source == null)
            return null;
        var result = new AdminListTopicsResponse.TopicInfo();
        result.setId(source.topicId().toString());
        result.setName(source.name());
        result.setInternal(source.isInternal());
        return result;
    }

    public static HashSet<String> mapAclOperations(Set<AclOperation> source) {
        if (source == null)
            return null;
        var result = new HashSet<String>(source.size());
        source.forEach(aclOperation -> result.add(aclOperation.name()));
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
        result.setPartitions(mapPartitions(source.partitions()));
        return result;
    }

    private static ArrayList<TopicPartitionInfo> mapPartitions(Collection<org.apache.kafka.common.TopicPartitionInfo> source) {
        if (source == null)
            return null;
        var result = new ArrayList<TopicPartitionInfo>(source.size());
        source.forEach(partitionInfoSource -> result.add(CommonMapper.mapTopicPartitionInfo(partitionInfoSource)));
        return result;
    }

    public static AdminConfigResponse mapConfigResponse(Config source) {
        if (source == null)
            return null;
        var result = new AdminConfigResponse(source.entries().size());
        source.entries().forEach(entry -> result.add(mapConfigEntry(entry)));
        return result;
    }

    private static AdminConfigResponse.Entry mapConfigEntry(ConfigEntry source) {
        if (source == null)
            return null;
        var result = new AdminConfigResponse.Entry();
        result.setName(source.name());
        result.setValue(source.value());
        result.setSource(source.source().name());
        result.setDefault(source.isDefault());
        result.setSensitive(source.isSensitive());
        result.setReadOnly(source.isReadOnly());
        result.setType(source.type().name());
        result.setDocumentation(source.documentation());
        return result;
    }
}
