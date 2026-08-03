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

package pro.savel.kafka.admin.responses;

import lombok.Getter;
import pro.savel.kafka.admin.AdminResponseMapper;
import pro.savel.kafka.common.contract.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;

@Getter
public class GroupMemberDescription {

    private String consumerId;
    private String groupInstanceId;
    private String clientId;
    private String host;
    private Collection<TopicPartition> assignment;
    private Collection<TopicPartition> targetAssignment;
    private Integer memberEpoch;
    private Boolean upgraded;

    private GroupMemberDescription() {
    }

    public static Collection<GroupMemberDescription> of(Collection<org.apache.kafka.clients.admin.MemberDescription> source) {
        if (source == null)
            return null;
        var result = new ArrayList<GroupMemberDescription>(source.size());
        source.forEach(memberDescription -> result.add(of(memberDescription)));
        return result;
    }

    public static GroupMemberDescription of(org.apache.kafka.clients.admin.MemberDescription source) {
        if (source == null)
            return null;
        var result = new GroupMemberDescription();
        result.consumerId = source.consumerId();
        result.groupInstanceId = source.groupInstanceId().orElse(null);
        result.clientId = source.clientId();
        result.host = source.host();
        result.assignment = mapMemberAssignment(source.assignment());
        result.targetAssignment = source.targetAssignment().map(GroupMemberDescription::mapMemberAssignment).orElse(null);
        result.memberEpoch = source.memberEpoch().orElse(null);
        result.upgraded = source.upgraded().orElse(null);
        return result;
    }

    public static Collection<TopicPartition> mapMemberAssignment(org.apache.kafka.clients.admin.MemberAssignment source) {
        if (source == null)
            return null;
        return TopicPartition.of(source.topicPartitions());
    }
}
