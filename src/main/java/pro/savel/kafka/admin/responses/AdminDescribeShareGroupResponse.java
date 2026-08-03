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
import pro.savel.kafka.common.contract.Node;
import pro.savel.kafka.common.contract.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;

@Getter
public class AdminDescribeShareGroupResponse implements AdminResponse {

    private String groupId;
    private Collection<ShareMemberDescription> members;
    private String groupState;
    private Node coordinator;
    private int groupEpoch;
    private int targetAssignmentEpoch;
    private Collection<String> authorizedOperations;

    private AdminDescribeShareGroupResponse() {
    }

    public static AdminDescribeShareGroupResponse of(org.apache.kafka.clients.admin.ShareGroupDescription source) {
        if (source == null)
            return null;
        var result = new AdminDescribeShareGroupResponse();
        result.groupId = source.groupId();
        result.members = ShareMemberDescription.of(source.members());
        result.groupState = AdminResponseMapper.mapGroupState(source.groupState());
        result.coordinator = Node.of(source.coordinator());
        result.groupEpoch = source.groupEpoch();
        result.targetAssignmentEpoch = source.targetAssignmentEpoch();
        result.authorizedOperations = AdminResponseMapper.mapAclOperations(source.authorizedOperations());
        return result;
    }

    @Getter
    public static class ShareMemberDescription {

        private String consumerId;
        private String clientId;
        private String host;
        private Collection<TopicPartition> assignment;
        private int memberEpoch;

        private ShareMemberDescription() {
        }

        private static Collection<ShareMemberDescription> of(
                Collection<org.apache.kafka.clients.admin.ShareMemberDescription> source) {
            if (source == null)
                return null;
            var result = new ArrayList<ShareMemberDescription>(source.size());
            source.forEach(memberDescription -> result.add(of(memberDescription)));
            return result;
        }

        private static ShareMemberDescription of(org.apache.kafka.clients.admin.ShareMemberDescription source) {
            if (source == null)
                return null;
            var result = new ShareMemberDescription();
            result.consumerId = source.consumerId();
            result.clientId = source.clientId();
            result.host = source.host();
            result.assignment = mapMemberAssignment(source.assignment());
            result.memberEpoch = source.memberEpoch();
            return result;
        }

        private static Collection<TopicPartition> mapMemberAssignment(
                org.apache.kafka.clients.admin.ShareMemberAssignment source) {
            if (source == null)
                return null;
            return TopicPartition.of(source.topicPartitions());
        }
    }
}
