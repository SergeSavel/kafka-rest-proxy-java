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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Getter
public class AdminDescribeStreamsGroupResponse implements AdminResponse {

    private String groupId;
    private int groupEpoch;
    private int targetAssignmentEpoch;
    private int topologyEpoch;
    private Collection<StreamsGroupSubtopologyDescription> subtopologies;
    private Collection<StreamsGroupMemberDescription> members;
    private String groupState;
    private Node coordinator;
    private Collection<String> authorizedOperations;

    private AdminDescribeStreamsGroupResponse() {
    }

    public static AdminDescribeStreamsGroupResponse of(org.apache.kafka.clients.admin.StreamsGroupDescription source) {
        if (source == null)
            return null;
        var result = new AdminDescribeStreamsGroupResponse();
        result.groupId = source.groupId();
        result.groupEpoch = source.groupEpoch();
        result.targetAssignmentEpoch = source.targetAssignmentEpoch();
        result.topologyEpoch = source.topologyEpoch();
        result.subtopologies = StreamsGroupSubtopologyDescription.of(source.subtopologies());
        result.members = StreamsGroupMemberDescription.of(source.members());
        result.groupState = AdminResponseMapper.mapGroupState(source.groupState());
        result.coordinator = Node.of(source.coordinator());
        result.authorizedOperations = AdminResponseMapper.mapAclOperations(source.authorizedOperations());
        return result;
    }

    @Getter
    public static class StreamsGroupMemberDescription {

        private String memberId;
        private int memberEpoch;
        private String instanceId;
        private String rackId;
        private String clientId;
        private String clientHost;
        private int topologyEpoch;
        private String processId;
        private Endpoint userEndpoint;
        private Map<String, String> clientTags;
        private Collection<TaskOffset> taskOffsets;
        private Collection<TaskOffset> taskEndOffsets;
        private StreamsGroupMemberAssignment assignment;
        private StreamsGroupMemberAssignment targetAssignment;
        private boolean isClassic;

        @Getter
        public static class Endpoint {

            private String host;
            private int port;

            private Endpoint() {
            }

            private static Endpoint of(org.apache.kafka.clients.admin.StreamsGroupMemberDescription.Endpoint source) {
                if (source == null)
                    return null;
                var result = new Endpoint();
                result.host = source.host();
                result.port = source.port();
                return result;
            }
        }

        @Getter
        public static class TaskOffset {

            private String subtopologyId;
            private int partition;
            private long offset;

            private TaskOffset() {
            }

            private static Collection<TaskOffset> of(Collection<org.apache.kafka.clients.admin.StreamsGroupMemberDescription.TaskOffset> source) {
                if (source == null)
                    return null;
                var result = new ArrayList<TaskOffset>(source.size());
                source.forEach(item -> result.add(of(item)));
                return result;
            }

            private static TaskOffset of(org.apache.kafka.clients.admin.StreamsGroupMemberDescription.TaskOffset source) {
                if (source == null)
                    return null;
                var result = new TaskOffset();
                result.subtopologyId = source.subtopologyId();
                result.partition = source.partition();
                result.offset = source.offset();
                return result;
            }
        }

        @Getter
        public static class StreamsGroupMemberAssignment {

            private Collection<TaskIds> activeTasks;
            private Collection<TaskIds> standbyTasks;
            private Collection<TaskIds> warmupTasks;

            @Getter
            public static class TaskIds {

                private String subtopologyId;
                private Collection<Integer> partitions;

                private TaskIds() {
                }

                private static TaskIds of(org.apache.kafka.clients.admin.StreamsGroupMemberAssignment.TaskIds source) {
                    if (source == null)
                        return null;
                    var result = new TaskIds();
                    result.subtopologyId = source.subtopologyId();
                    result.partitions = source.partitions();
                    return result;
                }

                private static Collection<TaskIds> of(Collection<org.apache.kafka.clients.admin.StreamsGroupMemberAssignment.TaskIds> source) {
                    if (source == null)
                        return null;
                    var result = new ArrayList<TaskIds>(source.size());
                    source.forEach(item -> result.add(of(item)));
                    return result;
                }
            }

            private StreamsGroupMemberAssignment() {
            }

            private static StreamsGroupMemberAssignment of(org.apache.kafka.clients.admin.StreamsGroupMemberAssignment source) {
                if (source == null)
                    return null;
                var result = new StreamsGroupMemberAssignment();
                result.activeTasks = TaskIds.of(source.activeTasks());
                result.standbyTasks = TaskIds.of(source.standbyTasks());
                result.warmupTasks = TaskIds.of(source.warmupTasks());
                return result;
            }
        }

        private StreamsGroupMemberDescription() {
        }

        private static Collection<StreamsGroupMemberDescription> of(Collection<org.apache.kafka.clients.admin.StreamsGroupMemberDescription> source) {
            if (source == null)
                return null;
            var result = new ArrayList<StreamsGroupMemberDescription>(source.size());
            source.forEach(item -> result.add(of(item)));
            return result;
        }

        private static StreamsGroupMemberDescription of(org.apache.kafka.clients.admin.StreamsGroupMemberDescription source) {
            if (source == null)
                return null;
            var result = new StreamsGroupMemberDescription();
            result.memberId = source.memberId();
            result.memberEpoch = source.memberEpoch();
            result.instanceId = source.instanceId().orElse(null);
            result.rackId = source.rackId().orElse(null);
            result.clientId = source.clientId();
            result.clientHost = source.clientHost();
            result.topologyEpoch = source.topologyEpoch();
            result.processId = source.processId();
            result.userEndpoint = Endpoint.of(source.userEndpoint().orElse(null));
            result.clientTags = source.clientTags();
            result.taskOffsets = TaskOffset.of(source.taskOffsets());
            result.taskEndOffsets = TaskOffset.of(source.taskEndOffsets());
            result.assignment = StreamsGroupMemberAssignment.of(source.assignment());
            result.targetAssignment = StreamsGroupMemberAssignment.of(source.targetAssignment());
            result.isClassic = source.isClassic();
            return result;
        }
    }

    @Getter
    public static class StreamsGroupSubtopologyDescription {

        private String subtopologyId;
        private Collection<String> sourceTopics;
        private Collection<String> repartitionSinkTopics;
        private Map<String, TopicInfo> stateChangelogTopics;
        private Map<String, TopicInfo> repartitionSourceTopics;

        @Getter
        public static class TopicInfo {

            private int partitions;
            private int replicationFactor;
            private Map<String, String> topicConfigs;

            private TopicInfo() {
            }

            private static TopicInfo of(org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription.TopicInfo source) {
                if (source == null)
                    return null;
                var result = new TopicInfo();
                result.partitions = source.partitions();
                result.replicationFactor = source.replicationFactor();
                result.topicConfigs = source.topicConfigs();
                return result;
            }

            private static Map<String, TopicInfo> of(Map<String, org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription.TopicInfo> source) {
                if (source == null)
                    return null;
                var result = new HashMap<String, TopicInfo>(source.size());
                source.forEach((topicName, topicInfoSource) -> result.put(topicName, of(topicInfoSource)));
                return result;
            }
        }

        private StreamsGroupSubtopologyDescription() {
        }

        private static Collection<StreamsGroupSubtopologyDescription> of(Collection<org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription> source) {
            if (source == null)
                return null;
            var result = new ArrayList<StreamsGroupSubtopologyDescription>(source.size());
            source.forEach(item -> result.add(of(item)));
            return result;
        }

        private static StreamsGroupSubtopologyDescription of(org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription source) {
            if (source == null)
                return null;
            var result = new StreamsGroupSubtopologyDescription();
            result.subtopologyId = source.subtopologyId();
            result.sourceTopics = source.sourceTopics();
            result.repartitionSinkTopics = source.repartitionSinkTopics();
            result.stateChangelogTopics = TopicInfo.of(source.stateChangelogTopics());
            result.repartitionSourceTopics = TopicInfo.of(source.repartitionSourceTopics());
            return result;
        }
    }
}
