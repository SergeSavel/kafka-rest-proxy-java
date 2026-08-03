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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

@Getter
public class AdminDescribeLogDirsResponse extends ArrayList<AdminDescribeLogDirsResponse.BrokersLogDirsDescription>
        implements AdminResponse {

    private AdminDescribeLogDirsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminDescribeLogDirsResponse of(
            Map<Integer, Map<String, org.apache.kafka.clients.admin.LogDirDescription>> source) {
        if (source == null)
            return null;
        var result = new AdminDescribeLogDirsResponse(source.size());
        source.forEach((brokerId, logDirsDescriptionSource) -> result
                .add(BrokersLogDirsDescription.of(brokerId, logDirsDescriptionSource)));
        return result;
    }

    @Getter
    public static class BrokersLogDirsDescription {

        private Integer brokerId;
        private Collection<LogDirDescription> logDirs;

        private BrokersLogDirsDescription() {
        }

        private static BrokersLogDirsDescription of(Integer brokerId,
                Map<String, org.apache.kafka.clients.admin.LogDirDescription> descriptionsSource) {
            var result = new BrokersLogDirsDescription();
            result.brokerId = brokerId;
            if (descriptionsSource != null) {
                result.logDirs = new ArrayList<>(descriptionsSource.size());
                descriptionsSource.forEach(
                        (path, descriptionSource) -> result.logDirs.add(LogDirDescription.of(path, descriptionSource)));
            }
            return result;
        }
    }

    @Getter
    public static class LogDirDescription {

        private String path;
        private String errorMessage;
        private Collection<TopicPartitionReplicaInfo> replicaInfos;

        private LogDirDescription() {
        }

        private static LogDirDescription of(String path, org.apache.kafka.clients.admin.LogDirDescription source) {
            var result = new LogDirDescription();
            result.path = path;
            if (source != null) {
                result.errorMessage = source.error() == null ? null : source.error().getMessage();
                var sourceReplicaInfos = source.replicaInfos();
                if (sourceReplicaInfos != null) {
                    result.replicaInfos = new ArrayList<>(sourceReplicaInfos.size());
                    sourceReplicaInfos.forEach((topicPartitionSource, replicaInfoSource) -> {
                        var item = TopicPartitionReplicaInfo.of(topicPartitionSource.topic(),
                                topicPartitionSource.partition(), replicaInfoSource);
                        result.replicaInfos.add(item);
                    });
                }
            }
            return result;
        }
    }

    @Getter
    public static class TopicPartitionReplicaInfo {

        private String topic;
        private int partition;
        private long size;
        private long offsetLag;
        private boolean isFuture;

        private TopicPartitionReplicaInfo() {
        }

        private static TopicPartitionReplicaInfo of(String topic, int partition,
                org.apache.kafka.clients.admin.ReplicaInfo source) {
            if (source == null)
                return null;
            var result = new TopicPartitionReplicaInfo();
            result.topic = topic;
            result.partition = partition;
            result.size = source.size();
            result.offsetLag = source.offsetLag();
            result.isFuture = source.isFuture();
            return result;
        }
    }
}
