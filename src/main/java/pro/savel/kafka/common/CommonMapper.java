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

package pro.savel.kafka.common;

import pro.savel.kafka.common.contract.Node;
import pro.savel.kafka.common.contract.PartitionInfo;
import pro.savel.kafka.common.contract.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;

public abstract class CommonMapper {

    public static ArrayList<Node> mapNodes(Collection<org.apache.kafka.common.Node> source) {
        if (source == null)
            return null;
        var result = new ArrayList<Node>(source.size());
        source.forEach(nodeSource -> result.add(CommonMapper.mapNode(nodeSource)));
        return result;
    }

    public static Node mapNode(org.apache.kafka.common.Node source) {
        if (source == null)
            return null;
        var result = new Node();
        result.setId(source.id());
        result.setHost(source.host());
        result.setPort(source.port());
        result.setRack(source.rack());
        return result;
    }

    public static PartitionInfo mapPartitionInfo(org.apache.kafka.common.PartitionInfo source) {
        if (source == null)
            return null;
        var result = new PartitionInfo();
        result.setTopic(source.topic());
        result.setPartition(source.partition());
        result.setLeader(mapNode(source.leader()));
        var replicas = new ArrayList<Node>(source.replicas().length);
        for (org.apache.kafka.common.Node replica : source.replicas())
            replicas.add(mapNode(replica));
        result.setReplicas(replicas);
        return result;
    }

    public static TopicPartitionInfo mapTopicPartitionInfo(org.apache.kafka.common.TopicPartitionInfo source) {
        if (source == null)
            return null;
        var result = new TopicPartitionInfo();
        result.setPartition(source.partition());
        result.setLeader(mapNode(source.leader()));
        result.setReplicas(mapNodes(source.replicas()));
        result.setIsr(mapNodes(source.isr()));
        result.setElr(mapNodes(source.elr()));
        return result;
    }
}
