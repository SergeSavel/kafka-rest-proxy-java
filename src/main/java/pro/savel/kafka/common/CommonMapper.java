package pro.savel.kafka.common;

import pro.savel.kafka.common.contract.Node;
import pro.savel.kafka.common.contract.PartitionInfo;

import java.util.ArrayList;

public abstract class CommonMapper {

    public static Node mapNode(org.apache.kafka.common.Node source) {
        if (source == null)
            return null;
        var result = new Node();
        result.setId(source.id());
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

}
