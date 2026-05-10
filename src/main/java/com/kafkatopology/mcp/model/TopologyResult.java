package com.kafkatopology.mcp.model;

import java.util.List;

public record TopologyResult(ClusterInfo cluster, List<TopicInfo> topics) {

    public record ClusterInfo(String clusterId, String controller, List<String> brokers) {}

    public record TopicInfo(String name, int partitionCount, int replicationFactor, List<PartitionInfo> partitions, List<String> consumerGroups) {}

    public record PartitionInfo(
        int partition,
        String leader,
        List<String> replicas,
        List<String> isr,
        boolean isUnderReplicated
    ) {}
}
