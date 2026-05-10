package com.kafkatopology.mcp.model;

import java.util.List;

public record MessageFlowResult(
    String topic,
    int partitionCount,
    int replicationFactor,
    List<ConsumerGroupInfo> consumerGroups,
    String note
) {

    public record ConsumerGroupInfo(
        String groupId,
        String state,
        List<MemberInfo> members,
        long totalLag
    ) {}

    public record MemberInfo(
        String clientId,
        List<String> assignedPartitions,
        long totalLag
    ) {}
}
