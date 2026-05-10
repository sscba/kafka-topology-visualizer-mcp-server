package com.kafkatopology.mcp.model;

import java.util.List;

public record ConsumerLagResult(List<GroupLag> groups) {

    public record GroupLag(
        String groupId,
        long totalLag,
        String state,
        List<PartitionLag> partitionLags
    ) {}

    public record PartitionLag(
        String topic,
        int partition,
        long committedOffset,
        long logEndOffset,
        long lag
    ) {}
}
