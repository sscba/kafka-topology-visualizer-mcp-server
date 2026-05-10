package com.kafkatopology.mcp.model;

import java.util.List;

public record SkewResult(
    String topic,
    double thresholdPct,
    boolean skewDetected,
    long meanValue,
    String metricName,
    List<PartitionStats> partitions
) {

    public record PartitionStats(
        int partition,
        long value,
        double deviationPct,
        boolean isSkewed
    ) {}
}
