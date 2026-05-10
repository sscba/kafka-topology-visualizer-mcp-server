package com.kafkatopology.mcp.model;

public record PartitionRecommendation(
    String topic,
    int currentPartitions,
    int recommendedPartitions,
    int replicationFactor,
    double estimatedStoragePerPartitionGB,
    double totalStorageGB,
    String reasoning
) {}
