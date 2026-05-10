package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.SkewResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Service
public class SkewService {

    private final AdminClient adminClient;

    public SkewService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Cacheable(value = "skew", key = "#topicName + ':' + #thresholdPct")
    public SkewResult detectSkew(String topicName, double thresholdPct) {
        try {
            TopicDescription desc = adminClient.describeTopics(List.of(topicName))
                .allTopicNames().get(30, SECONDS).get(topicName);

            Set<Integer> brokerIds = desc.partitions().stream()
                .flatMap(p -> p.replicas().stream())
                .map(Node::id)
                .collect(Collectors.toSet());

            try {
                Map<Integer, Map<String, LogDirDescription>> logDirs = adminClient
                    .describeLogDirs(brokerIds)
                    .allDescriptions().get(10, TimeUnit.SECONDS);

                Map<Integer, Long> partitionSizes = extractPartitionSizes(topicName, desc, logDirs);
                return computeSkew(topicName, thresholdPct, partitionSizes, "log size (bytes)");

            } catch (TimeoutException e) {
                // describeLogDirs is slow on large clusters; fall back to offset-based estimation
                return detectSkewByOffsets(topicName, desc, thresholdPct);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while detecting skew", e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Failed to detect skew: " + cause.getMessage(), e);
        }
    }

    private Map<Integer, Long> extractPartitionSizes(
            String topicName,
            TopicDescription desc,
            Map<Integer, Map<String, LogDirDescription>> logDirs) {

        Map<Integer, Long> sizes = new HashMap<>();
        for (TopicPartitionInfo tpi : desc.partitions()) {
            if (tpi.leader() == null) continue;
            int leaderId = tpi.leader().id();
            Map<String, LogDirDescription> leaderDirs = logDirs.get(leaderId);
            if (leaderDirs == null) continue;

            for (LogDirDescription logDir : leaderDirs.values()) {
                ReplicaInfo info = logDir.replicaInfos()
                    .get(new TopicPartition(topicName, tpi.partition()));
                if (info != null) {
                    sizes.put(tpi.partition(), info.size());
                    break;
                }
            }
        }
        return sizes;
    }

    private SkewResult detectSkewByOffsets(String topicName, TopicDescription desc, double thresholdPct)
            throws InterruptedException, ExecutionException, TimeoutException {

        Map<TopicPartition, OffsetSpec> latestSpecs = desc.partitions().stream()
            .collect(Collectors.toMap(
                p -> new TopicPartition(topicName, p.partition()), p -> OffsetSpec.latest()));
        Map<TopicPartition, OffsetSpec> earliestSpecs = desc.partitions().stream()
            .collect(Collectors.toMap(
                p -> new TopicPartition(topicName, p.partition()), p -> OffsetSpec.earliest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
            adminClient.listOffsets(latestSpecs).all().get(30, SECONDS);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
            adminClient.listOffsets(earliestSpecs).all().get(30, SECONDS);

        Map<Integer, Long> messageCounts = new HashMap<>();
        for (TopicPartitionInfo tpi : desc.partitions()) {
            TopicPartition tp = new TopicPartition(topicName, tpi.partition());
            long latest = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0;
            long earliest = earliestOffsets.containsKey(tp) ? earliestOffsets.get(tp).offset() : 0;
            messageCounts.put(tpi.partition(), latest - earliest);
        }

        return computeSkew(topicName, thresholdPct, messageCounts, "message count (estimated from offsets)");
    }

    private SkewResult computeSkew(String topicName, double thresholdPct,
                                   Map<Integer, Long> partitionMetric, String metricName) {
        if (partitionMetric.isEmpty()) {
            return new SkewResult(topicName, thresholdPct, false, 0, metricName, List.of());
        }

        double mean = partitionMetric.values().stream()
            .mapToLong(Long::longValue).average().orElse(0);

        List<SkewResult.PartitionStats> stats = partitionMetric.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> {
                double deviation = mean == 0 ? 0 : Math.abs(e.getValue() - mean) / mean * 100;
                return new SkewResult.PartitionStats(e.getKey(), e.getValue(), deviation, deviation > thresholdPct);
            })
            .collect(toList());

        boolean skewDetected = stats.stream().anyMatch(SkewResult.PartitionStats::isSkewed);
        return new SkewResult(topicName, thresholdPct, skewDetected, (long) mean, metricName, stats);
    }
}
