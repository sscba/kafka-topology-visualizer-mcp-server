package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.config.KafkaConnectionConfig;
import com.kafkatopology.mcp.model.PartitionRecommendation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class PartitionAdvisorService {

    private final AdminClient adminClient;
    private final KafkaConnectionConfig config;

    public PartitionAdvisorService(AdminClient adminClient, KafkaConnectionConfig config) {
        this.adminClient = adminClient;
        this.config = config;
    }

    public PartitionRecommendation suggestPartitionStrategy(
            String topic, double targetThroughputMbps, int consumerCount, int retentionDays) {

        int currentPartitions = fetchCurrentPartitionCount(topic);
        double producerThroughputMbps = config.getPartition().getDefaultProducerThroughputMbps();

        int minFromThroughput = (int) Math.ceil(targetThroughputMbps / producerThroughputMbps);
        int recommended = nextPowerOfTwo(Math.max(1, Math.max(minFromThroughput, consumerCount)));

        // Replication: 3 for any meaningful throughput, 1 only for dev/minimal workloads
        int replicationFactor = targetThroughputMbps >= 10.0 ? 3 : 1;

        // Storage per partition per retention window (MB → GB)
        double totalMbPerDay = targetThroughputMbps * 86_400.0 / 1024.0;
        double storagePerPartitionGB = (totalMbPerDay * retentionDays) / (1024.0 * recommended);
        double totalStorageGB = storagePerPartitionGB * recommended * replicationFactor;

        String reasoning = buildReasoning(
            targetThroughputMbps, producerThroughputMbps, consumerCount,
            minFromThroughput, recommended, replicationFactor,
            retentionDays, storagePerPartitionGB, totalStorageGB);

        return new PartitionRecommendation(
            topic, currentPartitions, recommended,
            replicationFactor, storagePerPartitionGB, totalStorageGB, reasoning);
    }

    private int fetchCurrentPartitionCount(String topic) {
        try {
            Map<String, TopicDescription> result = adminClient
                .describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS);
            return result.containsKey(topic) ? result.get(topic).partitions().size() : 0;
        } catch (Exception e) {
            return 0; // topic does not exist yet — that's fine
        }
    }

    private int nextPowerOfTwo(int n) {
        if (n <= 1) return 1;
        int power = 1;
        while (power < n) power <<= 1;
        return power;
    }

    private String buildReasoning(double targetThroughput, double producerThroughput, int consumerCount,
                                   int minFromThroughput, int recommended, int replicationFactor,
                                   int retentionDays, double storagePerPartitionGB, double totalStorageGB) {
        return String.format(
            "Target: %.1f MB/s throughput, %d consumers, %d days retention. " +
            "Throughput-based minimum: ceil(%.1f / %.1f producer) = %d partition(s). " +
            "Consumer parallelism minimum: %d partition(s). " +
            "Recommended: %d partition(s) (next power of 2 for even distribution). " +
            "Replication factor: %d. " +
            "Estimated storage: %.2f GB per partition, %.2f GB total (with replication).",
            targetThroughput, consumerCount, retentionDays,
            targetThroughput, producerThroughput, minFromThroughput,
            consumerCount, recommended, replicationFactor,
            storagePerPartitionGB, totalStorageGB);
    }
}
