package com.kafkatopology.mcp.tool;

import com.kafkatopology.mcp.model.PartitionRecommendation;
import com.kafkatopology.mcp.service.PartitionAdvisorService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

@Component
public class PartitionStrategyTool {

    private final PartitionAdvisorService advisorService;

    public PartitionStrategyTool(PartitionAdvisorService advisorService) {
        this.advisorService = advisorService;
    }

    @Tool(description = """
        Recommends an optimal Kafka partition strategy for a topic based on throughput requirements,
        consumer count, and data retention. Computes partition count (as next power of 2 for even
        distribution), replication factor, and estimated storage. Producer throughput per partition
        is configurable via application.yml (default: 10 MB/s).
        """)
    public String suggestPartitionStrategy(
            @ToolParam(description = "Topic name to evaluate (used to fetch current partition count if it exists).")
            String topicName,
            @ToolParam(description = "Target producer throughput in MB/s for the topic.")
            double targetThroughputMbps,
            @ToolParam(description = "Number of consumers in the consumer group (sets the minimum useful partition count).")
            int consumerCount,
            @ToolParam(description = "Data retention in days (used to estimate total storage).")
            int retentionDays) {

        try {
            PartitionRecommendation rec = advisorService.suggestPartitionStrategy(
                topicName, targetThroughputMbps, consumerCount, retentionDays);
            return format(rec);
        } catch (Exception e) {
            return "Error generating partition strategy: " + e.getMessage();
        }
    }

    private String format(PartitionRecommendation r) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Partition Strategy Recommendation: `").append(r.topic()).append("`\n\n");

        sb.append("| Parameter | Value |\n");
        sb.append("|-----------|-------|\n");
        sb.append("| Current partitions | ").append(r.currentPartitions() == 0 ? "N/A (new topic)" : r.currentPartitions()).append(" |\n");
        sb.append("| **Recommended partitions** | **").append(r.recommendedPartitions()).append("** |\n");
        sb.append("| Replication factor | ").append(r.replicationFactor()).append(" |\n");
        sb.append("| Est. storage per partition | ").append(String.format("%.2f GB", r.estimatedStoragePerPartitionGB())).append(" |\n");
        sb.append("| Est. total storage (with replication) | ").append(String.format("%.2f GB", r.totalStorageGB())).append(" |\n");

        sb.append("\n**Reasoning:** ").append(r.reasoning()).append("\n");

        if (r.currentPartitions() > 0 && r.recommendedPartitions() > r.currentPartitions()) {
            sb.append("\n> **Action required:** Current partition count (").append(r.currentPartitions())
              .append(") is below the recommendation (").append(r.recommendedPartitions())
              .append("). Note: Kafka only supports increasing partition counts, never decreasing.\n");
        } else if (r.currentPartitions() > 0 && r.recommendedPartitions() <= r.currentPartitions()) {
            sb.append("\n> Current partition count meets the recommendation. No changes needed.\n");
        }

        return sb.toString();
    }
}
