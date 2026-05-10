package com.kafkatopology.mcp.tool;

import com.kafkatopology.mcp.model.ConsumerLagResult;
import com.kafkatopology.mcp.service.LagService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

@Component
public class ConsumerLagTool {

    private final LagService lagService;

    public ConsumerLagTool(LagService lagService) {
        this.lagService = lagService;
    }

    @Tool(description = """
        Reports consumer lag for Kafka consumer groups: per-partition committed offset, log end offset (LEO),
        and lag (LEO minus committed). High lag means consumers are falling behind producers.
        Optionally filter by a specific consumer group ID and/or topic name.
        Returns all consumer groups across all topics when no filters are provided.
        """)
    public String getConsumerLag(
            @ToolParam(description = "Consumer group ID to check. Leave blank for all groups.", required = false)
            String groupId,
            @ToolParam(description = "Topic name to filter lag results. Leave blank for all topics.", required = false)
            String topicName) {

        try {
            ConsumerLagResult result = lagService.getConsumerLag(
                emptyToNull(groupId), emptyToNull(topicName));
            return format(result);
        } catch (Exception e) {
            return "Error getting consumer lag: " + e.getMessage();
        }
    }

    private String format(ConsumerLagResult r) {
        if (r.groups().isEmpty()) {
            return "No consumer groups found with committed offsets for the given filters.";
        }

        StringBuilder sb = new StringBuilder("## Consumer Lag Report\n\n");

        for (ConsumerLagResult.GroupLag g : r.groups()) {
            sb.append("### Group: `").append(g.groupId()).append("`\n");
            sb.append("State: **").append(g.state()).append("** | Total lag: **").append(g.totalLag()).append("**\n\n");

            if (!g.partitionLags().isEmpty()) {
                sb.append("| Topic | Partition | Committed | LEO | Lag |\n");
                sb.append("|-------|-----------|-----------|-----|-----|\n");
                for (ConsumerLagResult.PartitionLag pl : g.partitionLags()) {
                    sb.append("| ").append(pl.topic())
                      .append(" | ").append(pl.partition())
                      .append(" | ").append(pl.committedOffset())
                      .append(" | ").append(pl.logEndOffset())
                      .append(" | ").append(pl.lag())
                      .append(" |\n");
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    private String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
