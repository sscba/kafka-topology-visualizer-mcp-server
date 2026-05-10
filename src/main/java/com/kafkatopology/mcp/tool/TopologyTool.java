package com.kafkatopology.mcp.tool;

import com.kafkatopology.mcp.model.TopologyResult;
import com.kafkatopology.mcp.service.TopologyService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

@Component
public class TopologyTool {

    private final TopologyService topologyService;

    public TopologyTool(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Tool(description = """
        Describes the full Kafka cluster topology: brokers, topics, partitions, partition leaders,
        in-sync replicas (ISR), and replication factor. Use this to understand cluster structure,
        identify under-replicated partitions, or get an overview of topics. Supports optional
        topic name filter using a Java regex pattern (e.g. "^orders" to match topics starting with 'orders').
        """)
    public String describeTopology(
            @ToolParam(description = "Optional Java regex to filter topic names. Leave blank for all topics.", required = false)
            String topicFilter,
            @ToolParam(description = "Include consumer group names alongside each topic. Defaults to false.", required = false)
            boolean includeConsumerGroups) {

        try {
            TopologyResult result = topologyService.describeTopology(topicFilter, includeConsumerGroups);
            return format(result);
        } catch (Exception e) {
            return "Error describing topology: " + e.getMessage();
        }
    }

    private String format(TopologyResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Kafka Cluster Topology\n\n");
        sb.append("**Cluster ID:** ").append(r.cluster().clusterId()).append("\n");
        sb.append("**Controller:** ").append(r.cluster().controller()).append("\n");
        sb.append("**Brokers (").append(r.cluster().brokers().size()).append("):**\n");
        r.cluster().brokers().forEach(b -> sb.append("  - ").append(b).append("\n"));

        boolean showGroups = r.topics().stream()
            .anyMatch(t -> !t.consumerGroups().isEmpty());

        sb.append("\n**Topics (").append(r.topics().size()).append("):**\n\n");
        if (showGroups) {
            sb.append("| Topic | Partitions | Replication | Health | Consumer Groups |\n");
            sb.append("|-------|------------|-------------|--------|-----------------|\n");
        } else {
            sb.append("| Topic | Partitions | Replication | Health |\n");
            sb.append("|-------|------------|-------------|--------|\n");
        }

        for (TopologyResult.TopicInfo t : r.topics()) {
            long underReplicated = t.partitions().stream()
                .filter(TopologyResult.PartitionInfo::isUnderReplicated).count();
            String health = underReplicated > 0
                ? "UNDER-REPLICATED (" + underReplicated + " partition(s))"
                : "OK";
            sb.append("| ").append(t.name())
              .append(" | ").append(t.partitionCount())
              .append(" | ").append(t.replicationFactor())
              .append(" | ").append(health);
            if (showGroups) {
                String groups = t.consumerGroups().isEmpty() ? "—"
                    : String.join(", ", t.consumerGroups());
                sb.append(" | ").append(groups);
            }
            sb.append(" |\n");
        }

        return sb.toString();
    }
}
