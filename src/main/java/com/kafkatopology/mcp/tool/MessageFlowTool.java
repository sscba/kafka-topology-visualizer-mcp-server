package com.kafkatopology.mcp.tool;

import com.kafkatopology.mcp.model.MessageFlowResult;
import com.kafkatopology.mcp.service.FlowService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

@Component
public class MessageFlowTool {

    private final FlowService flowService;

    public MessageFlowTool(FlowService flowService) {
        this.flowService = flowService;
    }

    @Tool(description = """
        Traces the consumer-side message flow for a Kafka topic: which consumer groups subscribe to it,
        member assignments per partition, and per-member lag. Useful for understanding data routing,
        consumer group health, and identifying stalled consumers.
        Note: Producer-side lineage is not available via Kafka AdminClient alone.
        """)
    public String traceMessageFlow(
            @ToolParam(description = "Topic name to trace message flow for.")
            String topicName,
            @ToolParam(description = "Optional consumer group ID to limit the trace. Leave blank for all groups.", required = false)
            String consumerGroup) {

        try {
            MessageFlowResult result = flowService.traceMessageFlow(
                topicName, emptyToNull(consumerGroup));
            return format(result);
        } catch (Exception e) {
            return "Error tracing message flow for topic '" + topicName + "': " + e.getMessage();
        }
    }

    private String format(MessageFlowResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Message Flow: `").append(r.topic()).append("`\n\n");
        sb.append("**Partitions:** ").append(r.partitionCount())
          .append(" | **Replication factor:** ").append(r.replicationFactor()).append("\n\n");

        if (r.consumerGroups().isEmpty()) {
            sb.append("No consumer groups found consuming this topic.\n");
        } else {
            sb.append("### Consumer Groups\n\n");
            for (MessageFlowResult.ConsumerGroupInfo g : r.consumerGroups()) {
                sb.append("#### `").append(g.groupId()).append("` — ")
                  .append(g.state()).append(" | Total lag: **").append(g.totalLag()).append("**\n\n");

                if (!g.members().isEmpty()) {
                    sb.append("| Member | Assigned Partitions | Lag |\n");
                    sb.append("|--------|---------------------|-----|\n");
                    for (MessageFlowResult.MemberInfo m : g.members()) {
                        sb.append("| ").append(m.clientId())
                          .append(" | ").append(String.join(", ", m.assignedPartitions()))
                          .append(" | ").append(m.totalLag())
                          .append(" |\n");
                    }
                } else {
                    sb.append("_No active members (group may be in rebalance or idle)_\n");
                }
                sb.append("\n");
            }
        }

        sb.append("> ").append(r.note()).append("\n");
        return sb.toString();
    }

    private String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
