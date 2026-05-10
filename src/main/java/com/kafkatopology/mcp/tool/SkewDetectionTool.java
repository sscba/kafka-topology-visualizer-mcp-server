package com.kafkatopology.mcp.tool;

import com.kafkatopology.mcp.model.SkewResult;
import com.kafkatopology.mcp.service.SkewService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

@Component
public class SkewDetectionTool {

    private final SkewService skewService;

    public SkewDetectionTool(SkewService skewService) {
        this.skewService = skewService;
    }

    @Tool(description = """
        Detects partition skew for a Kafka topic by comparing log sizes (or estimated message counts)
        across partitions. Flags partitions where the deviation from the mean exceeds the threshold.
        Skew causes hot partitions, uneven broker load, and consumer throughput imbalance.
        Uses broker log-dir sizes when available; falls back to offset-based estimation on timeout.
        """)
    public String detectSkew(
            @ToolParam(description = "Topic name to analyze for partition skew.")
            String topicName,
            @ToolParam(description = "Deviation threshold percentage to flag a partition as skewed. Default: 20.0", required = false)
            Double thresholdPct) {

        double threshold = (thresholdPct == null || thresholdPct <= 0) ? 20.0 : thresholdPct;

        try {
            SkewResult result = skewService.detectSkew(topicName, threshold);
            return format(result);
        } catch (Exception e) {
            return "Error detecting skew for topic '" + topicName + "': " + e.getMessage();
        }
    }

    private String format(SkewResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Partition Skew Analysis: `").append(r.topic()).append("`\n\n");
        sb.append("**Metric:** ").append(r.metricName()).append("\n");
        sb.append("**Mean value:** ").append(r.meanValue()).append("\n");
        sb.append("**Threshold:** ").append(r.thresholdPct()).append("%\n");
        sb.append("**Skew detected:** ").append(r.skewDetected() ? "YES" : "No").append("\n\n");

        sb.append("| Partition | Value | Deviation | Status |\n");
        sb.append("|-----------|-------|-----------|--------|\n");
        for (SkewResult.PartitionStats p : r.partitions()) {
            sb.append("| ").append(p.partition())
              .append(" | ").append(p.value())
              .append(" | ").append(String.format("%.1f%%", p.deviationPct()))
              .append(" | ").append(p.isSkewed() ? "SKEWED" : "OK")
              .append(" |\n");
        }

        if (r.skewDetected()) {
            sb.append("\n**Recommendation:** Review producer key distribution. " +
                "Consider a custom partitioner or increase partition count to redistribute load.");
        }

        return sb.toString();
    }
}
