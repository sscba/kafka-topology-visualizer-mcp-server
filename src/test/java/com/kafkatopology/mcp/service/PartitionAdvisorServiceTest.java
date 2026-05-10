package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.config.KafkaConnectionConfig;
import com.kafkatopology.mcp.model.PartitionRecommendation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartitionAdvisorServiceTest {

    @Mock private AdminClient adminClient;
    @Mock private DescribeTopicsResult describeTopicsResult;
    @Mock private KafkaFuture<Map<String, TopicDescription>> topicsFuture;

    private KafkaConnectionConfig config;
    private PartitionAdvisorService service;

    @BeforeEach
    void setUp() {
        config = new KafkaConnectionConfig();
        config.getPartition().setDefaultProducerThroughputMbps(10.0);
        service = new PartitionAdvisorService(adminClient, config);
    }

    @Test
    void recommendsAtLeastConsumerCountPartitions() throws Exception {
        stubCurrentPartitions("my-topic", 4);

        PartitionRecommendation rec = service.suggestPartitionStrategy("my-topic", 5.0, 8, 7);

        // 8 consumers → at least 8 partitions → next power of 2 = 8
        assertThat(rec.recommendedPartitions()).isGreaterThanOrEqualTo(8);
        assertThat(rec.topic()).isEqualTo("my-topic");
    }

    @Test
    void recommendsEnoughPartitionsForThroughput() throws Exception {
        stubCurrentPartitions("high-volume", 4);

        // 50 MB/s target / 10 MB/s per partition = 5, next power of 2 = 8
        PartitionRecommendation rec = service.suggestPartitionStrategy("high-volume", 50.0, 2, 3);

        assertThat(rec.recommendedPartitions()).isGreaterThanOrEqualTo(8);
    }

    @Test
    void returnsCurrentPartitionCountForExistingTopic() throws Exception {
        stubCurrentPartitions("existing-topic", 6);

        PartitionRecommendation rec = service.suggestPartitionStrategy("existing-topic", 10.0, 2, 7);

        assertThat(rec.currentPartitions()).isEqualTo(6);
    }

    @Test
    void returnsZeroCurrentPartitionsForNewTopic() throws Exception {
        when(adminClient.describeTopics(anyCollection())).thenThrow(new RuntimeException("topic not found"));

        PartitionRecommendation rec = service.suggestPartitionStrategy("new-topic", 10.0, 2, 7);

        assertThat(rec.currentPartitions()).isEqualTo(0);
    }

    @Test
    void recommendedPartitionsIsAlwaysPowerOfTwo() throws Exception {
        stubCurrentPartitions("topic", 4);

        // 30 MB/s / 10 MB/s = 3 → next power of 2 = 4; consumer=3 → max(4,3)=4
        PartitionRecommendation rec = service.suggestPartitionStrategy("topic", 30.0, 3, 1);

        int n = rec.recommendedPartitions();
        assertThat(n).isGreaterThan(0);
        assertThat(n & (n - 1)).isEqualTo(0); // power of 2 check: n & (n-1) == 0
    }

    @Test
    void reasoningContainsKeyMetrics() throws Exception {
        stubCurrentPartitions("topic", 4);

        PartitionRecommendation rec = service.suggestPartitionStrategy("topic", 20.0, 4, 14);

        assertThat(rec.reasoning()).contains("20.0", "4", "14");
    }

    @Test
    void storageEstimateIsPositive() throws Exception {
        stubCurrentPartitions("topic", 4);

        PartitionRecommendation rec = service.suggestPartitionStrategy("topic", 10.0, 2, 7);

        assertThat(rec.estimatedStoragePerPartitionGB()).isPositive();
        assertThat(rec.totalStorageGB()).isPositive();
        assertThat(rec.totalStorageGB()).isGreaterThan(rec.estimatedStoragePerPartitionGB());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void stubCurrentPartitions(String topic, int count) throws Exception {
        Node node = new Node(0, "localhost", 9092);
        List<TopicPartitionInfo> partitions = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new TopicPartitionInfo(i, node, List.of(node), List.of(node)));
        }
        TopicDescription desc = new TopicDescription(topic, false, partitions);

        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(topicsFuture);
        when(topicsFuture.get(10, java.util.concurrent.TimeUnit.SECONDS))
            .thenReturn(Map.of(topic, desc));
    }
}
