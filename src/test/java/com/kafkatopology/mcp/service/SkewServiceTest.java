package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.SkewResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SkewServiceTest {

    @Mock private AdminClient adminClient;
    @Mock private DescribeTopicsResult describeTopicsResult;
    @Mock private KafkaFuture<Map<String, TopicDescription>> topicsFuture;
    @Mock private DescribeLogDirsResult logDirsResult;
    @Mock private KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> logDirsFuture;
    @Mock private ListOffsetsResult listOffsetsResult;
    @Mock private KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> offsetsFuture;

    private SkewService service;

    @BeforeEach
    void setUp() {
        service = new SkewService(adminClient);
    }

    @Test
    void detectsNoSkewWhenPartitionSizesAreUniform() throws Exception {
        String topic = "uniform-topic";
        Node leader = new Node(0, "broker-1", 9092);
        stubTopic(topic, leader, 3);
        stubLogDirs(topic, leader.id(), Map.of(0, 1000L, 1, 1000L, 2, 1000L));

        SkewResult result = service.detectSkew(topic, 20.0);

        assertThat(result.skewDetected()).isFalse();
        assertThat(result.partitions()).allMatch(p -> !p.isSkewed());
    }

    @Test
    void detectsSkewWhenOnePartitionIsMuchLarger() throws Exception {
        String topic = "skewed-topic";
        Node leader = new Node(0, "broker-1", 9092);
        stubTopic(topic, leader, 3);
        // partition 2 is 5x mean → 400% deviation, well above 20% threshold
        stubLogDirs(topic, leader.id(), Map.of(0, 1000L, 1, 1000L, 2, 5000L));

        SkewResult result = service.detectSkew(topic, 20.0);

        assertThat(result.skewDetected()).isTrue();
        assertThat(result.partitions())
            .filteredOn(p -> p.partition() == 2)
            .first()
            .matches(p -> p.isSkewed());
    }

    @Test
    void respectsCustomThreshold() throws Exception {
        String topic = "mildly-skewed";
        Node leader = new Node(0, "broker-1", 9092);
        stubTopic(topic, leader, 2);
        // ~13% deviation (300/2300) — below 20% but above 10%
        stubLogDirs(topic, leader.id(), Map.of(0, 1000L, 1, 1300L));

        SkewResult atTwentyPct = service.detectSkew(topic, 20.0);
        SkewResult atTenPct = service.detectSkew(topic, 10.0);

        assertThat(atTwentyPct.skewDetected()).isFalse();
        assertThat(atTenPct.skewDetected()).isTrue();
    }

    @Test
    void fallsBackToOffsetEstimateWhenLogDirsTimeout() throws Exception {
        String topic = "fallback-topic";
        Node leader = new Node(0, "broker-1", 9092);
        stubTopic(topic, leader, 2);

        // describeLogDirs times out
        when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);
        when(logDirsResult.allDescriptions()).thenReturn(logDirsFuture);
        when(logDirsFuture.get(10, TimeUnit.SECONDS))
            .thenThrow(new java.util.concurrent.TimeoutException("timeout"));

        // Fallback: use offset ranges
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        stubOffsets(Map.of(tp0, offsetInfo(0L), tp1, offsetInfo(0L)),
                    Map.of(tp0, offsetInfo(1000L), tp1, offsetInfo(1000L)));

        SkewResult result = service.detectSkew(topic, 20.0);

        assertThat(result).isNotNull();
        assertThat(result.topic()).isEqualTo(topic);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void stubTopic(String name, Node leader, int partitionCount) throws Exception {
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new TopicPartitionInfo(i, leader, List.of(leader), List.of(leader)));
        }
        TopicDescription desc = new TopicDescription(name, false, partitions);

        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(topicsFuture);
        when(topicsFuture.get(30, TimeUnit.SECONDS)).thenReturn(Map.of(name, desc));
    }

    private void stubLogDirs(String topic, int brokerId, Map<Integer, Long> partitionSizes) throws Exception {
        Map<TopicPartition, ReplicaInfo> replicaInfos = new HashMap<>();
        partitionSizes.forEach((partition, size) ->
            replicaInfos.put(new TopicPartition(topic, partition), new ReplicaInfo(size, 0, false)));

        LogDirDescription logDir = new LogDirDescription(null, replicaInfos);
        Map<Integer, Map<String, LogDirDescription>> logDirsMap = Map.of(brokerId, Map.of("/var/kafka/data", logDir));

        when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);
        when(logDirsResult.allDescriptions()).thenReturn(logDirsFuture);
        when(logDirsFuture.get(10, TimeUnit.SECONDS)).thenReturn(logDirsMap);
    }

    private void stubOffsets(Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest,
                              Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest) throws Exception {
        when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);
        when(listOffsetsResult.all()).thenReturn(offsetsFuture);
        when(offsetsFuture.get(30, TimeUnit.SECONDS)).thenReturn(earliest, latest);
    }

    private ListOffsetsResult.ListOffsetsResultInfo offsetInfo(long offset) {
        return new ListOffsetsResult.ListOffsetsResultInfo(offset, System.currentTimeMillis(), Optional.of(0));
    }
}
