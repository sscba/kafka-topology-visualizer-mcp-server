package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.MessageFlowResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlowServiceTest {

    @Mock private AdminClient adminClient;
    @Mock private DescribeTopicsResult describeTopicsResult;
    @Mock private KafkaFuture<Map<String, TopicDescription>> topicDescFuture;
    @Mock private ListConsumerGroupsResult listGroupsResult;
    @Mock private KafkaFuture<Collection<ConsumerGroupListing>> validGroupsFuture;
    @Mock private ListOffsetsResult listOffsetsResult;
    @Mock private KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> latestOffsetsFuture;

    private FlowService service;

    @BeforeEach
    void setUp() {
        service = new FlowService(adminClient);
    }

    @Test
    void groupConsumingTopicAppearsInResult() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);
        stubTopic("orders", 1, 1);
        stubCommittedOffsets("my-group", Map.of(tp, new OffsetAndMetadata(50)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));
        stubGroupDescription("my-group", List.of());

        MessageFlowResult result = service.traceMessageFlow("orders", "my-group");

        assertThat(result.topic()).isEqualTo("orders");
        assertThat(result.consumerGroups()).hasSize(1);
        assertThat(result.consumerGroups().get(0).groupId()).isEqualTo("my-group");
        assertThat(result.note()).isNotBlank();
    }

    @Test
    void groupNotConsumingTopicIsFilteredOut() throws Exception {
        stubTopic("orders", 1, 1);
        // group only has offsets for a different topic
        stubCommittedOffsets("other-group", Map.of(
            new TopicPartition("payments", 0), new OffsetAndMetadata(10)));

        MessageFlowResult result = service.traceMessageFlow("orders", "other-group");

        assertThat(result.consumerGroups()).isEmpty();
    }

    @Test
    void nullConsumerGroupFetchesAllGroups() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);
        stubTopic("orders", 1, 1);
        stubAllGroups(List.of("group-a", "group-b"));
        stubCommittedOffsets("group-a", Map.of(tp, new OffsetAndMetadata(40)));
        stubCommittedOffsets("group-b", Map.of(tp, new OffsetAndMetadata(60)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));
        stubGroupDescription("group-a", List.of());
        stubGroupDescription("group-b", List.of());

        MessageFlowResult result = service.traceMessageFlow("orders", null);

        assertThat(result.consumerGroups()).hasSize(2);
        verify(adminClient).listConsumerGroups();
    }

    @Test
    void specificConsumerGroupSkipsGroupListing() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);
        stubTopic("orders", 1, 1);
        stubCommittedOffsets("exact-group", Map.of(tp, new OffsetAndMetadata(50)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));
        stubGroupDescription("exact-group", List.of());

        service.traceMessageFlow("orders", "exact-group");

        verify(adminClient, never()).listConsumerGroups();
    }

    @Test
    void memberLagIsLeoMinusCommitted() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);
        stubTopic("orders", 1, 1);
        stubCommittedOffsets("my-group", Map.of(tp, new OffsetAndMetadata(70)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));
        stubGroupDescription("my-group", List.of(memberWith("client-1", Set.of(tp))));

        MessageFlowResult result = service.traceMessageFlow("orders", "my-group");

        assertThat(result.consumerGroups().get(0).members().get(0).totalLag()).isEqualTo(30);
    }

    @Test
    void memberLagIsNeverNegative() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);
        stubTopic("orders", 1, 1);
        // committed offset exceeds LEO — transient state during leader election
        stubCommittedOffsets("my-group", Map.of(tp, new OffsetAndMetadata(120)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));
        stubGroupDescription("my-group", List.of(memberWith("client-1", Set.of(tp))));

        MessageFlowResult result = service.traceMessageFlow("orders", "my-group");

        assertThat(result.consumerGroups().get(0).members().get(0).totalLag()).isZero();
    }

    @Test
    void totalGroupLagIsSumOfMemberLags() throws Exception {
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);
        stubTopic("orders", 2, 1);
        stubCommittedOffsets("my-group", Map.of(
            tp0, new OffsetAndMetadata(80),
            tp1, new OffsetAndMetadata(60)));
        stubLatestOffsets(Map.of(
            tp0, offsetInfo(100),
            tp1, offsetInfo(100)));
        stubGroupDescription("my-group", List.of(
            memberWith("client-1", Set.of(tp0)),
            memberWith("client-2", Set.of(tp1))));

        MessageFlowResult result = service.traceMessageFlow("orders", "my-group");

        // client-1 lag=20, client-2 lag=40, total=60
        assertThat(result.consumerGroups().get(0).totalLag()).isEqualTo(60);
    }

    @Test
    void interruptedExceptionRethrowsAndSetsInterruptFlag() throws Exception {
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(topicDescFuture);
        when(topicDescFuture.get(30, TimeUnit.SECONDS)).thenThrow(new InterruptedException("interrupted"));

        assertThatThrownBy(() -> service.traceMessageFlow("orders", "g"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Interrupted");
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear flag so other tests are not affected
    }

    @Test
    void executionExceptionRethrowsWithMessage() throws Exception {
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(topicDescFuture);
        when(topicDescFuture.get(30, TimeUnit.SECONDS))
            .thenThrow(new ExecutionException("boom", new RuntimeException("unknown topic")));

        assertThatThrownBy(() -> service.traceMessageFlow("orders", "g"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to trace message flow");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void stubTopic(String name, int partitions, int replication) throws Exception {
        Node leader = new Node(0, "broker-1", 9092);
        List<Node> replicas = Collections.nCopies(replication, leader);
        List<TopicPartitionInfo> tpInfos = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            tpInfos.add(new TopicPartitionInfo(i, leader, replicas, replicas));
        }
        TopicDescription desc = new TopicDescription(name, false, tpInfos);

        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(topicDescFuture);
        when(topicDescFuture.get(30, TimeUnit.SECONDS)).thenReturn(Map.of(name, desc));
    }

    private void stubAllGroups(List<String> groupIds) throws Exception {
        List<ConsumerGroupListing> listings = groupIds.stream()
            .map(id -> new ConsumerGroupListing(id, false))
            .toList();
        when(adminClient.listConsumerGroups()).thenReturn(listGroupsResult);
        when(listGroupsResult.valid()).thenReturn(validGroupsFuture);
        when(validGroupsFuture.get(30, TimeUnit.SECONDS)).thenReturn(listings);
    }

    private void stubCommittedOffsets(String groupId,
                                      Map<TopicPartition, OffsetAndMetadata> offsets) throws Exception {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future = mock(KafkaFuture.class);
        when(adminClient.listConsumerGroupOffsets(groupId)).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(future);
        when(future.get(30, TimeUnit.SECONDS)).thenReturn(offsets);
    }

    private void stubGroupDescription(String groupId, List<MemberDescription> members) throws Exception {
        ConsumerGroupDescription desc = new ConsumerGroupDescription(
            groupId, false, members, "range", ConsumerGroupState.STABLE, new Node(-1, "", -1));
        DescribeConsumerGroupsResult descResult = mock(DescribeConsumerGroupsResult.class);
        KafkaFuture<Map<String, ConsumerGroupDescription>> future = mock(KafkaFuture.class);
        when(adminClient.describeConsumerGroups(List.of(groupId))).thenReturn(descResult);
        when(descResult.all()).thenReturn(future);
        when(future.get(30, TimeUnit.SECONDS)).thenReturn(Map.of(groupId, desc));
    }

    private void stubLatestOffsets(
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets) throws Exception {
        when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);
        when(listOffsetsResult.all()).thenReturn(latestOffsetsFuture);
        when(latestOffsetsFuture.get(30, TimeUnit.SECONDS)).thenReturn(offsets);
    }

    private MemberDescription memberWith(String clientId, Set<TopicPartition> partitions) {
        return new MemberDescription(
            clientId + "-id", Optional.empty(), clientId, "host",
            new MemberAssignment(partitions));
    }

    private ListOffsetsResult.ListOffsetsResultInfo offsetInfo(long offset) {
        return new ListOffsetsResult.ListOffsetsResultInfo(offset, System.currentTimeMillis(), Optional.of(0));
    }
}
