package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.ConsumerLagResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LagServiceTest {

    @Mock private AdminClient adminClient;
    @Mock private ListConsumerGroupsResult listGroupsResult;
    @Mock private KafkaFuture<Collection<ConsumerGroupListing>> validGroupsFuture;
    @Mock private ListConsumerGroupOffsetsResult offsetsResult;
    @Mock private KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture;
    @Mock private ListOffsetsResult listOffsetsResult;
    @Mock private KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> latestOffsetsFuture;
    @Mock private DescribeConsumerGroupsResult describeGroupsResult;
    @Mock private KafkaFuture<Map<String, ConsumerGroupDescription>> describeGroupsFuture;

    private LagService service;

    @BeforeEach
    void setUp() {
        service = new LagService(adminClient);
    }

    @Test
    void computesLagAsLeoMinusCommittedOffset() throws Exception {
        TopicPartition tp = new TopicPartition("orders", 0);

        stubGroup("my-group", ConsumerGroupState.STABLE);
        stubCommittedOffsets("my-group", Map.of(tp, new OffsetAndMetadata(90)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));

        ConsumerLagResult result = service.getConsumerLag("my-group", null);

        assertThat(result.groups()).hasSize(1);
        ConsumerLagResult.GroupLag group = result.groups().get(0);
        assertThat(group.groupId()).isEqualTo("my-group");
        assertThat(group.totalLag()).isEqualTo(10);
        assertThat(group.partitionLags().get(0).lag()).isEqualTo(10);
        assertThat(group.partitionLags().get(0).committedOffset()).isEqualTo(90);
        assertThat(group.partitionLags().get(0).logEndOffset()).isEqualTo(100);
    }

    @Test
    void lagIsNeverNegative() throws Exception {
        // LEO < committed can happen briefly during leader elections
        TopicPartition tp = new TopicPartition("orders", 0);

        stubGroup("my-group", ConsumerGroupState.STABLE);
        stubCommittedOffsets("my-group", Map.of(tp, new OffsetAndMetadata(110)));
        stubLatestOffsets(Map.of(tp, offsetInfo(100)));

        ConsumerLagResult result = service.getConsumerLag("my-group", null);

        assertThat(result.groups().get(0).partitionLags().get(0).lag()).isZero();
    }

    @Test
    void returnsEmptyResultForGroupWithNoOffsets() throws Exception {
        stubGroup("empty-group", ConsumerGroupState.EMPTY);
        stubCommittedOffsets("empty-group", Map.of());

        ConsumerLagResult result = service.getConsumerLag("empty-group", null);

        assertThat(result.groups()).isEmpty();
    }

    @Test
    void aggregatesTotalLagAcrossAllPartitions() throws Exception {
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);

        stubGroup("my-group", ConsumerGroupState.STABLE);
        stubCommittedOffsets("my-group", Map.of(
            tp0, new OffsetAndMetadata(80),
            tp1, new OffsetAndMetadata(70)
        ));
        stubLatestOffsets(Map.of(
            tp0, offsetInfo(100),
            tp1, offsetInfo(100)
        ));

        ConsumerLagResult result = service.getConsumerLag("my-group", null);

        assertThat(result.groups().get(0).totalLag()).isEqualTo(50); // 20 + 30
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void stubGroup(String groupId, ConsumerGroupState state) throws Exception {
        ConsumerGroupDescription desc = new ConsumerGroupDescription(
            groupId, false, List.of(), "range", state, new Node(-1, "", -1));

        when(adminClient.describeConsumerGroups(List.of(groupId))).thenReturn(describeGroupsResult);
        when(describeGroupsResult.all()).thenReturn(describeGroupsFuture);
        when(describeGroupsFuture.get(30, TimeUnit.SECONDS)).thenReturn(Map.of(groupId, desc));
    }

    private void stubCommittedOffsets(String groupId,
                                      Map<TopicPartition, OffsetAndMetadata> offsets) throws Exception {
        lenient().when(adminClient.listConsumerGroupOffsets(eq(groupId), any())).thenReturn(offsetsResult);
        when(adminClient.listConsumerGroupOffsets(eq(groupId))).thenReturn(offsetsResult);
        when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(committedFuture);
        when(committedFuture.get(30, TimeUnit.SECONDS)).thenReturn(offsets);
    }

    private void stubLatestOffsets(Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets) throws Exception {
        when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);
        when(listOffsetsResult.all()).thenReturn(latestOffsetsFuture);
        when(latestOffsetsFuture.get(30, TimeUnit.SECONDS)).thenReturn(offsets);
    }

    private ListOffsetsResult.ListOffsetsResultInfo offsetInfo(long offset) {
        return new ListOffsetsResult.ListOffsetsResultInfo(offset, System.currentTimeMillis(), Optional.of(0));
    }
}
