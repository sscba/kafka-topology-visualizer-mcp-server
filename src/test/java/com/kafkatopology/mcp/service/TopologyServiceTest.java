package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.TopologyResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopologyServiceTest {

    @Mock private AdminClient adminClient;
    @Mock private DescribeClusterResult clusterResult;
    @Mock private ListTopicsResult listTopicsResult;
    @Mock private DescribeTopicsResult describeTopicsResult;
    @Mock private ListConsumerGroupsResult listGroupsResult;

    @Mock private KafkaFuture<Collection<Node>> nodesFuture;
    @Mock private KafkaFuture<Node> controllerFuture;
    @Mock private KafkaFuture<String> clusterIdFuture;
    @Mock private KafkaFuture<Set<String>> topicNamesFuture;
    @Mock private KafkaFuture<Map<String, TopicDescription>> topicDescFuture;
    @Mock private KafkaFuture<Collection<ConsumerGroupListing>> validGroupsFuture;

    private TopologyService service;

    @BeforeEach
    void setUp() {
        service = new TopologyService(adminClient);
    }

    @Test
    void returnsClusterIdAndBrokerList() throws Exception {
        Node broker1 = new Node(0, "broker-1", 9092);
        Node broker2 = new Node(1, "broker-2", 9092);

        stubCluster("cluster-abc", broker1, List.of(broker1, broker2));
        stubTopics(Set.of(), Map.of());

        TopologyResult result = service.describeTopology(null, false);

        assertThat(result.cluster().clusterId()).isEqualTo("cluster-abc");
        assertThat(result.cluster().brokers()).hasSize(2);
    }

    @Test
    void returnsTopicWithPartitionDetails() throws Exception {
        Node leader = new Node(0, "broker-1", 9092);
        stubCluster("cid", leader, List.of(leader));

        TopicPartitionInfo tpi = new TopicPartitionInfo(0, leader, List.of(leader), List.of(leader));
        TopicDescription desc = new TopicDescription("orders", false, List.of(tpi));
        stubTopics(Set.of("orders"), Map.of("orders", desc));

        TopologyResult result = service.describeTopology(null, false);

        assertThat(result.topics()).hasSize(1);
        TopologyResult.TopicInfo topic = result.topics().get(0);
        assertThat(topic.name()).isEqualTo("orders");
        assertThat(topic.partitionCount()).isEqualTo(1);
        assertThat(topic.replicationFactor()).isEqualTo(1);
    }

    @Test
    void detectsUnderReplicatedPartitions() throws Exception {
        Node leader = new Node(0, "broker-1", 9092);
        Node replica = new Node(1, "broker-2", 9092);
        stubCluster("cid", leader, List.of(leader));

        // 2 replicas but ISR only has 1 → under-replicated
        TopicPartitionInfo tpi = new TopicPartitionInfo(0, leader, List.of(leader, replica), List.of(leader));
        TopicDescription desc = new TopicDescription("risky-topic", false, List.of(tpi));
        stubTopics(Set.of("risky-topic"), Map.of("risky-topic", desc));

        TopologyResult result = service.describeTopology(null, false);

        assertThat(result.topics().get(0).partitions().get(0).isUnderReplicated()).isTrue();
    }

    @Test
    void filtersByTopicNameRegex() throws Exception {
        Node leader = new Node(0, "broker-1", 9092);
        stubCluster("cid", leader, List.of(leader));

        TopicPartitionInfo tpi = new TopicPartitionInfo(0, leader, List.of(leader), List.of(leader));
        TopicDescription ordersDesc = new TopicDescription("orders", false, List.of(tpi));
        TopicDescription paymentsDesc = new TopicDescription("payments", false, List.of(tpi));
        stubTopics(Set.of("orders", "payments"), Map.of("orders", ordersDesc, "payments", paymentsDesc));
        when(topicDescFuture.get(30, TimeUnit.SECONDS)).thenReturn(Map.of("orders", ordersDesc));

        TopologyResult result = service.describeTopology("^orders$", false);

        assertThat(result.topics()).hasSize(1);
        assertThat(result.topics().get(0).name()).isEqualTo("orders");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void stubCluster(String clusterId, Node controller, List<Node> brokers) throws Exception {
        when(adminClient.describeCluster()).thenReturn(clusterResult);
        when(clusterResult.nodes()).thenReturn(nodesFuture);
        when(clusterResult.controller()).thenReturn(controllerFuture);
        when(clusterResult.clusterId()).thenReturn(clusterIdFuture);
        when(nodesFuture.get(30, TimeUnit.SECONDS)).thenReturn(brokers);
        when(controllerFuture.get(30, TimeUnit.SECONDS)).thenReturn(controller);
        when(clusterIdFuture.get(30, TimeUnit.SECONDS)).thenReturn(clusterId);
    }

    private void stubTopics(Set<String> names, Map<String, TopicDescription> descriptions) throws Exception {
        when(adminClient.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(topicNamesFuture);
        when(topicNamesFuture.get(30, TimeUnit.SECONDS)).thenReturn(names);

        if (!names.isEmpty()) {
            when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
            when(describeTopicsResult.allTopicNames()).thenReturn(topicDescFuture);
            when(topicDescFuture.get(30, TimeUnit.SECONDS)).thenReturn(descriptions);
        }
    }
}
