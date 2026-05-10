package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.TopologyResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Service
public class TopologyService {

    private final AdminClient adminClient;

    public TopologyService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Cacheable(value = "topology", key = "#topicFilter + ':' + #includeConsumerGroups")
    public TopologyResult describeTopology(String topicFilter, boolean includeConsumerGroups) {
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(30, SECONDS);
            Node controller = clusterResult.controller().get(30, SECONDS);
            String clusterId = clusterResult.clusterId().get(30, SECONDS);

            List<String> brokers = nodes.stream()
                .map(n -> n.host() + ":" + n.port() + " (id=" + n.id() + ")")
                .sorted()
                .collect(toList());
            String controllerStr = controller.host() + ":" + controller.port() + " (id=" + controller.id() + ")";

            ListTopicsOptions opts = new ListTopicsOptions().listInternal(false);
            Set<String> allTopics = adminClient.listTopics(opts).names().get(30, SECONDS);

            Set<String> filteredTopics = filterTopics(allTopics, topicFilter);

            List<TopologyResult.TopicInfo> topicInfos = new ArrayList<>();
            if (!filteredTopics.isEmpty()) {
                Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(filteredTopics)
                    .allTopicNames()
                    .get(30, SECONDS);

                topicInfos = descriptions.values().stream()
                    .map(this::toTopicInfo)
                    .sorted(Comparator.comparing(TopologyResult.TopicInfo::name))
                    .collect(toList());

                if (includeConsumerGroups) {
                    Map<String, List<String>> topicToGroups = buildTopicToGroupsMap(filteredTopics);
                    topicInfos = topicInfos.stream()
                        .map(t -> new TopologyResult.TopicInfo(
                            t.name(), t.partitionCount(), t.replicationFactor(), t.partitions(),
                            topicToGroups.getOrDefault(t.name(), List.of())))
                        .collect(toList());
                }
            }

            return new TopologyResult(
                new TopologyResult.ClusterInfo(clusterId, controllerStr, brokers),
                topicInfos
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while describing topology", e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Failed to describe topology: " + cause.getMessage(), e);
        }
    }

    private Set<String> filterTopics(Set<String> topics, String topicFilter) {
        if (topicFilter == null || topicFilter.isBlank()) return topics;
        Pattern p = Pattern.compile(topicFilter, Pattern.CASE_INSENSITIVE);
        return topics.stream()
            .filter(name -> p.matcher(name).find())
            .collect(Collectors.toSet());
    }

    private TopologyResult.TopicInfo toTopicInfo(TopicDescription desc) {
        List<TopologyResult.PartitionInfo> partitions = desc.partitions().stream()
            .map(tpi -> {
                List<String> replicas = tpi.replicas().stream()
                    .map(n -> String.valueOf(n.id())).collect(toList());
                List<String> isr = tpi.isr().stream()
                    .map(n -> String.valueOf(n.id())).collect(toList());
                String leaderStr = tpi.leader() != null
                    ? tpi.leader().host() + ":" + tpi.leader().port()
                    : "none";
                boolean underReplicated = isr.size() < replicas.size();
                return new TopologyResult.PartitionInfo(
                    tpi.partition(), leaderStr, replicas, isr, underReplicated);
            })
            .sorted(Comparator.comparingInt(TopologyResult.PartitionInfo::partition))
            .collect(toList());

        int replicationFactor = desc.partitions().isEmpty()
            ? 0
            : desc.partitions().get(0).replicas().size();

        return new TopologyResult.TopicInfo(
            desc.name(), desc.partitions().size(), replicationFactor, partitions, List.of());
    }

    private Map<String, List<String>> buildTopicToGroupsMap(Set<String> topics)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<String> groupIds = adminClient.listConsumerGroups().valid().get(30, SECONDS)
            .stream().map(ConsumerGroupListing::groupId).collect(toList());

        Map<String, TreeSet<String>> topicToGroups = new HashMap<>();
        for (String groupId : groupIds) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(30, SECONDS);

            offsets.keySet().stream()
                .map(TopicPartition::topic)
                .filter(topics::contains)
                .forEach(topic -> topicToGroups
                    .computeIfAbsent(topic, k -> new TreeSet<>())
                    .add(groupId));
        }

        return topicToGroups.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
    }
}
