package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.ConsumerLagResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Service
public class LagService {

    private final AdminClient adminClient;

    public LagService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Cacheable(value = "lag", key = "#groupId + ':' + #topicName")
    public ConsumerLagResult getConsumerLag(String groupId, String topicName) {
        try {
            List<String> groups = resolveGroups(groupId);
            List<ConsumerLagResult.GroupLag> groupLags = new ArrayList<>();

            for (String gid : groups) {
                ConsumerGroupDescription desc = adminClient
                    .describeConsumerGroups(List.of(gid))
                    .all().get(30, SECONDS)
                    .get(gid);

                Map<TopicPartition, OffsetAndMetadata> committed = fetchCommittedOffsets(gid, topicName);
                if (committed.isEmpty()) continue;

                Map<TopicPartition, OffsetSpec> latestSpecs = committed.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    adminClient.listOffsets(latestSpecs).all().get(30, SECONDS);

                List<ConsumerLagResult.PartitionLag> partitionLags = committed.entrySet().stream()
                    .filter(e -> topicName == null || e.getKey().topic().equals(topicName))
                    .map(e -> {
                        TopicPartition tp = e.getKey();
                        long committed0 = e.getValue() != null ? e.getValue().offset() : 0;
                        long leo = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0;
                        long lag = Math.max(0, leo - committed0);
                        return new ConsumerLagResult.PartitionLag(
                            tp.topic(), tp.partition(), committed0, leo, lag);
                    })
                    .sorted(Comparator.comparing(ConsumerLagResult.PartitionLag::topic)
                        .thenComparingInt(ConsumerLagResult.PartitionLag::partition))
                    .collect(toList());

                long totalLag = partitionLags.stream().mapToLong(ConsumerLagResult.PartitionLag::lag).sum();
                groupLags.add(new ConsumerLagResult.GroupLag(
                    gid, totalLag, desc.state().toString(), partitionLags));
            }

            return new ConsumerLagResult(groupLags);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while getting consumer lag", e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Failed to get consumer lag: " + cause.getMessage(), e);
        }
    }

    private List<String> resolveGroups(String groupId)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (groupId != null && !groupId.isBlank()) return List.of(groupId);
        return adminClient.listConsumerGroups().valid().get(30, SECONDS).stream()
            .map(ConsumerGroupListing::groupId)
            .collect(toList());
    }

    private Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(String groupId, String topicName)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (topicName != null && !topicName.isBlank()) {
            List<TopicPartition> partitions = fetchTopicPartitions(topicName);
            return adminClient.listConsumerGroupOffsets(
                groupId,
                new ListConsumerGroupOffsetsOptions().topicPartitions(partitions)
            ).partitionsToOffsetAndMetadata().get(30, SECONDS);
        }
        return adminClient.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata().get(30, SECONDS);
    }

    private List<TopicPartition> fetchTopicPartitions(String topicName)
            throws InterruptedException, ExecutionException, TimeoutException {
        TopicDescription desc = adminClient.describeTopics(List.of(topicName))
            .allTopicNames().get(30, SECONDS).get(topicName);
        return desc.partitions().stream()
            .map(p -> new TopicPartition(topicName, p.partition()))
            .collect(toList());
    }
}
