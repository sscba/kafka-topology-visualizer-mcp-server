package com.kafkatopology.mcp.service;

import com.kafkatopology.mcp.model.MessageFlowResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Service
public class FlowService {

    private static final String PRODUCER_LIMITATION_NOTE =
        "Note: Producer-side lineage is not tracked by Kafka AdminClient. " +
        "This view shows consumer-side flow only. " +
        "Integrate with Confluent Schema Registry to discover producer origins.";

    private final AdminClient adminClient;

    public FlowService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public MessageFlowResult traceMessageFlow(String topicName, String consumerGroup) {
        try {
            TopicDescription topicDesc = adminClient.describeTopics(List.of(topicName))
                .allTopicNames().get(30, SECONDS).get(topicName);

            List<String> candidateGroups = resolveCandidateGroups(consumerGroup);
            List<MessageFlowResult.ConsumerGroupInfo> groupInfos = new ArrayList<>();

            for (String gid : candidateGroups) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                    .listConsumerGroupOffsets(gid)
                    .partitionsToOffsetAndMetadata().get(30, SECONDS);

                boolean consumesTopic = offsets.keySet().stream()
                    .anyMatch(tp -> tp.topic().equals(topicName));
                if (!consumesTopic) continue;

                ConsumerGroupDescription desc = adminClient
                    .describeConsumerGroups(List.of(gid))
                    .all().get(30, SECONDS).get(gid);

                Map<TopicPartition, OffsetSpec> latestSpecs = offsets.keySet().stream()
                    .filter(tp -> tp.topic().equals(topicName))
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    adminClient.listOffsets(latestSpecs).all().get(30, SECONDS);

                List<MessageFlowResult.MemberInfo> members = desc.members().stream()
                    .map(member -> buildMemberInfo(member, topicName, offsets, latestOffsets))
                    .collect(toList());

                long totalLag = members.stream().mapToLong(MessageFlowResult.MemberInfo::totalLag).sum();
                groupInfos.add(new MessageFlowResult.ConsumerGroupInfo(
                    gid, desc.state().toString(), members, totalLag));
            }

            return new MessageFlowResult(
                topicName,
                topicDesc.partitions().size(),
                topicDesc.partitions().isEmpty() ? 0 : topicDesc.partitions().get(0).replicas().size(),
                groupInfos,
                PRODUCER_LIMITATION_NOTE
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while tracing message flow", e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Failed to trace message flow: " + cause.getMessage(), e);
        }
    }

    private List<String> resolveCandidateGroups(String consumerGroup)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (consumerGroup != null && !consumerGroup.isBlank()) return List.of(consumerGroup);
        return adminClient.listConsumerGroups().valid().get(30, SECONDS).stream()
            .map(ConsumerGroupListing::groupId)
            .collect(toList());
    }

    private MessageFlowResult.MemberInfo buildMemberInfo(
            MemberDescription member,
            String topicName,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets) {

        List<String> assigned = member.assignment().topicPartitions().stream()
            .filter(tp -> tp.topic().equals(topicName))
            .map(tp -> topicName + "-" + tp.partition())
            .sorted()
            .collect(toList());

        long memberLag = member.assignment().topicPartitions().stream()
            .filter(tp -> tp.topic().equals(topicName))
            .mapToLong(tp -> {
                long committed = offsets.containsKey(tp) && offsets.get(tp) != null
                    ? offsets.get(tp).offset() : 0;
                long leo = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0;
                return Math.max(0, leo - committed);
            }).sum();

        return new MessageFlowResult.MemberInfo(member.clientId(), assigned, memberLag);
    }
}
