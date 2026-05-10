package com.kafkatopology.mcp.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.kafkatopology.mcp.tool.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(KafkaConnectionConfig.class)
public class KafkaConfig {

    @Bean
    public AdminClient adminClient(KafkaConnectionConfig config) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);

        String protocol = config.getSecurityProtocol();
        if ("SASL_SSL".equals(protocol) || "SASL_PLAINTEXT".equals(protocol)) {
            props.put("security.protocol", protocol);
            props.put(SaslConfigs.SASL_MECHANISM, config.getSaslMechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG, config.buildJaasConfig());
        }

        return AdminClient.create(props);
    }

    @Bean
    public CacheManager cacheManager(KafkaConnectionConfig config) {
        CaffeineCacheManager manager = new CaffeineCacheManager();
        int topologyTtl = config.getCache().getTopologyTtlSeconds();
        int lagTtl = config.getCache().getLagTtlSeconds();

        manager.registerCustomCache("topology",
            Caffeine.newBuilder().expireAfterWrite(topologyTtl, TimeUnit.SECONDS).build());
        manager.registerCustomCache("skew",
            Caffeine.newBuilder().expireAfterWrite(topologyTtl, TimeUnit.SECONDS).build());
        manager.registerCustomCache("lag",
            Caffeine.newBuilder().expireAfterWrite(lagTtl, TimeUnit.SECONDS).build());

        return manager;
    }

    @Bean
    public ToolCallbackProvider kafkaTools(TopologyTool topologyTool,
                                           ConsumerLagTool lagTool,
                                           SkewDetectionTool skewTool,
                                           MessageFlowTool flowTool,
                                           PartitionStrategyTool partitionTool) {
        return MethodToolCallbackProvider.builder()
            .toolObjects(topologyTool, lagTool, skewTool, flowTool, partitionTool)
            .build();
    }
}
