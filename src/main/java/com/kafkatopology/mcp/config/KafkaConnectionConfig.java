package com.kafkatopology.mcp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaConnectionConfig {

    private String bootstrapServers = "localhost:9092";
    private String securityProtocol = "PLAINTEXT";
    private String saslMechanism = "PLAIN";
    private String saslUsername = "";
    private String saslPassword = "";
    private Cache cache = new Cache();
    private Partition partition = new Partition();

    @Data
    public static class Cache {
        private int topologyTtlSeconds = 30;
        private int lagTtlSeconds = 15;
    }

    @Data
    public static class Partition {
        private double defaultProducerThroughputMbps = 10.0;
    }

    public String buildJaasConfig() {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + saslUsername + "\" password=\"" + saslPassword + "\";";
    }
}
