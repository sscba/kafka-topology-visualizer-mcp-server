# kafka-topology-visualizer MCP Server — Setup & Usage Guide

A Spring Boot MCP server that connects to a live Kafka cluster and exposes five analysis tools to Claude. It communicates via **stdio (JSON-RPC 2.0)** — no HTTP listener.

---

## Prerequisites

| Requirement | Check |
|-------------|-------|
| Java 21+ | `java -version` |
| Maven 3.9+ | `mvn -version` |
| Docker + Docker Compose | `docker compose version` |
| Claude Desktop or Claude Code CLI | — |

---

## Step 1 — Start a Local Kafka Cluster

The `docker-compose.yml` in this repo sets up Zookeeper, a single Kafka broker, and seeds three topics (`orders`, `payments`, `inventory`).

```bash
docker compose up -d
```

Wait ~15 seconds for the `kafka-init` service to create the seed topics, then verify:

```bash
docker exec -it <kafka-container> /usr/bin/kafka-topics --list --bootstrap-server localhost:9092
```

To stop: `docker compose down`

> **Production / existing cluster:** Skip this step and point `kafka.bootstrap-servers` at your cluster in the next step.

---

## Step 2 — Configure Kafka Connection

Edit [src/main/resources/application.yml](src/main/resources/application.yml):

```yaml
kafka:
  bootstrap-servers: localhost:9092        # change for remote clusters
  security-protocol: PLAINTEXT             # PLAINTEXT | SASL_PLAINTEXT | SASL_SSL | SSL
  sasl-mechanism: PLAIN
  sasl-username: ""
  sasl-password: ""
  cache:
    topology-ttl-seconds: 30              # how long topology results are cached
    lag-ttl-seconds: 15                   # how long lag results are cached
  partition:
    default-producer-throughput-mbps: 10.0
```

For **SASL_SSL** (e.g. Confluent Cloud):
```yaml
kafka:
  bootstrap-servers: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
  security-protocol: SASL_SSL
  sasl-mechanism: PLAIN
  sasl-username: <API_KEY>
  sasl-password: <API_SECRET>
```

---

## Step 3 — Build the Server

```bash
mvn clean package -DskipTests
```

This produces:
```
target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar
```

---

## Step 4 — Register with Claude

### Option A: Claude Desktop (Windows)

Open `%APPDATA%\Claude\claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "kafka-topology-visualizer": {
      "command": "java",
      "args": [
        "-jar",
        "C:/Users/shivc/SDE/Project/MCP/kafka-topology-visualizer-mcp/kafkatopologyvisualizermcp/target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar"
      ]
    }
  }
}
```

Restart Claude Desktop. On the next launch the server starts automatically as a subprocess.

### Option B: Claude Code CLI

```bash
claude mcp add kafka-topology-visualizer java \
  -jar ./target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar
```

Or add to `.claude/mcp.json` in the project directory:

```json
{
  "mcpServers": {
    "kafka-topology-visualizer": {
      "command": "java",
      "args": ["-jar", "./target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar"]
    }
  }
}
```

Verify the server is connected:
```
/mcp
```

You should see `kafka-topology-visualizer` listed as connected with 5 tools.

---

## Step 5 — Tool Reference

The server exposes five tools. Claude selects and calls them automatically based on your natural-language prompt. You can also ask Claude to use a specific tool by name.

---

### `describeTopology`

Describes the full Kafka cluster: brokers, topics, partitions, leaders, ISR, replication factor, and optionally consumer group memberships.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `topicFilter` | String | No | (all topics) | Java regex to filter topic names |
| `includeConsumerGroups` | boolean | No | `false` | Attach consumer group names to each topic |

**Example prompts:**
- "Show me the full Kafka topology"
- "List all topics matching `^orders` and include their consumer groups"
- "Are there any under-replicated partitions in my cluster?"
- "Which broker is the controller?"

---

### `getConsumerLag`

Reports committed offset, log-end offset, and lag per partition for every consumer group. Filter by group or topic to narrow results.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `groupId` | String | No | (all groups) | Consumer group ID to filter |
| `topicName` | String | No | (all topics) | Topic name to filter |

**Example prompts:**
- "What is the consumer lag across all groups right now?"
- "Show lag for the consumer group `payments-service`"
- "How far behind is `inventory-worker` on the `inventory` topic?"

---

### `traceMessageFlow`

Traces which consumer groups subscribe to a topic, which members are assigned which partitions, and the per-member lag.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `topicName` | String | **Yes** | — | Topic to trace |
| `consumerGroup` | String | No | (all groups) | Limit trace to one consumer group |

**Example prompts:**
- "Trace the message flow for the `orders` topic"
- "Which consumers are reading from `payments` and what is each member's lag?"
- "Show me the partition assignments for group `fraud-detector` on `orders`"

---

### `detectSkew`

Compares log sizes / message counts across partitions and flags any that deviate beyond a threshold. Useful for identifying hot partitions caused by bad key distribution.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `topicName` | String | **Yes** | — | Topic to analyze |
| `thresholdPct` | Double | No | `20.0` | Deviation % above which a partition is flagged |

**Example prompts:**
- "Detect partition skew on the `inventory` topic"
- "Is there any skew in `payments` using a 15% threshold?"
- "Which partitions in `orders` are receiving disproportionately more data?"

---

### `suggestPartitionStrategy`

Recommends an optimal partition count and replication factor based on your throughput target, consumer count, and retention requirements. Also estimates storage.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `topicName` | String | **Yes** | — | Topic to evaluate |
| `targetThroughputMbps` | double | **Yes** | — | Target producer throughput in MB/s |
| `consumerCount` | int | **Yes** | — | Number of consumers in the group |
| `retentionDays` | int | **Yes** | — | Data retention period in days |

**Example prompts:**
- "Suggest a partition strategy for `orders`: 50 MB/s throughput, 12 consumers, 7-day retention"
- "How should I partition `payments` for 100 MB/s with 20 consumers and 14 days retention?"
- "I'm getting 200 MB/s on `events` with 30 consumers — what partition count do you recommend?"

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Server not listed in `/mcp` | Wrong jar path | Check path with `ls target/*.jar` |
| `Connection refused` on tool call | Kafka not running | `docker compose up -d` and wait 15s |
| `Authentication failed` | Wrong SASL credentials | Double-check `sasl-username`/`sasl-password` |
| Empty topology result | Wrong bootstrap server | Verify `kafka.bootstrap-servers` in `application.yml` |
| Stale topology data | Cache TTL | Results cache for 30s (topology) / 15s (lag) — wait or reduce TTL |

---

## Architecture Overview

```
Claude (Desktop / Code)
        │ stdio (JSON-RPC 2.0)
        ▼
kafka-topology-visualizer (Spring Boot jar)
        │ Kafka AdminClient
        ▼
Kafka Cluster (localhost:9092 or remote)
```

The server starts as a subprocess of Claude and communicates entirely over stdin/stdout. No ports are opened. Each tool call is a synchronous AdminClient request (with Caffeine TTL caching to avoid hammering the cluster).
