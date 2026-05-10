# kafka-topology-visualizer-mcp

[![CI](https://github.com/shivchandekar/kafka-topology-visualizer-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/shivchandekar/kafka-topology-visualizer-mcp/actions/workflows/ci.yml)

A Spring Boot MCP server that connects to a live Kafka cluster and exposes five analysis tools to Claude — covering topology, consumer lag, message flow tracing, partition skew detection, and partition strategy recommendations. Communicates over **stdio (JSON-RPC 2.0)**, no HTTP listener required.

```
Claude (Desktop / Code)
        │  stdio (JSON-RPC 2.0)
        ▼
kafka-topology-visualizer (Spring Boot jar)
        │  Kafka AdminClient
        ▼
Kafka Cluster (localhost:9092 or remote) 
```

---

## Tools

| Tool | Description |
|------|-------------|
| `describeTopology` | Brokers, topics, partitions, leaders, ISR, replication factor, optional consumer groups |
| `getConsumerLag` | Committed offset, log-end offset, and lag per partition per consumer group |
| `traceMessageFlow` | Consumer group subscriptions, partition assignments, and per-member lag for a topic |
| `detectSkew` | Flags partitions deviating beyond a configurable threshold — identifies hot partitions |
| `suggestPartitionStrategy` | Recommends partition count, replication factor, and storage estimate from throughput/consumer/retention inputs |

For full parameter reference and example Claude prompts, see [GUIDE.md](GUIDE.md).

---

## Prerequisites

| Requirement | Version | Check |
|-------------|---------|-------|
| Java | 21+ | `java -version` |
| Maven | 3.9+ | `mvn -version` |
| Docker + Docker Compose | Any recent | `docker compose version` |
| Claude Desktop or Claude Code CLI | Latest | — |

---

## Quick Start

### 1. Start a local Kafka cluster

The included `docker-compose.yml` starts Zookeeper, a Kafka broker, and seeds three topics (`orders`, `payments`, `inventory`):

```bash
docker compose up -d
```

Wait ~15 seconds for `kafka-init` to create the seed topics.

> **Using an existing cluster?** Skip this step and set `kafka.bootstrap-servers` in `application.yml`.

### 2. Configure the Kafka connection

Edit [src/main/resources/application.yml](src/main/resources/application.yml):

```yaml
kafka:
  bootstrap-servers: localhost:9092        # your broker address
  security-protocol: PLAINTEXT             # PLAINTEXT | SASL_PLAINTEXT | SASL_SSL | SSL
  sasl-mechanism: PLAIN
  sasl-username: ""
  sasl-password: ""
```

For **Confluent Cloud / SASL_SSL**:

```yaml
kafka:
  bootstrap-servers: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
  security-protocol: SASL_SSL
  sasl-mechanism: PLAIN
  sasl-username: <API_KEY>
  sasl-password: <API_SECRET>
```

### 3. Build the jar

```bash
mvn clean package -DskipTests
```

Output: `target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar`

### 4. Register with Claude

**Claude Desktop (Windows)** — add to `%APPDATA%\Claude\claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kafka-topology-visualizer": {
      "command": "java",
      "args": ["-jar", "C:/path/to/target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar"]
    }
  }
}
```

Restart Claude Desktop. The server starts automatically as a subprocess.

**Claude Code CLI**:

```bash
claude mcp add kafka-topology-visualizer java \
  -jar ./target/kafkatopologyvisualizermcp-0.0.1-SNAPSHOT.jar
```

Verify the connection:

```
/mcp
```

You should see `kafka-topology-visualizer` listed as connected with 5 tools.

---

## Usage

Once registered, describe what you want in plain English:

```
Show me the full Kafka topology
What is the consumer lag for the payments-service group?
Trace the message flow for the orders topic
Detect partition skew on inventory with a 15% threshold
Suggest a partition strategy for orders: 50 MB/s, 12 consumers, 7-day retention
```

For the complete tool reference, configuration options, troubleshooting guide, and more example prompts, see **[GUIDE.md](GUIDE.md)**.

---

## Running Tests

Tests use `spring-kafka-test` (embedded Kafka) — no running cluster needed:

```bash
mvn test
```

Five service-layer test classes cover all tools.

---

## Project Structure

```
src/
├── main/java/com/kafkatopology/mcp/
│   ├── config/          # KafkaConfig, KafkaConnectionConfig
│   ├── model/           # Result DTOs
│   ├── service/         # Business logic (TopologyService, LagService, …)
│   └── tool/            # MCP tool definitions exposed to Claude
└── test/java/com/kafkatopology/mcp/service/
    └── *Test.java       # Unit tests for all services
```

---

## Tech Stack

- **Java 21** / Spring Boot 3.4.5
- **Spring AI 1.0.0** — MCP server (stdio transport)
- **Apache Kafka 3.7.0** / Spring Kafka 3.3.11
- **Caffeine** — in-process TTL cache (topology: 30s, lag: 15s)
- **Lombok** / Jackson
