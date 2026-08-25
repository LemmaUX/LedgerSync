# ADR-001: Streaming Platform Selection

**Date:** 2026-08-27  
**Status:** Accepted  
**Decision:** Apache Kafka (Confluent Platform 7.x)

## Context

LedgerSync requires a durable, ordered event transport layer for financial transactions. The platform must support:
- **Exactly-once semantics** for financial correctness
- **Schema evolution** via Schema Registry
- **Multi-source ingestion** from CDC connectors
- **7-year retention** for regulatory compliance
- **Sub-second latency** for real-time reconciliation

## Evaluated Alternatives

| Criterion | Apache Kafka | Redpanda | Apache Pulsar |
| :--- | :--- | :--- | :--- |
| **Exactly-once** | ✅ Native (idempotent producers + transactions) | ✅ Native | ⚠️ Limited (bookkeeper-based) |
| **Schema Registry** | ✅ Confluent Schema Registry (Avro/JSON/Protobuf) | ⚠️ Compatible but limited ecosystem | ✅ Pulsar Schema Registry |
| **CDC Connectors** | ✅ Debezium (mature, production-proven) | ✅ Debezium (compatible) | ⚠️ Native connectors, less mature |
| **Retention** | ✅ Configurable per-topic (days to years) | ✅ Configurable | ✅ Configurable per-namespace |
| **Operational Complexity** | 🟡 High (ZooKeeper or KRaft, JVM-based) | 🟢 Low (single binary, no JVM) | 🔴 Very High (BookKeeper + ZooKeeper + broker) |
| **Community & Hiring** | 🟢 Largest ecosystem, easy to hire | 🟡 Growing, smaller talent pool | 🔴 Niche, hard to hire |
| **Flink Integration** | ✅ First-class (KafkaSource/Sink) | ✅ Compatible (Kafka protocol) | 🟡 Pulsar-Flink connector exists but less mature |

## Decision Rationale

**Apache Kafka** is selected for the following reasons:

1. **Financial Correctness Guarantees** — Kafka's transactional API (introduced in 0.11) provides exactly-once semantics across producer → broker → consumer. For reconciliation, a missed or duplicated transaction is a financial liability. Kafka's idempotent producers + transactional consumers guarantee this.

2. **Debezium Maturity** — Debezium's Kafka Connect source connectors for PostgreSQL and MySQL are production-proven at scale (Stripe, Uber, Shopify). The CDC layer is the most fragile part of the system; we need the most battle-tested connector ecosystem.

3. **Flink Integration** — Flink's `KafkaSource` and `KafkaSink` are first-class citizens with exactly-once guarantees via two-phase commit. Pulsar-Flink integration exists but is less mature and lacks the same level of community support.

4. **Schema Evolution** — Confluent Schema Registry provides backward/forward compatibility checks at write time. This prevents schema drift across sources — a critical requirement when reconciling heterogeneous systems.

5. **Hiring & Community** — Kafka engineers are abundant. In a fintech environment, the ability to hire and onboard quickly is a strategic advantage.

## Trade-offs Accepted

| Trade-off | Impact | Mitigation |
| :--- | :--- | :--- |
| **Operational Complexity** | Kafka requires ZooKeeper (or KRaft) + Schema Registry + Kafka Connect. More moving parts than Redpanda. | Use Confluent Cloud for production (managed service). For local dev, use Docker Compose with KRaft mode (no ZooKeeper). |
| **JVM Overhead** | Kafka brokers are JVM-based, consuming more memory than Redpanda's Rust implementation. | Allocate 4-8GB heap per broker. For 5k TPS, 3 brokers are sufficient. |
| **Cost** | Confluent Platform has licensing costs for enterprise features (Schema Registry, Connect). | Use open-source Apache Kafka + Apicurio Registry (free alternative) for cost-sensitive deployments. |

## Evolution Trigger

If throughput exceeds **100k TPS sustained** or **latency requirements tighten to < 1ms p99**, re-evaluate Redpanda for its lower latency and reduced JVM overhead.
