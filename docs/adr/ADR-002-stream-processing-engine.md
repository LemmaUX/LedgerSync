# ADR-002: Stream Processing Engine

**Date:** 2026-08-27  
**Status:** Accepted  
**Decision:** Apache Flink 1.18 (Java API)

## Context

LedgerSync requires a stream processing engine capable of:
- **Stateful event-time processing** with watermarks (transactions arrive out-of-order from different sources)
- **Temporal joins** across multiple streams (match transaction from System A at T1 with System B at T1+45s)
- **Exactly-once semantics** via checkpointing
- **Fault tolerance** with sub-second recovery
- **Complex event processing** (exception classification: missing, duplicate, mismatch)

## Evaluated Alternatives

| Criterion | Apache Flink | Kafka Streams | Spark Structured Streaming |
| :--- | :--- | :--- | :--- |
| **Event-Time Processing** | ✅ Native (watermarks, allowed lateness) | ✅ Native (windowing) | ✅ Native (watermarks) |
| **Stateful Processing** | ✅ Rich state (value state, map state, list state) | ✅ KeyValueStore (limited) | ✅ Stateful aggregations (less flexible) |
| **Temporal Joins** | ✅ Interval joins (match events within time window) | ❌ Not supported (requires workarounds) | ⚠️ Limited (requires manual implementation) |
| **Exactly-Once** | ✅ Checkpointing + two-phase commit | ✅ Exactly-once (but limited to Kafka) | ✅ Checkpointing (but higher latency) |
| **Fault Tolerance** | ✅ Sub-second recovery (incremental checkpoints) | ✅ Fast recovery (but single JVM) | ⚠️ Minutes (micro-batch model) |
| **Latency** | 🟢 Sub-second (true streaming) | 🟢 Sub-second (true streaming) | 🟡 Seconds (micro-batch) |
| **Complexity** | 🔴 High (steep learning curve, verbose API) | 🟢 Low (embedded in Java app, simple API) | 🟡 Medium (SQL-like API, but batch-oriented mindset) |
| **Fintech Adoption** | 🟢 Standard for real-time reconciliation (Stripe, Square) | 🟡 Used for simpler use cases (enrichment, filtering) | 🟡 Used for batch-heavy workloads (ETL, reporting) |

## Decision Rationale

**Apache Flink** is selected for the following reasons:

1. **Temporal Joins** — The core reconciliation problem is matching transactions across sources within a time window. Flink's **interval joins** (`eventA.timestamp BETWEEN eventB.timestamp - INTERVAL '30' SECOND AND eventB.timestamp + INTERVAL '30' SECOND`) are purpose-built for this. Kafka Streams has no equivalent; you'd have to implement custom state management.

2. **Event-Time Semantics** — Transactions from different sources arrive with different clocks and network delays. Flink's **watermarks** and **allowed lateness** handle out-of-order events correctly. A transaction from System A at T1 might arrive at the reconciler at T1+10s, while its counterpart from System B arrives at T1+45s. Flink matches them correctly; Kafka Streams would require complex workarounds.

3. **Stateful Exception Classification** — After matching, we need to classify exceptions (missing, duplicate, mismatch). Flink's **KeyedState** (e.g., `MapState<transactionId, Transaction>`) allows efficient lookups and updates. Kafka Streams' `KeyValueStore` is less flexible for complex state structures.

4. **Exactly-Once Guarantees** — Flink's **checkpointing** (every 10s) + **two-phase commit** to Kafka sinks guarantee that a transaction is reconciled exactly once, even if the Flink job crashes mid-processing. This is non-negotiable for financial correctness.

5. **Industry Standard** — Flink is the de facto standard for real-time reconciliation in fintech (Stripe's Sigma, Square's reconciliation engine). Using Flink signals to interviewers that you understand the state of the art.

## Trade-offs Accepted

| Trade-off | Impact | Mitigation |
| :--- | :--- | :--- |
| **Steep Learning Curve** | Flink's API is verbose and complex. Watermarks, checkpoints, and state management require deep understanding. | Start with simple examples (word count, windowed aggregation). Use Flink's Table API (SQL-like) for simpler logic. Allocate 2 weeks for team ramp-up. |
| **Operational Overhead** | Flink requires a cluster (JobManager + TaskManagers), checkpoint storage (S3/HDFS), and monitoring. | Use Flink's Kubernetes operator for production. For local dev, run in standalone mode (single JVM). |
| **Java-Only (for production)** | Flink's Python API (PyFlink) is less mature and lacks some features (e.g., complex state). | Use Java for Flink jobs. Python for API/tooling. Clear separation of concerns. |

## Evolution Trigger

If the reconciliation logic becomes **simple key-value lookups** (no temporal joins, no complex state), re-evaluate Kafka Streams for its lower operational overhead.
