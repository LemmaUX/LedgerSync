# LedgerSync

**Real-time Financial Transaction Reconciliation System**

LedgerSync is a stream processing system for real-time reconciliation of financial transactions across heterogeneous sources (internal ledger, payment gateways, acquiring banks). It detects and classifies exceptions (missing, duplicate, mismatch) with sub-second latency.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│   Debezium      │────▶│     Kafka       │
│  (Internal      │  CDC│   (CDC          │ Avro│  (Event         │
│   Ledger)       │     │   Connector)    │     │   Transport)    │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Iceberg      │◀────│     Flink       │◀────│     Kafka       │
│   (Data Lake    │Write│   (Reconciliation│Norm.│  (Canonical     │
│   7-year        │     │   Engine)       │     │   Transactions) │
│   Retention)    │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │  (Exceptions &  │
                        │   Matches)      │
                        └─────────────────┘
```

## Day 1 Deliverables Status

| Deliverable | Status | Location |
| :--- | :--- | :--- |
| **ADR-001: Streaming Platform** | ✅ Complete | `docs/adr/ADR-001-streaming-platform.md` |
| **ADR-002: Stream Processing Engine** | ✅ Complete | `docs/adr/ADR-002-stream-processing-engine.md` |
| **ADR-003: Data Lake Format** | ✅ Complete | `docs/adr/ADR-003-data-lake-format.md` |
| **Avro Schemas** | ✅ Complete | `schemas/` |
| **Docker Compose** | ✅ Complete | `docker-compose.yml` |
| **Flink Job Skeleton** | ✅ Complete | `flink-jobs/event-normalizer/` |
| **Domain Model** | ✅ Complete | `docs/domain-model.md` |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Java 11+ (for Flink job development)
- Maven 3.8+ (for building Flink jobs)

### Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- **Kafka** (port 9092) - Event transport layer
- **Schema Registry** (port 8081) - Avro schema management
- **PostgreSQL** (port 5432) - Operational DB + CDC source
- **Debezium** (port 8083) - CDC connector
- **Flink** (port 8082) - Stream processing cluster
- **MinIO** (port 9000/9001) - S3-compatible storage for Iceberg

### Build Flink Job

```bash
cd flink-jobs/event-normalizer
mvn clean package
```

### Submit Flink Job

```bash
# Upload JAR to Flink cluster
curl -X POST -H "Expect:" -F "jarfile=@target/event-normalizer-1.0.0-SNAPSHOT.jar" \
  http://localhost:8082/jars/upload

# Submit job
curl -X POST -H "Content-Type: application/json" \
  -d '{"jar-id": "<jar-id-from-upload>"}' \
  http://localhost:8082/jars/<jar-id>/run
```

## Project Structure

```
ledgersync/
├── docs/
│   ├── adr/                    # Architecture Decision Records
│   │   ├── ADR-001-streaming-platform.md
│   │   ├── ADR-002-stream-processing-engine.md
│   │   └── ADR-003-data-lake-format.md
│   └── domain-model.md         # Domain entities & relationships
├── schemas/                    # Avro schemas
│   ├── canonical_transaction.avsc
│   └── internal_ledger_transaction.avsc
├── flink-jobs/
│   └── event-normalizer/       # Flink job for event normalization
│       ├── pom.xml
│       └── src/main/java/com/ledgersync/flink/
│           ├── EventNormalizerJob.java
│           └── NormalizerFunction.java
├── scripts/
│   └── init-db.sql             # Database initialization
├── docker-compose.yml          # Local development infrastructure
└── README.md
```

## Next Steps

**Day 2**: Implement the Temporal Reconciliation Engine
- Interval joins across multiple transaction streams
- Exception classification (missing, duplicate, mismatch)
- Dual writes to PostgreSQL (operational) and Iceberg (audit)

## Architectural Principles

1. **Every component must justify a concrete architectural property** — No stack padding
2. **Exactly-once semantics** — Financial correctness is non-negotiable
3. **Immutable audit trail** — 7-year retention for regulatory compliance
4. **Schema evolution** — Backward/forward compatibility at write time
5. **Event-time processing** — Handle out-of-order events correctly
