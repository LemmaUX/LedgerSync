# 🏦 LedgerSync: Real-Time Payment Reconciliation Platform

LedgerSync es una plataforma de ingeniería de datos de grado producción diseñada para reconciliar transacciones financieras en tiempo real a través de sistemas heterogéneos (Ledger interno, Pasarelas de Pago, Bancos Adquirentes).

## 🏗️ Arquitectura

- **Ingesta:** Debezium CDC (PostgreSQL/MySQL) → Kafka
- **Procesamiento:** Apache Flink (Java) con semántica *Exactly-Once*, Event-Time Watermarks y Stateful Temporal Joins.
- **Almacenamiento Operacional:** PostgreSQL (para consultas de baja latencia del API).
- **Lago de Auditoría:** Apache Iceberg sobre MinIO (S3) para cumplimiento regulatorio (Time Travel, 7 años de retención).
- **Orquestación y Calidad:** Apache Airflow (Fallback batch y Data Quality checks).
- **API:** GraphQL (Python/Strawberry) para el equipo de operaciones.

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

## 🚀 Inicio Rápido (Local)

### Prerequisites

- Docker & Docker Compose
- Java 11+ (for Flink job development)
- Maven 3.8+ (for building Flink jobs)

### Start Infrastructure

```bash
# 1. Levantar infraestructura (Kafka, Schema Registry, Postgres, Debezium, Flink, MinIO)
docker-compose up -d
```

This starts:
- **Kafka** (port 9092) - Event transport layer
- **Schema Registry** (port 8081) - Avro schema management
- **PostgreSQL** (port 5432) - Operational DB + CDC source
- **Debezium** (port 8083) - CDC connector
- **Flink** (port 8082) - Stream processing cluster
- **MinIO** (port 9000/9001) - S3-compatible storage for Iceberg

### Build and Test Flink Jobs

```bash
# 2. Compilar jobs de Flink
cd flink-jobs/reconciliation-engine
mvn clean package

# 3. Ejecutar pruebas unitarias
mvn test
```

### Access UIs

- **Flink UI:** http://localhost:8082
- **MinIO Console:** http://localhost:9001 (credentials: minioadmin / minioadmin)
- **Airflow UI:** http://localhost:8080 (credentials: admin / admin)
- **Grafana:** http://localhost:3000

## 🛡️ Garantías de Ingeniería

| Garantía | Implementación | Beneficio |
|----------|----------------|-----------|
| **Tolerancia a datos fuera de orden** | Watermarks con 10s de latencia permitida | Manejo correcto de eventos tardíos |
| **Prevención de falsos positivos** | `KeyedProcessFunction` con Timers para confirmar transacciones perdidas | No marcar como excepción sin confirmación |
| **Calidad de datos** | Filtro *in-flight* con Dead Letter Queue para registros corruptos | Pipeline estable, datos auditables |
| **Exactly-Once Semantics** | Flink checkpoints + transactional sinks | Cero duplicados, cero pérdidas |
| **Audit Trail** | Apache Iceberg con Time Travel | Cumplimiento regulatorio (7 años) |

## 📁 Project Structure

```
ledgersync/
├── docs/
│   ├── adr/                    # Architecture Decision Records
│   │   ├── ADR-001-streaming-platform.md
│   │   ├── ADR-002-stream-processing-engine.md
│   │   └── ADR-003-data-lake-format.md
│   ├── domain-model.md         # Domain entities & relationships
│   └── production-runbook.md   # On-call runbook
├── schemas/                    # Avro schemas
│   ├── canonical_transaction.avsc
│   └── internal_ledger_transaction.avsc
├── flink-jobs/
│   ├── event-normalizer/       # Flink job for event normalization
│   │   ├── pom.xml
│   │   └── src/main/java/com/ledgersync/flink/
│   │       ├── EventNormalizerJob.java
│   │       └── NormalizerFunction.java
│   └── reconciliation-engine/  # Flink job for reconciliation
│       ├── pom.xml
│       └── src/
│           ├── main/java/com/ledgersync/
│           │   ├── domain/
│           │   │   ├── CanonicalTransaction.java
│           │   │   ├── Money.java
│           │   │   └── ReconciliationResult.java
│           │   └── flink/
│           │       ├── StatefulReconciliationFunction.java
│           │       ├── DataQualityFilterFunction.java
│           │       └── TemporalReconciliationJob.java
│           └── test/java/com/ledgersync/flink/
│               └── StatefulReconciliationFunctionTest.java
├── scripts/
│   └── init-db.sql             # Database initialization
├── docker-compose.yml          # Local development infrastructure
└── README.md
```

## 🧪 Testing

### Unit Tests

```bash
cd flink-jobs/reconciliation-engine
mvn test
```

Tests cover:
- Stateful reconciliation logic with Flink Test Utils
- Timer-based timeout handling
- Amount mismatch detection
- Data quality filter rules

### Integration Tests

```bash
# Run full pipeline with Docker Compose
docker-compose up -d
# Submit job and verify end-to-end flow
```

## 📊 Monitoring & Alerting

Key metrics to monitor in Grafana:
- **Kafka Lag:** Should remain < 1000 messages
- **Checkpoint Duration:** Alert if > 10s
- **Backpressure:** Alert on "High" or "Critical"
- **Reconciliation Rate:** Transactions reconciled per second
- **Exception Rate:** Percentage of transactions with exceptions

## 🔧 Troubleshooting

See [Production Runbook](docs/production-runbook.md) for detailed troubleshooting steps.

Quick reference:
- **High Kafka lag:** Scale Flink TaskManagers
- **Checkpoint failures:** Check S3/MinIO connectivity
- **Missing counterparts:** Trigger Airflow fallback DAG
- **DLQ spike:** Investigate upstream schema changes

## 📈 Next Steps

**Day 6+: Production Hardening**
- Implement comprehensive integration tests
- Set up CI/CD pipeline with automated deployment
- Configure production monitoring dashboards
- Document disaster recovery procedures
