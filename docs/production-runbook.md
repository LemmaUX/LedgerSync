# 🚨 Production Runbook: LedgerSync Reconciliation Engine

## Overview

This runbook provides step-by-step instructions for on-call engineers to diagnose and resolve issues with the LedgerSync reconciliation engine. The system processes financial transactions in real-time, so quick and accurate response is critical.

---

## 1. System Architecture Quick Reference

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│    Flink    │────▶│  PostgreSQL │
│ (CDC Events)│     │(Reconciliation)│   │ (Results)   │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Iceberg   │
                    │  (Audit Lake)│
                    └─────────────┘
```

**Key URLs:**
- **Flink UI:** `http://localhost:8082`
- **Kafka UI:** `http://localhost:9092` (or use CLI)
- **MinIO Console:** `http://localhost:9001` (credentials: minioadmin / minioadmin)
- **Airflow UI:** `http://localhost:8080` (credentials: admin / admin)
- **Grafana:** `http://localhost:3000`

---

## 2. Diagnóstico de Flink: Checkpoints y Backpressure

### Symptoms
- Grafana dashboard shows Kafka lag growing continuously
- Reconciliation latency exceeds SLA (>30s)
- Alert: "Checkpoint duration > 10s"

### Diagnosis Steps

1. **Open Flink UI:** Navigate to `http://localhost:8082`

2. **Go to Jobs > TemporalReconciliationJob**

3. **Check Checkpoint Duration:**
   - Navigate to **Checkpoints** tab
   - If **Checkpoint Duration > 10s**, there's contention in the State Backend (RocksDB/S3)
   - **Action:** 
     - Increase checkpoint interval if acceptable for business
     - Scale up TaskManagers with more memory for RocksDB
     ```bash
     kubectl scale deployment flink-taskmanager --replicas=6
     ```

4. **Check Backpressure:**
   - Navigate to **Task Managers** tab
   - Look for "High" or "Critical" backpressure indicators
   - If backpressure is high, TaskManagers cannot write to PostgreSQL/Iceberg fast enough
   - **Immediate Action:** Scale TaskManagers
     ```bash
     kubectl scale deployment flink-taskmanager --replicas=8
     ```

5. **Verify State Backend Health:**
   - Check S3/MinIO connectivity if using RocksDB incremental checkpoints
   - Ensure sufficient disk space on TaskManager nodes

---

## 3. Auditoría con Time Travel en Apache Iceberg

### Use Case
Regulatory request: "Show me the reconciliation state of transaction `tx-999` as it was on 2026-08-20 at 15:00:00"

### Steps

1. **Connect to Trino/Spark SQL** pointing to the Iceberg catalog in MinIO

2. **Execute Time Travel Query:**
   ```sql
   -- Conectar a Trino/Spark SQL apuntando al catálogo Iceberg en MinIO
   SELECT * FROM ledgersync.audit.reconciliation_history
   FOR TIMESTAMP AS OF TIMESTAMP '2026-08-20 15:00:00'
   WHERE transaction_id = 'tx-999';
   ```

3. **For full audit trail:**
   ```sql
   -- Show all snapshots for a table
   SELECT * FROM ledgersync.audit.reconciliation_history$snapshots;
   
   -- Query specific snapshot by ID
   SELECT * FROM ledgersync.audit.reconciliation_history
   FOR VERSION AS OF <snapshot_id>
   WHERE transaction_id = 'tx-999';
   ```

4. **Export results for compliance:**
   ```bash
   # Using AWS CLI with MinIO
   aws --endpoint-url http://localhost:9000 s3 cp s3://ledgersync/audit/reconciliation_history/ ./audit-export/
   ```

---

## 4. Ejecución Manual del Fallback de Airflow

### When to Use
- External provider had an outage; transactions arriving 2+ hours late (outside Flink's 30s window)
- Flink job missed reconciliations due to temporary network partition
- Need to reprocess historical data

### Steps

1. **Open Airflow UI:** `http://localhost:8080`

2. **Find DAG:** Search for `batch_reconciliation_fallback`

3. **Trigger DAG:**
   - Click on the DAG name
   - Click **Trigger DAG** (Run button)
   - Optionally set configuration:
     ```json
     {
       "start_date": "2026-08-20",
       "end_date": "2026-08-20",
       "source_system": "PAYMENT_GATEWAY"
     }
     ```

4. **Monitor Execution:**
   - Watch the task `verify_late_arrivals` logs
   - If matches are found, DB will be updated to `RECONCILED_LATE`
   - Alert will be sent to adjust Flink watermarks

5. **Post-Execution Actions:**
   - Verify reconciliation counts match expected values
   - If large discrepancies, notify engineering team
   - Consider adjusting Flink watermark strategy if late arrivals become frequent

---

## 5. Escalado de Emergencia

### Flink Scaling

**Scenario:** Processing throughput insufficient for transaction volume spike

```bash
# Increase TaskManager replicas
kubectl scale deployment flink-taskmanager --replicas=10

# Increase TaskManager memory (if resources allow)
kubectl patch deployment flink-taskmanager -p '{"spec":{"template":{"spec":{"containers":[{"name":"taskmanager","resources":{"requests":{"memory":"4Gi"}}}]}}}}'

# Monitor scaling progress
kubectl get pods -l app=flink-taskmanager -w
```

**Warning:** Scaling beyond 10 TaskManagers may require rebalancing Kafka partitions.

### PostgreSQL Scaling

**Scenario:** GraphQL API queries slow, reconciliation writes backing up

1. **Check index usage:**
   ```sql
   EXPLAIN ANALYZE SELECT * FROM reconciliations 
   WHERE status = 'EXCEPTION' AND created_at > NOW() - INTERVAL '1 hour';
   ```

2. **Verify critical indexes exist:**
   ```sql
   -- Index for unresolved exceptions
   CREATE INDEX IF NOT EXISTS idx_reconciliations_unresolved 
   ON reconciliations(status, created_at) 
   WHERE status = 'EXCEPTION';
   
   -- Index for transaction lookup
   CREATE INDEX IF NOT EXISTS idx_reconciliations_tx_id 
   ON reconciliations(transaction_id);
   ```

3. **Scale read replicas (if configured):**
   ```bash
   kubectl scale deployment postgresql-read-replica --replicas=3
   ```

### Kafka Scaling

**Scenario:** Producer throughput exceeding broker capacity

```bash
# Add Kafka brokers (advanced, requires rebalancing)
kubectl scale statefulset kafka-broker --replicas=5

# Increase topic partitions (requires consumer restart)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic canonical-transactions \
  --partitions 24
```

---

## 6. Common Alerts and Responses

| Alert | Severity | Immediate Action | Root Cause Analysis |
|-------|----------|------------------|---------------------|
| Checkpoint Duration > 10s | High | Scale TaskManagers | State backend contention, S3 latency |
| Kafka Lag > 10000 messages | Critical | Scale Flink + check backpressure | Insufficient parallelism, slow sinks |
| Missing Counterpart Rate > 5% | Medium | Trigger Airflow fallback | Provider outage, watermark misconfiguration |
| DLQ Volume Spike | High | Investigate data quality issue | Schema change, upstream bug |
| PostgreSQL Connection Pool Exhausted | Critical | Scale DB, increase pool size | Slow queries, connection leak |

---

## 7. Contact Escalation Matrix

| Issue Type | First Contact | Escalation |
|------------|---------------|------------|
| Infrastructure (K8s, Flink, Kafka) | On-call SRE | Platform Team Lead |
| Data Quality / Schema Issues | Data Engineer | Data Platform Lead |
| Business Logic / Reconciliation | Backend Engineer | Payments Team Lead |
| Security / Compliance | Security Engineer | CTO |

---

## 8. Post-Incident Checklist

After resolving any production incident:

- [ ] Document timeline in incident report
- [ ] Update runbook if new diagnosis steps discovered
- [ ] Review alerting thresholds (were they appropriate?)
- [ ] Schedule post-mortem if P0/P1 incident
- [ ] Verify all reconciliations caught up
- [ ] Confirm no data loss or corruption

---

**Last Updated:** 2026-08-20  
**Owner:** LedgerSync Engineering Team
