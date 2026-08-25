# ADR-003: Data Lake Format

**Date:** 2026-08-27  
**Status:** Accepted  
**Decision:** Apache Iceberg

## Context

LedgerSync requires an immutable audit trail for regulatory compliance (7-year retention). The data lake must support:
- **Time travel** (query data as of a specific timestamp)
- **Schema evolution** (add columns without rewriting data)
- **ACID transactions** (concurrent reads/writes without conflicts)
- **Efficient querying** (predicate pushdown, partition pruning)
- **Integration with Flink** (streaming writes) and **Spark** (batch reads)

## Evaluated Alternatives

| Criterion | Apache Iceberg | Delta Lake | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Time Travel** | ✅ Native (snapshot isolation) | ✅ Native (snapshot isolation) | ✅ Native (timeline-based) |
| **Schema Evolution** | ✅ Add/drop/rename columns (no rewrite) | ✅ Add columns (drop/rename requires rewrite) | ✅ Add columns (drop/rename limited) |
| **ACID Transactions** | ✅ Optimistic concurrency control | ✅ Optimistic concurrency control | ✅ Optimistic concurrency control |
| **Flink Integration** | ✅ First-class (IcebergSink with streaming writes) | 🟡 Flink-Delta connector exists but less mature | ✅ First-class (Hudi-Flink sink) |
| **Spark Integration** | ✅ First-class | ✅ First-class (Databricks native) | ✅ First-class |
| **Query Performance** | 🟢 Excellent (hidden partitioning, data skipping) | 🟢 Excellent (data skipping, Z-ordering) | 🟡 Good (indexing, but less mature) |
| **Community & Governance** | 🟢 Apache project (neutral governance) | 🟡 Databricks-controlled (open-core model) | 🟢 Apache project (neutral governance) |
| **Complexity** | 🟡 Medium (requires understanding of metadata layers) | 🟢 Low (Databricks manages complexity) | 🔴 High (complex indexing, compaction) |

## Decision Rationale

**Apache Iceberg** is selected for the following reasons:

1. **Flink-First** — LedgerSync's primary write path is Flink (streaming reconciliation results). Iceberg's `FlinkSink` provides **streaming writes** with exactly-once guarantees. Delta Lake's Flink integration is less mature.

2. **Schema Evolution** — Financial schemas evolve frequently (new transaction types, new metadata fields). Iceberg allows **adding, dropping, and renaming columns** without rewriting existing data. Delta Lake requires rewrites for drop/rename.

3. **Neutral Governance** — Iceberg is an Apache project with neutral governance. Delta Lake is controlled by Databricks, which creates vendor lock-in risk (e.g., Databricks-specific features may not be available in open-source Delta).

4. **Time Travel for Audit** — Regulatory audits require querying data as of a specific timestamp (e.g., "show me all transactions as of 2025-12-31"). Iceberg's **snapshot isolation** provides this natively: `SELECT * FROM ledger_snapshots FOR TIMESTAMP AS OF '2025-12-31'`.

5. **Query Performance** — Iceberg's **hidden partitioning** (partition by `transaction_date` without exposing it in the schema) and **data skipping** (min/max statistics per file) provide excellent query performance without requiring users to understand partitioning logic.

## Trade-offs Accepted

| Trade-off | Impact | Mitigation |
| :--- | :--- | :--- |
| **Metadata Overhead** | Iceberg maintains metadata files (manifest lists, manifests) that add storage overhead. | Metadata is small (< 1% of data size). Use S3/GCS for cheap, durable storage. |
| **Compaction Required** | Small files accumulate over time (Flink writes many small files). Query performance degrades. | Run nightly compaction job (Spark or Flink batch) to merge small files into larger ones. |
| **Learning Curve** | Iceberg's metadata layers (metadata file → manifest list → manifest → data files) are complex. | Start with simple examples. Use Iceberg's REST catalog for easier management. |

## Evolution Trigger

If the team is **all-in on Databricks** and requires tight integration with Databricks notebooks/delta sharing, re-evaluate Delta Lake.
