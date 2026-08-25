# LedgerSync Domain Model

## Overview

LedgerSync operates on three core domain entities that form the foundation of the reconciliation system:

1. **Transaction** - The atomic unit of financial data from source systems
2. **Reconciliation Match** - The result of matching transactions across sources
3. **Exception** - Classified discrepancies requiring resolution
4. **AuditRecord** - Immutable audit trail for compliance

---

## Entity: Transaction

Represents a financial transaction normalized to the canonical schema.

### Attributes

| Field | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | UUID | Unique identifier (UUID v4) |
| `source_system` | Enum | Origin: `INTERNAL_LEDGER`, `PAYMENT_GATEWAY`, `ACQUIRING_BANK` |
| `external_reference` | String? | Reference ID from source system (nullable) |
| `amount` | Money | Transaction amount in minor units |
| `status` | Enum | `PENDING`, `COMPLETED`, `FAILED`, `REFUNDED`, `CHARGED_BACK` |
| `transaction_type` | Enum | `PAYMENT`, `REFUND`, `CHARGEBACK`, `FEE`, `TRANSFER` |
| `occurred_at` | Timestamp | When transaction occurred (source clock) |
| `ingested_at` | Timestamp | When ingested by LedgerSync |
| `metadata` | Map<String,String>? | Additional context (card brand, MCC, etc.) |

### Money Value Object

```json
{
  "value": 10000,      // Amount in minor units (cents)
  "currency": "USD"    // ISO 4217 currency code
}
```

### Architectural Justification

- **Canonical Schema**: Enables uniform processing across heterogeneous sources
- **Immutable After Ingestion**: Transactions are never updated; corrections arrive as new events
- **Dual Timestamps**: `occurred_at` enables event-time processing; `ingested_at` enables latency monitoring

---

## Entity: Reconciliation Match

Represents the result of matching transactions across multiple source systems within a time window.

### Attributes

| Field | Type | Description |
| :--- | :--- | :--- |
| `match_id` | UUID | Unique match identifier |
| `transaction_ids` | List<UUID> | IDs of matched transactions (one per source) |
| `status` | Enum | `MATCHED`, `PARTIAL_MATCH`, `UNMATCHED` |
| `matched_at` | Timestamp | When the match was computed |
| `confidence_score` | Float | Match confidence (0.0 - 1.0) |
| `matching_rules` | List<String> | Applied rules (e.g., `amount_equals`, `time_window_30s`) |

### Lifecycle

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  PENDING    │────▶│  MATCHED     │────▶│  FINALIZED  │
└─────────────┘     └──────────────┘     └─────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │  EXCEPTION   │
                  └──────────────┘
```

### Architectural Justification

- **Explicit Match Identity**: Enables tracking match history and reprocessing
- **Rule Transparency**: Auditors can verify which rules produced each match
- **Confidence Scoring**: Enables prioritization of manual review

---

## Entity: Exception

Represents a classified discrepancy between source systems requiring resolution.

### Attributes

| Field | Type | Description |
| :--- | :--- | :--- |
| `exception_id` | UUID | Unique exception identifier |
| `match_id` | UUID? | Associated match (null if no partial match) |
| `transaction_ids` | List<UUID> | Related transaction IDs |
| `exception_type` | Enum | `MISSING`, `DUPLICATE`, `AMOUNT_MISMATCH`, `STATUS_MISMATCH`, `TIME_ANOMALY` |
| `severity` | Enum | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `details` | JSON | Structured details about the discrepancy |
| `created_at` | Timestamp | When exception was detected |
| `resolved_at` | Timestamp? | When exception was resolved |
| `resolution_code` | String? | Resolution classification |

### Exception Types

| Type | Description | Example |
| :--- | :--- | :--- |
| `MISSING` | Transaction exists in one source but not others | Internal ledger has TXN, payment gateway does not |
| `DUPLICATE` | Same transaction appears multiple times in a source | Gateway shows two charges for same UUID |
| `AMOUNT_MISMATCH` | Amounts differ across sources | Ledger: $100.00, Gateway: $99.50 |
| `STATUS_MISMATCH` | Status differs across sources | Ledger: COMPLETED, Gateway: PENDING |
| `TIME_ANOMALY` | Timestamps outside expected window | Occurred at T1, ingested at T1+6h |

### Severity Classification

| Severity | Criteria | SLA |
| :--- | :--- | :--- |
| `LOW` | Minor timing differences (< 1 min), auto-resolvable | 7 days |
| `MEDIUM` | Amount mismatch < $100, status delay | 24 hours |
| `HIGH` | Amount mismatch >= $100, missing transactions | 4 hours |
| `CRITICAL` | Potential fraud, regulatory impact | Immediate |

### Architectural Justification

- **Typed Exceptions**: Enables automated resolution workflows per type
- **Severity-Based Routing**: Critical exceptions escalate to humans immediately
- **Structured Details**: Machine-readable format enables automated remediation

---

## Entity: AuditRecord

Immutable record of all state changes for regulatory compliance (7-year retention).

### Attributes

| Field | Type | Description |
| :--- | :--- | :--- |
| `record_id` | UUID | Unique audit record identifier |
| `entity_type` | Enum | `TRANSACTION`, `MATCH`, `EXCEPTION` |
| `entity_id` | UUID | ID of the entity being audited |
| `action` | Enum | `CREATED`, `UPDATED`, `DELETED` |
| `changed_by` | String | Actor: `system`, `user:<id>`, `job:<name>` |
| `changed_at` | Timestamp | When change occurred |
| `before_state` | JSON? | State before change (null for CREATED) |
| `after_state` | JSON? | State after change (null for DELETED) |
| `correlation_id` | UUID? | Links related audit records (e.g., same job run) |

### Architectural Justification

- **Complete History**: Every state transition is recorded immutably
- **Actor Attribution**: Clear accountability for each change
- **Before/After Snapshots**: Enables reconstruction of any historical state
- **Correlation ID**: Groups related changes for debugging and auditing

---

## Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                        DOMAIN MODEL                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐                                       │
│  │ Transaction      │                                       │
│  │──────────────────│                                       │
│  │ transaction_id   │◄──────┐                               │
│  │ source_system    │       │                               │
│  │ amount           │       │                               │
│  │ status           │       │                               │
│  │ occurred_at      │       │                               │
│  └──────────────────┘       │                               │
│                             │ 1:N                           │
│                             ▼                               │
│  ┌──────────────────┐       │    ┌──────────────────┐      │
│  │ Reconciliation   │       │    │ Exception        │      │
│  │ Match            │       │    │──────────────────│      │
│  │──────────────────│       │    │ exception_id     │      │
│  │ match_id         │       │    │ match_id         │      │
│  │ transaction_ids  │───────┘    │ exception_type   │      │
│  │ status           │            │ severity         │      │
│  │ matched_at       │            │ resolved_at      │      │
│  └──────────────────┘            └──────────────────┘      │
│                                                             │
│  ┌──────────────────┐                                       │
│  │ AuditRecord      │                                       │
│  │──────────────────│                                       │
│  │ record_id        │                                       │
│  │ entity_type      │  (Transaction, Match, Exception)     │
│  │ entity_id        │                                       │
│  │ action           │  (CREATED, UPDATED, DELETED)         │
│  │ changed_by       │  (system, user)                      │
│  │ changed_at       │                                       │
│  │ before_state     │  (JSON)                              │
│  │ after_state      │  (JSON)                              │
│  └──────────────────┘                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Invariants

1. **Transaction Immutability**: Once ingested, a Transaction's `occurred_at` and `amount` cannot change. Corrections arrive as new events with the same `transaction_id`.

2. **Match Completeness**: A Reconciliation Match must reference at least two transactions from different source systems (or one transaction with explicit `UNMATCHED` status).

3. **Exception Traceability**: Every Exception must be traceable to at least one Transaction. Orphaned exceptions are invalid.

4. **Audit Integrity**: AuditRecords are append-only. Deletion or modification of an AuditRecord is prohibited.

---

## Compliance Requirements

- **Retention Period**: 7 years (regulatory requirement for financial records)
- **Data Residency**: All data must remain in-region (no cross-border replication without encryption)
- **Access Logging**: All queries to production data must be logged with actor attribution
- **Right to Erasure**: Not applicable (financial records exempt under GDPR Article 17(3)(b))
