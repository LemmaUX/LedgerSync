package com.ledgersync.domain;

/**
 * CanonicalTransaction - Normalized transaction record from any source system.
 */
public class CanonicalTransaction {
    
    public enum SourceSystem {
        INTERNAL_LEDGER,
        PAYMENT_GATEWAY,
        ACQUIRING_BANK
    }

    private String transactionId;
    private SourceSystem sourceSystem;
    private String externalReference;
    private Money amount;
    private String status;
    private String transactionType;
    private Long occurredAt;
    private Long ingestedAt;

    public CanonicalTransaction() {}

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public SourceSystem getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(SourceSystem sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getExternalReference() {
        return externalReference;
    }

    public void setExternalReference(String externalReference) {
        this.externalReference = externalReference;
    }

    public Money getAmount() {
        return amount;
    }

    public void setAmount(Money amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public Long getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Long occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Long getIngestedAt() {
        return ingestedAt;
    }

    public void setIngestedAt(Long ingestedAt) {
        this.ingestedAt = ingestedAt;
    }

    @Override
    public String toString() {
        return "CanonicalTransaction{id='" + transactionId + "', source=" + sourceSystem 
             + ", amount=" + amount + ", occurredAt=" + occurredAt + "}";
    }
}
