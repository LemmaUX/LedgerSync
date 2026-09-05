package com.ledgersync.domain;

/**
 * ReconciliationResult - Output of the reconciliation engine.
 */
public class ReconciliationResult {
    
    public enum Status {
        RECONCILED,
        EXCEPTION
    }
    
    public enum ExceptionType {
        NONE,
        MISSING_COUNTERPART,
        AMOUNT_MISMATCH,
        DUPLICATE
    }

    private String transactionId;
    private Status status;
    private ExceptionType exceptionType;
    private String ledgerTransactionId;
    private String gatewayTransactionId;
    private Long reconciledAt;
    private String details;

    public ReconciliationResult() {}

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ExceptionType getExceptionType() {
        return exceptionType;
    }

    public void setExceptionType(ExceptionType exceptionType) {
        this.exceptionType = exceptionType;
    }

    public String getLedgerTransactionId() {
        return ledgerTransactionId;
    }

    public void setLedgerTransactionId(String ledgerTransactionId) {
        this.ledgerTransactionId = ledgerTransactionId;
    }

    public String getGatewayTransactionId() {
        return gatewayTransactionId;
    }

    public void setGatewayTransactionId(String gatewayTransactionId) {
        this.gatewayTransactionId = gatewayTransactionId;
    }

    public Long getReconciledAt() {
        return reconciledAt;
    }

    public void setReconciledAt(Long reconciledAt) {
        this.reconciledAt = reconciledAt;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "ReconciliationResult{txId='" + transactionId + "', status=" + status 
             + ", exceptionType=" + exceptionType + "}";
    }
}
