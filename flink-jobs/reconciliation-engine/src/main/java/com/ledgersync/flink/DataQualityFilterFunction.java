package com.ledgersync.flink;

import com.ledgersync.domain.CanonicalTransaction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataQualityFilterFunction - In-flight data quality filter for CanonicalTransaction stream.
 * 
 * Implements the following validation rules:
 * 1. transaction_id cannot be null or empty
 * 2. amount must be greater than 0 (prevents negative/zero amount attacks)
 * 3. occurred_at must be reasonable (not in 1970, not too far in future)
 * 
 * Records that fail validation are logged to stderr (for DLQ integration) and dropped.
 * In production, these would be sent to a Kafka Dead Letter Queue topic for later analysis.
 */
public class DataQualityFilterFunction extends RichFlatMapFunction<CanonicalTransaction, CanonicalTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(DataQualityFilterFunction.class);
    
    // Tolerance for clock skew: 1 day in milliseconds
    private static final long CLOCK_SKEW_TOLERANCE_MS = 86400000L;

    @Override
    public void flatMap(CanonicalTransaction tx, Collector<CanonicalTransaction> out) throws Exception {
        // Regla 1: transaction_id no puede ser nulo o vacío
        if (tx.getTransactionId() == null || tx.getTransactionId().trim().isEmpty()) {
            emitToDlq(tx, "NULL_OR_EMPTY_TRANSACTION_ID");
            return;
        }

        // Regla 2: El monto debe ser mayor a 0
        if (tx.getAmount() == null || tx.getAmount().getValue() <= 0) {
            emitToDlq(tx, "INVALID_OR_NEGATIVE_AMOUNT");
            return;
        }

        // Regla 3: occurred_at debe ser razonable (no en el futuro lejano ni en 1970)
        long now = System.currentTimeMillis();
        if (tx.getOccurredAt() <= 0 || tx.getOccurredAt() > now + CLOCK_SKEW_TOLERANCE_MS) {
            emitToDlq(tx, "INVALID_TIMESTAMP");
            return;
        }

        // Si pasa todas las reglas, sigue el flujo normal
        out.collect(tx);
    }

    private void emitToDlq(CanonicalTransaction tx, String reason) {
        String txId = tx.getTransactionId() != null ? tx.getTransactionId() : "UNKNOWN";
        String message = String.format("DATA_QUALITY_VIOLATION: %s | TX_ID: %s | SOURCE: %s", 
                                       reason, txId, tx.getSourceSystem());
        LOG.error(message);
        System.err.println(message);
        
        // In production, send to Kafka DLQ topic:
        // dlqOutput.collect(enrichDlqRecord(tx, reason));
    }
}
