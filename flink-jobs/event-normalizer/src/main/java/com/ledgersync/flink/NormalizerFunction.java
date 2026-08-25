package com.ledgersync.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NormalizerFunction - Transforms source-specific CDC events to canonical transaction schema.
 * 
 * This function performs the following mappings for Internal Ledger events:
 * - uuid → transaction_id
 * - "INTERNAL_LEDGER" → source_system
 * - amount_cents + currency_code → amount (Money record)
 * - status → status (enum mapping)
 * - type → transaction_type (enum mapping)
 * - created_at → occurred_at
 * - ingested_at = current timestamp
 * 
 * Architectural Justification:
 * - Separation of concerns: Normalization logic is isolated from I/O concerns
 * - Testability: Pure function can be unit tested without Flink runtime
 * - Extensibility: Easy to add new source system normalizers
 */
public class NormalizerFunction implements FlatMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizerFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private static final String SOURCE_SYSTEM = "INTERNAL_LEDGER";

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        try {
            // Parse incoming CDC event (JSON format from Debezium)
            JsonNode event = MAPPER.readTree(value);
            
            // Extract Debezium envelope fields
            JsonNode payload = event.has("payload") ? event.get("payload") : event;
            JsonNode before = payload.has("before") ? payload.get("before") : payload;
            JsonNode after = payload.has("after") ? payload.get("after") : payload;
            
            // Use 'after' for INSERT/UPDATE events, skip DELETE events
            if (after == null || after.isMissingNode()) {
                LOG.debug("Skipping DELETE event: {}", value);
                return;
            }
            
            // Build canonical transaction
            ObjectNode canonical = MAPPER.createObjectNode();
            
            // Map uuid → transaction_id
            String transactionId = extractField(after, "uuid");
            if (transactionId == null || transactionId.isEmpty()) {
                LOG.warn("Missing transaction ID, skipping event: {}", value);
                return;
            }
            canonical.put("transaction_id", transactionId);
            
            // Set source_system
            canonical.put("source_system", SOURCE_SYSTEM);
            
            // Map external_reference (null for internal ledger)
            canonical.putNull("external_reference");
            
            // Build Money object from amount_cents + currency_code
            ObjectNode amount = MAPPER.createObjectNode();
            amount.put("value", extractLongField(after, "amount_cents", 0L));
            amount.put("currency", extractField(after, "currency_code", "USD"));
            canonical.set("amount", amount);
            
            // Map status with enum normalization
            String rawStatus = extractField(after, "status", "PENDING");
            canonical.put("status", normalizeStatus(rawStatus));
            
            // Map transaction_type with enum normalization
            String rawType = extractField(after, "type", "PAYMENT");
            canonical.put("transaction_type", normalizeTransactionType(rawType));
            
            // Map timestamps
            long occurredAt = extractLongField(after, "created_at", System.currentTimeMillis());
            canonical.put("occurred_at", occurredAt);
            canonical.put("ingested_at", System.currentTimeMillis());
            
            // Initialize metadata as empty object (can be extended later)
            ObjectNode metadata = MAPPER.createObjectNode();
            // Add source-specific metadata
            metadata.put("source_table", "transactions");
            metadata.put("cdc_operation", getOperation(payload));
            canonical.set("metadata", metadata);
            
            // Output normalized event as JSON string
            out.collect(MAPPER.writeValueAsString(canonical));
            
            LOG.debug("Normalized transaction: {}", transactionId);
            
        } catch (Exception e) {
            LOG.error("Failed to normalize event: {}", value, e);
            // In production, send to dead-letter queue instead of dropping
            // For now, we skip malformed events to prevent pipeline failure
        }
    }
    
    /**
     * Extract string field from JSON node with optional default.
     */
    private String extractField(JsonNode node, String fieldName) {
        return extractField(node, fieldName, null);
    }
    
    private String extractField(JsonNode node, String fieldName, String defaultValue) {
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            return defaultValue;
        }
        return field.asText(defaultValue);
    }
    
    /**
     * Extract long field from JSON node with optional default.
     */
    private long extractLongField(JsonNode node, String fieldName, long defaultValue) {
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            return defaultValue;
        }
        return field.asLong(defaultValue);
    }
    
    /**
     * Normalize status from source-specific values to canonical enum.
     * 
     * Canonical statuses: PENDING, COMPLETED, FAILED, REFUNDED, CHARGED_BACK
     */
    private String normalizeStatus(String rawStatus) {
        if (rawStatus == null) {
            return "PENDING";
        }
        
        switch (rawStatus.toUpperCase()) {
            case "COMPLETED":
            case "SUCCESS":
            case "SETTLED":
                return "COMPLETED";
            case "FAILED":
            case "ERROR":
            case "REJECTED":
                return "FAILED";
            case "REFUNDED":
            case "RETURNED":
                return "REFUNDED";
            case "CHARGED_BACK":
            case "DISPUTED":
                return "CHARGED_BACK";
            case "PENDING":
            case "PROCESSING":
            case "IN_PROGRESS":
            default:
                return "PENDING";
        }
    }
    
    /**
     * Normalize transaction type from source-specific values to canonical enum.
     * 
     * Canonical types: PAYMENT, REFUND, CHARGEBACK, FEE, TRANSFER
     */
    private String normalizeTransactionType(String rawType) {
        if (rawType == null) {
            return "PAYMENT";
        }
        
        switch (rawType.toUpperCase()) {
            case "REFUND":
            case "RETURN":
                return "REFUND";
            case "CHARGEBACK":
            case "DISPUTE":
                return "CHARGEBACK";
            case "FEE":
            case "CHARGE":
                return "FEE";
            case "TRANSFER":
            case "WIRE":
                return "TRANSFER";
            case "PAYMENT":
            case "PURCHASE":
            case "SALE":
            default:
                return "PAYMENT";
        }
    }
    
    /**
     * Extract CDC operation type from Debezium envelope.
     */
    private String getOperation(JsonNode payload) {
        if (payload.has("op")) {
            return payload.get("op").asText("UNKNOWN");
        }
        return "UNKNOWN";
    }
}
