package com.ledgersync.flink;

import com.ledgersync.domain.CanonicalTransaction;
import com.ledgersync.domain.Money;
import com.ledgersync.domain.ReconciliationResult;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * StatefulReconciliationFunctionTest - Unit tests for stateful reconciliation logic.
 * 
 * Uses Flink's test utilities to verify:
 * - Successful reconciliation when both ledger and gateway transactions arrive
 * - Timeout handling when counterpart is missing
 * - State management and timer behavior without a full Flink cluster
 */
public class StatefulReconciliationFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, CanonicalTransaction, ReconciliationResult> testHarness;

    @BeforeEach
    public void setup() throws Exception {
        StatefulReconciliationFunction function = new StatefulReconciliationFunction();
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function),
            tx -> tx.getTransactionId(),
            Types.STRING
        );
        testHarness.open();
    }

    @Test
    public void testReconciledMatch() throws Exception {
        // 1. Llega transacción del Ledger
        CanonicalTransaction ledgerTx = createTx("tx-001", "INTERNAL_LEDGER", 1000L, "USD", 5000L);
        testHarness.processElement(ledgerTx, 1000L); // Event time: 1000ms

        // 2. Llega transacción del Gateway (mismo ID, mismo monto) dentro de la ventana de 30s
        CanonicalTransaction gatewayTx = createTx("tx-001", "PAYMENT_GATEWAY", 2000L, "USD", 5000L);
        testHarness.processElement(gatewayTx, 2000L); // Event time: 2000ms

        // 3. Avanzar tiempo de procesamiento para disparar la evaluación
        testHarness.setProcessingTime(3000L);

        // 4. Verificar que se emitió exactamente 1 resultado RECONCILED
        assertEquals(1, testHarness.getOutput().size());
        ReconciliationResult result = testHarness.getOutput().poll().getValue();
        assertEquals("RECONCILED", result.getStatus().name());
        assertEquals("NONE", result.getExceptionType().name());
    }

    @Test
    public void testMissingCounterpartTimeout() throws Exception {
        // 1. Llega solo una transacción
        CanonicalTransaction ledgerTx = createTx("tx-002", "INTERNAL_LEDGER", 1000L, "USD", 5000L);
        testHarness.processElement(ledgerTx, 1000L);

        // 2. Avanzar el tiempo de evento MÁS ALLÁ de la ventana de 30s (30000ms + 1000ms = 31000ms)
        // Esto debe disparar el onTimer()
        testHarness.setProcessingTime(32000L);
        testHarness.processWatermark(32000L);

        // 3. Verificar que se emitió la excepción MISSING_COUNTERPART
        assertEquals(1, testHarness.getOutput().size());
        ReconciliationResult result = testHarness.getOutput().poll().getValue();
        assertEquals("EXCEPTION", result.getStatus().name());
        assertEquals("MISSING_COUNTERPART", result.getExceptionType().name());
    }

    @Test
    public void testAmountMismatch() throws Exception {
        // 1. Llega transacción del Ledger con monto 5000
        CanonicalTransaction ledgerTx = createTx("tx-003", "INTERNAL_LEDGER", 1000L, "USD", 5000L);
        testHarness.processElement(ledgerTx, 1000L);

        // 2. Llega transacción del Gateway con monto diferente (6000)
        CanonicalTransaction gatewayTx = createTx("tx-003", "PAYMENT_GATEWAY", 2000L, "USD", 6000L);
        testHarness.processElement(gatewayTx, 2000L);

        // 3. Verificar que se emitió EXCEPTION con AMOUNT_MISMATCH
        assertEquals(1, testHarness.getOutput().size());
        ReconciliationResult result = testHarness.getOutput().poll().getValue();
        assertEquals("EXCEPTION", result.getStatus().name());
        assertEquals("AMOUNT_MISMATCH", result.getExceptionType().name());
    }

    private CanonicalTransaction createTx(String id, String source, long occurredAt, String currency, long amount) {
        CanonicalTransaction tx = new CanonicalTransaction();
        tx.setTransactionId(id);
        tx.setSourceSystem(CanonicalTransaction.SourceSystem.valueOf(source));
        tx.setOccurredAt(occurredAt);
        
        Money money = new Money();
        money.setCurrency(currency);
        money.setValue(amount);
        tx.setAmount(money);
        
        return tx;
    }
}
