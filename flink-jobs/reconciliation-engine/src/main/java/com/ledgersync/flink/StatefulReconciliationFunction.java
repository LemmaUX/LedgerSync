package com.ledgersync.flink;

import com.ledgersync.domain.CanonicalTransaction;
import com.ledgersync.domain.Money;
import com.ledgersync.domain.ReconciliationResult;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * StatefulReconciliationFunction - Performs stateful temporal reconciliation between
 * ledger and gateway transactions using KeyedProcessFunction with timers.
 * 
 * Architecture:
 * - Uses keyed state to store pending transactions waiting for their counterpart
 * - Registers processing time timers for timeout detection (30s window)
 * - Emits RECONCILED when both sides match, EXCEPTION on timeout or mismatch
 */
public class StatefulReconciliationFunction extends KeyedProcessFunction<String, CanonicalTransaction, ReconciliationResult> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulReconciliationFunction.class);
    
    private static final long RECONCILIATION_WINDOW_MS = 30000L; // 30 seconds
    
    private transient ListState<CanonicalTransaction> pendingTransactionsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        ListStateDescriptor<CanonicalTransaction> descriptor = new ListStateDescriptor<>(
            "pending-transactions",
            Types.POJO(CanonicalTransaction.class)
        );
        pendingTransactionsState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(
            CanonicalTransaction tx,
            Context ctx,
            Collector<ReconciliationResult> out) throws Exception {
        
        LOG.debug("Processing transaction: {}", tx.getTransactionId());
        
        // Retrieve pending transactions for this key
        List<CanonicalTransaction> pending = new ArrayList<>();
        for (CanonicalTransaction p : pendingTransactionsState.get()) {
            pending.add(p);
        }
        
        // Check if we have a counterpart to reconcile with
        for (CanonicalTransaction counterpart : pending) {
            if (!counterpart.getSourceSystem().equals(tx.getSourceSystem())) {
                // Found counterpart from different source system - attempt reconciliation
                if (amountsMatch(tx.getAmount(), counterpart.getAmount())) {
                    // Successful reconciliation
                    ReconciliationResult result = createReconciledResult(tx, counterpart);
                    out.collect(result);
                    
                    // Clear state and timer
                    pendingTransactionsState.clear();
                    ctx.timerService().deleteProcessingTimeTimer(getTimerKey(tx.getTransactionId()));
                    LOG.info("Reconciled transaction: {}", tx.getTransactionId());
                    return;
                } else {
                    // Amount mismatch
                    ReconciliationResult result = createMismatchResult(tx, counterpart);
                    out.collect(result);
                    
                    pendingTransactionsState.clear();
                    ctx.timerService().deleteProcessingTimeTimer(getTimerKey(tx.getTransactionId()));
                    LOG.warn("Amount mismatch for transaction: {}", tx.getTransactionId());
                    return;
                }
            }
        }
        
        // No counterpart found yet - store in state and register timer
        pending.add(tx);
        pendingTransactionsState.update(pending);
        
        // Register timer if this is the first transaction for this key
        if (pending.size() == 1) {
            long timerTimestamp = ctx.timerService().currentProcessingTime() + RECONCILIATION_WINDOW_MS;
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            LOG.debug("Registered timer for transaction: {} at {}", tx.getTransactionId(), timerTimestamp);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<ReconciliationResult> out) throws Exception {
        
        LOG.debug("Timer fired for key: {} at {}", ctx.getKey(), timestamp);
        
        // Retrieve pending transactions
        List<CanonicalTransaction> pending = new ArrayList<>();
        for (CanonicalTransaction p : pendingTransactionsState.get()) {
            pending.add(p);
        }
        
        if (!pending.isEmpty()) {
            // Timeout - no counterpart arrived within window
            CanonicalTransaction tx = pending.get(0);
            ReconciliationResult result = createMissingCounterpartResult(tx);
            out.collect(result);
            
            // Clear state
            pendingTransactionsState.clear();
            LOG.warn("Missing counterpart for transaction: {}", tx.getTransactionId());
        }
    }
    
    private boolean amountsMatch(Money m1, Money m2) {
        if (m1 == null || m2 == null) {
            return false;
        }
        return m1.getValue().equals(m2.getValue()) && 
               m1.getCurrency().equals(m2.getCurrency());
    }
    
    private ReconciliationResult createReconciledResult(CanonicalTransaction tx1, CanonicalTransaction tx2) {
        ReconciliationResult result = new ReconciliationResult();
        result.setTransactionId(tx1.getTransactionId());
        result.setStatus(ReconciliationResult.Status.RECONCILED);
        result.setExceptionType(ReconciliationResult.ExceptionType.NONE);
        result.setLedgerTransactionId(
            tx1.getSourceSystem() == CanonicalTransaction.SourceSystem.INTERNAL_LEDGER ? 
            tx1.getTransactionId() : tx2.getTransactionId()
        );
        result.setGatewayTransactionId(
            tx1.getSourceSystem() == CanonicalTransaction.SourceSystem.PAYMENT_GATEWAY ? 
            tx1.getTransactionId() : tx2.getTransactionId()
        );
        result.setReconciledAt(System.currentTimeMillis());
        return result;
    }
    
    private ReconciliationResult createMismatchResult(CanonicalTransaction tx1, CanonicalTransaction tx2) {
        ReconciliationResult result = new ReconciliationResult();
        result.setTransactionId(tx1.getTransactionId());
        result.setStatus(ReconciliationResult.Status.EXCEPTION);
        result.setExceptionType(ReconciliationResult.ExceptionType.AMOUNT_MISMATCH);
        result.setDetails("Amount mismatch: " + tx1.getAmount() + " vs " + tx2.getAmount());
        result.setReconciledAt(System.currentTimeMillis());
        return result;
    }
    
    private ReconciliationResult createMissingCounterpartResult(CanonicalTransaction tx) {
        ReconciliationResult result = new ReconciliationResult();
        result.setTransactionId(tx.getTransactionId());
        result.setStatus(ReconciliationResult.Status.EXCEPTION);
        result.setExceptionType(ReconciliationResult.ExceptionType.MISSING_COUNTERPART);
        result.setDetails("No counterpart received within " + (RECONCILIATION_WINDOW_MS / 1000) + "s window");
        result.setReconciledAt(System.currentTimeMillis());
        return result;
    }
    
    private long getTimerKey(String transactionId) {
        // Use transaction ID hash as timer key (simplified - in production use proper keying)
        return transactionId.hashCode();
    }
}
