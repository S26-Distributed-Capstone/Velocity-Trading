package edu.yu.marketmaker.errorcases.local;

import edu.yu.marketmaker.model.Fill;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HA failover tests for the "Submitting External Orders" workflow, driven
 * against the live docker-compose stack.
 *
 * <p>{@code docs/error-cases.md} cases 1-3 were written before the system
 * gained its 3-replica backup design. With backups in place the scenario the
 * team wants verified is <em>failover</em>: one replica is killed and the
 * survivors keep the system running. Each test kills a single replica
 * ({@code exchange-1} / {@code trading-state-1}) — never a whole tier — and
 * asserts the order path stays alive:
 *
 * <ul>
 *   <li><b>Case 1</b> (exchange goes down): kill one exchange replica
 *       mid-traffic; the {@code service-lb} routes around it and orders keep
 *       filling. The killed replica rejoins cleanly on restart.</li>
 *   <li><b>Case 2</b> (publisher retries): two waves of orders with fresh
 *       UUIDs produce independent fills — no dedup — unchanged by HA.</li>
 *   <li><b>Case 3</b> (trading-state goes down): kill one trading-state
 *       replica; leadership/Hazelcast failover keeps fills being recorded.
 *       The killed replica rejoins cleanly on restart.</li>
 * </ul>
 *
 * Requires a working docker daemon — the {@code @BeforeAll} boot fails
 * loudly if docker is unreachable.
 */
class SubmittingExternalOrderErrorsLocalTest {

    private static final String TAG = "ErrorsIT-Submit";
    private static final Duration FAILOVER_BUDGET = Duration.ofMinutes(2);

    @BeforeAll
    static void bootStack() throws Exception {
        ErrorsITSupport.bootStack(TAG);
        ErrorsITSupport.driveTrafficUntilEverySymbolHasFill(Duration.ofMinutes(3));
    }

    @AfterAll
    static void teardownStack() throws Exception {
        ErrorsITSupport.teardownStack(TAG);
    }

    @AfterEach
    void restoreKilledReplicas() throws Exception {
        // Each test kills exchange-1 and/or trading-state-1; bring both back
        // so the next test starts from a full 3-replica stack. `up -d` is a
        // no-op for a replica that is already running.
        ErrorsITSupport.start("exchange-1");
        ErrorsITSupport.start("trading-state-1");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));
    }

    // --- Error Case 1: an exchange replica goes down ---

    /**
     * Kill one exchange replica while orders are flowing. The other two
     * replicas plus the {@code service-lb} must keep accepting orders, so new
     * fills continue to land in trading-state. This is the documented "order
     * is handled" outcome — failover, not loss.
     */
    @Test
    void exchangeReplicaFailoverKeepsOrdersFilling() throws Exception {
        int fillsBefore = ErrorsITSupport.fillCountOrMinusOne();
        assertTrue(fillsBefore >= 0, "trading-state must be reachable before the test");

        ErrorsITSupport.kill("exchange-1");

        ErrorsITSupport.awaitNewFillsAfter(fillsBefore, FAILOVER_BUDGET);
    }

    /**
     * A killed exchange replica rejoins the stack on restart: its container
     * runs again and the exchange tier stays healthy through the LB.
     */
    @Test
    void killedExchangeReplicaRejoinsCleanly() throws Exception {
        ErrorsITSupport.kill("exchange-1");
        ErrorsITSupport.start("exchange-1");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));

        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> {
                    try {
                        return ErrorsITSupport.isRunning("exchange-1");
                    } catch (Exception e) {
                        return false;
                    }
                }, "exchange-1 container did not return to running state after restart");
    }

    // --- Error Case 2: publisher retries with fresh ids (no duplicate fill) ---

    /**
     * Two waves of orders carry fresh UUIDs (every order the publisher
     * generates gets a new id). They must produce independent fills — the
     * exchange does not dedup — so a publisher's blind retry after a timeout
     * is correctly accounted for as a separate trade.
     */
    @Test
    void retriedOrdersWithFreshIdsProduceIndependentFills() throws Exception {
        ErrorsITSupport.reseedQuietly();

        Set<UUID> existingFillIds = new HashSet<>();
        for (Fill f : ErrorsITSupport.getAllFills()) existingFillIds.add(f.getId());

        int wave1 = ErrorsITSupport.submitOrders(List.of("AAPL"), 5);
        int wave2 = ErrorsITSupport.submitOrders(List.of("AAPL"), 5);
        assertTrue(wave1 > 0 && wave2 > 0, "both waves must have accepted orders");

        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30), () -> {
            try {
                long newFills = ErrorsITSupport.getAllFills().stream()
                        .filter(f -> !existingFillIds.contains(f.getId()))
                        .count();
                return newFills >= 2;
            } catch (Exception e) {
                return false;
            }
        }, "expected at least two new AAPL fills across the two waves");

        Set<UUID> newOrderIds = new HashSet<>();
        for (Fill f : ErrorsITSupport.getAllFills()) {
            if (!existingFillIds.contains(f.getId())) {
                assertEquals("AAPL", f.symbol(), "test only submitted AAPL: " + f);
                assertNotNull(f.orderId(), "every fill must carry an orderId: " + f);
                newOrderIds.add(f.orderId());
            }
        }
        assertTrue(newOrderIds.size() >= 2,
                "two waves with fresh order ids must produce >= 2 distinct orderIds in fills: "
                        + newOrderIds);
    }

    // --- Error Case 3: a trading-state replica goes down ---

    /**
     * Kill one trading-state replica while orders are flowing. Hazelcast data
     * replication plus leader failover (the exchange resolves the trading-state
     * leader via Zookeeper on every RSocket send) must keep fills being
     * recorded — the documented "fill is recorded, position is correct"
     * outcome, achieved by the backup rather than lost.
     */
    @Test
    void tradingStateReplicaFailoverKeepsFillsRecorded() throws Exception {
        int fillsBefore = ErrorsITSupport.fillCountOrMinusOne();
        assertTrue(fillsBefore >= 0, "trading-state must be reachable before the test");

        ErrorsITSupport.kill("trading-state-1");

        ErrorsITSupport.awaitNewFillsAfter(fillsBefore, FAILOVER_BUDGET);
    }

    /**
     * A killed trading-state replica rejoins the stack on restart: its
     * container runs again and the trading-state tier stays healthy.
     */
    @Test
    void killedTradingStateReplicaRejoinsCleanly() throws Exception {
        ErrorsITSupport.kill("trading-state-1");
        ErrorsITSupport.start("trading-state-1");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));

        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> {
                    try {
                        return ErrorsITSupport.isRunning("trading-state-1");
                    } catch (Exception e) {
                        return false;
                    }
                }, "trading-state-1 container did not return to running state after restart");
    }

    // --- Sanity: a single replica loss does not take the tier down ---

    /**
     * Killing one exchange replica must not make the exchange tier
     * unreachable: {@code GET /quotes/{symbol}} through the load balancer
     * keeps returning a quote, served by a surviving replica.
     */
    @Test
    void killingExchangeReplicaKeepsTheTierServingQuotes() throws Exception {
        ErrorsITSupport.reseedQuietly();
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> ErrorsITSupport.currentExchangeQuoteId("AAPL") != null,
                "AAPL quote did not appear in exchange after seed");

        ErrorsITSupport.kill("exchange-1");

        ErrorsITSupport.awaitCondition(Duration.ofSeconds(45),
                () -> ErrorsITSupport.currentExchangeQuoteId("AAPL") != null,
                "exchange tier stopped serving /quotes/AAPL after one replica was killed");
    }
}
