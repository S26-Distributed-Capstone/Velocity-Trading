package edu.yu.marketmaker.errorcases.cluster;

import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end error-case tests for the "Submitting External Orders" workflow,
 * driven against the live compose stack rather than mocks. Mirrors the
 * scenarios in {@code docs/error-cases.md}:
 *
 * <ul>
 *   <li><b>Case 1</b>: Exchange goes down before handling the order — the
 *       publisher's POST fails, no fill reaches trading-state.</li>
 *   <li><b>Case 2</b>: Publisher retries with a fresh order id — both the
 *       original and the retry produce independent fills (no dedup).</li>
 *   <li><b>Case 3</b>: Trading-state goes down before handling the fill — the
 *       exchange's RSocket send is fire-and-forget, so the fill is silently
 *       lost and absent from {@code /state/fills} after restart, even though
 *       the exchange-side quote quantity was decremented.</li>
 * </ul>
 *
 * Opt-in: requires {@code -Derrorcases.cluster.it=true} and a working docker
 * daemon. Mirrors the structure of {@code ClusterIntegrationWithSystemTest}.
 */
@EnabledIfSystemProperty(named = "errorcases.cluster.it", matches = "true")
class SubmittingExternalOrderErrorsClusterTest {

    private static final String TAG = "ErrorsIT-Submit";

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
    void restoreCoreServices() throws Exception {
        // Each crash test kills exchange or trading-state; restore both so the
        // next test starts from a healthy stack. Idempotent: if already up,
        // `compose up -d` is a no-op.
        ErrorsITSupport.start("trading-state");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));
        ErrorsITSupport.start("exchange");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));
    }

    // --- Error Case 1: Exchange goes down before handling the order ---

    /**
     * Stop the exchange container, then submit orders via the publisher. With
     * the exchange offline, every {@code POST /orders} call from the publisher
     * must fail at the network layer — the publisher reports zero accepted
     * orders, no fills appear in trading-state, and no positions move. This is
     * the documented "order is lost without state corruption" outcome.
     */
    @Test
    void exchangeDownBeforeHandlingOrderProducesNoFillsAndNoStateCorruption() throws Exception {
        long fillsBefore = ErrorsITSupport.getAllFills().size();
        long aaplBefore = ErrorsITSupport.getNetPositionOrZero("AAPL");

        ErrorsITSupport.stop("exchange");
        // Wait for /health to fail so the publisher actually sees a dead exchange.
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXCHANGE_PORT),
                "exchange did not become unhealthy after stop");

        int accepted = ErrorsITSupport.submitOrders(List.of("AAPL"), 5);
        assertEquals(0, accepted,
                "with exchange down, publisher must report zero accepted orders");

        long fillsAfter = ErrorsITSupport.getAllFills().size();
        assertEquals(fillsBefore, fillsAfter,
                "no fill must reach trading-state while the exchange is down");
        assertEquals(aaplBefore, ErrorsITSupport.getNetPositionOrZero("AAPL"),
                "AAPL net position must not move while exchange is down");
    }

    /**
     * After the exchange is restarted, submitting orders must succeed again
     * and produce fresh fills. This proves the case-1 outcome doesn't poison
     * future processing — a transient exchange outage is fully recoverable.
     */
    @Test
    void exchangeCrashAndRestartIsFullyRecoverable() throws Exception {
        ErrorsITSupport.stop("exchange");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXCHANGE_PORT),
                "exchange did not become unhealthy after stop");
        ErrorsITSupport.start("exchange");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));

        int fillsBefore = ErrorsITSupport.getAllFills().size();
        int accepted = ErrorsITSupport.submitOrders(List.of("AAPL"), 25);
        assertTrue(accepted > 0, "exchange must accept orders again after restart");

        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> {
                    try {
                        return ErrorsITSupport.getAllFills().size() > fillsBefore;
                    } catch (Exception e) {
                        return false;
                    }
                }, "no new fills recorded after exchange restart");
    }

    // --- Error Case 2: Publisher retries with a new id (no duplicate fill) ---

    /**
     * Two distinct order ids targeting the same symbol must produce two
     * distinct fills. The exchange treats every id as a fresh order — there
     * is no dedup, so a publisher's blind retry after a case-2 timeout is
     * accounted for as a separate trade.
     */
    @Test
    void retriedOrdersWithFreshIdsProduceIndependentFills() throws Exception {
        Set<UUID> existingFillIds = new HashSet<>();
        for (Fill f : ErrorsITSupport.getAllFills()) existingFillIds.add(f.getId());

        // Submitting more orders here is fine — every order the publisher
        // generates already gets a fresh UUID, so two waves model the
        // "original + retry" pair from case 2 without coupling to wall time.
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
        // sanity: no two new orderIds collide
        assertEquals(newOrderIds.size(), newOrderIds.stream().distinct().count(),
                "orderIds across retries must be distinct");
    }

    // --- Error Case 3: Trading state goes down before handling the fill ---

    /**
     * RSocket {@code state.fills} is fire-and-forget. With trading-state
     * stopped, the exchange still accepts orders (its quote map is in shared
     * Hazelcast and the dispatch decrements the local quantity), but the fill
     * never reaches trading-state. Once trading-state restarts, those fills
     * are absent from {@code /state/fills} — the documented critical
     * inconsistency that the doc calls out as a known gap.
     *
     * Strong form: we also assert the exchange-side quote was decremented
     * while trading-state was down, when no market-maker republished mid-test.
     * If a market-maker did republish, the decrement is no longer observable;
     * the fill-loss assertion is the unconditional check.
     */
    @Test
    void tradingStateDownLosesFillsSilentlyAndIsAbsentAfterRestart() throws Exception {
        // Use a fresh, single-symbol bootstrap so we have plenty of qty to fill
        // without contending with previous-test load.
        ErrorsITSupport.seedQuotes(List.of("AAPL"));
        Quote quoteBefore = ErrorsITSupport.currentExchangeQuote("AAPL");
        assertNotNull(quoteBefore, "AAPL must have a current quote before the test");

        ErrorsITSupport.stop("trading-state");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.TRADING_STATE_PORT),
                "trading-state did not become unhealthy after stop");

        // Submit some BUY orders directly to the exchange while trading-state
        // is dead. BUYs cross the ask side; each accepted order decrements
        // askQuantity on the exchange's quote (the documented "quote was
        // already decremented" claim). Some calls may 200 (RSocket
        // fire-and-forget swallows the failure downstream) and some may 500;
        // either way, capture the order ids that the exchange acknowledged
        // with a 200 so we can prove they are never reflected in trading-state's
        // fills after restart.
        int perOrderQty = 1;
        int orderCount = 5;
        Set<UUID> blackHoleOrderIds = new HashSet<>();
        for (int i = 0; i < orderCount; i++) {
            UUID orderId = UUID.randomUUID();
            int rc = ErrorsITSupport.submitOrderDirectlyWithId(
                    "AAPL", orderId, perOrderQty, 101.0, "BUY");
            if (rc == 200) blackHoleOrderIds.add(orderId);
        }

        // Snapshot exchange quote while trading-state is still down. If the
        // quoteId is unchanged, the askQuantity must have decremented by at
        // least the cumulative quantity of accepted orders — proving the
        // exchange wrote a decrement even though the fill was lost downstream.
        Quote quoteDuringOutage = ErrorsITSupport.currentExchangeQuote("AAPL");
        if (quoteDuringOutage != null
                && quoteDuringOutage.quoteId().equals(quoteBefore.quoteId())) {
            int expectedAtMost = quoteBefore.askQuantity() - blackHoleOrderIds.size() * perOrderQty;
            assertTrue(quoteDuringOutage.askQuantity() <= expectedAtMost,
                    "exchange must have decremented askQuantity by at least "
                            + (blackHoleOrderIds.size() * perOrderQty)
                            + " (from " + quoteBefore.askQuantity()
                            + " to " + quoteDuringOutage.askQuantity()
                            + "); documented: quote was decremented even though fill was lost");
        }

        ErrorsITSupport.start("trading-state");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));

        // After restart, give the system a moment to settle, then assert that
        // the fills we created during the outage are absent — this is the
        // documented "fill is lost" outcome.
        Thread.sleep(3000);
        Set<UUID> orderIdsAfter = new HashSet<>();
        for (Fill f : ErrorsITSupport.getAllFills()) {
            if (f.orderId() != null) orderIdsAfter.add(f.orderId());
        }
        for (UUID lost : blackHoleOrderIds) {
            assertFalse(orderIdsAfter.contains(lost),
                    "fire-and-forget fill must not reappear after trading-state restart: " + lost);
        }
    }

    /**
     * Even though fills sent during the outage are lost, the exchange's local
     * {@code /quotes/{symbol}} state does not get into a worse shape than
     * before: a quote remains queryable for AAPL with either the same id
     * (no MM ran during the brief window) or a fresher id from a market-maker
     * that successfully republished after trading-state came back. The point
     * is: the symbol is not stuck in a broken state.
     */
    @Test
    void tradingStateRestartDoesNotLeaveExchangeQuoteUnreachable() throws Exception {
        // Pre-seed and confirm AAPL has a current quote before stopping
        // trading-state — removes cross-test-state flake risk.
        ErrorsITSupport.seedQuotes(List.of("AAPL"));
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(15),
                () -> ErrorsITSupport.currentExchangeQuoteId("AAPL") != null,
                "AAPL quote did not appear in exchange after seed");

        ErrorsITSupport.stop("trading-state");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.TRADING_STATE_PORT),
                "trading-state did not become unhealthy after stop");

        // Quote should still be readable from exchange — exchange does not
        // depend on trading-state to serve GET /quotes.
        UUID midOutageQuoteId = ErrorsITSupport.currentExchangeQuoteId("AAPL");
        assertNotNull(midOutageQuoteId,
                "exchange must keep serving /quotes/AAPL while trading-state is down");

        ErrorsITSupport.start("trading-state");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));

        // After recovery the symbol is still queryable (possibly with a fresh
        // MM-generated quoteId). The pre-existing inconsistency window is
        // documented; the recovery itself is functional.
        UUID afterRecovery = ErrorsITSupport.currentExchangeQuoteId("AAPL");
        assertNotNull(afterRecovery, "exchange must serve /quotes/AAPL after recovery");
    }

    /**
     * Sanity: while the exchange is down, every direct order submission must
     * fail at the network layer (helper reports -1), the publisher counts
     * zero accepted orders, and unrelated services (trading-state's fills,
     * exposure /exposure) remain reachable. Keeps the other tests honest by
     * showing the failure mode is contained to the exchange's blast radius.
     */
    @Test
    void exchangeOutageIsContainedAndOtherServicesStayReachable() throws Exception {
        ErrorsITSupport.stop("exchange");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXCHANGE_PORT),
                "exchange did not become unhealthy after stop");
        int rc = ErrorsITSupport.submitOrderDirectly("AAPL", 1, 101.0, "BUY");
        assertEquals(-1, rc, "with exchange down, helper must report -1 (network failure)");

        int accepted = ErrorsITSupport.submitOrders(List.of("AAPL"), 1);
        assertEquals(0, accepted,
                "publisher must not count orders as accepted while exchange is down");
        assertNotNull(ErrorsITSupport.getAllFills(),
                "trading-state /state/fills must remain reachable while only exchange is down");
        assertNotNull(ErrorsITSupport.getExposureOrNull(),
                "exposure-reservation /exposure must remain reachable while only exchange is down");
        assertNull(ErrorsITSupport.currentExchangeQuoteId("AAPL"),
                "currentExchangeQuoteId must return null while exchange is down");
    }
}
