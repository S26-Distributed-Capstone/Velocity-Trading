package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end error-case tests for the "Updating Quote" workflow, driven
 * against the live compose stack. Mirrors the scenarios in
 * {@code docs/error-cases.md}:
 *
 * <ul>
 *   <li><b>Case 4</b>: A market-maker node goes down before handling a
 *       position update — the cluster reassigns its symbols to surviving
 *       workers, the node restarts, the cluster reconverges to 7 members.</li>
 *   <li><b>Case 5</b>: After reservation but before publishing the quote, the
 *       MM crashes — the leaked reservation is bounded by the quote TTL,
 *       and the global exposure number stays under {@code totalCapacity}.</li>
 *   <li><b>Case 6</b>: Reservation service crashes before processing a
 *       reservation request — the market maker must not publish the newly
 *       generated quote, so the old exchange-side quote remains active.</li>
 *   <li><b>Case 7</b>: Exchange goes down — fresh MM-generated quotes can't
 *       be persisted, but once the exchange returns, market-makers republish
 *       on their next refresh cycle.</li>
 * </ul>
 *
 * Opt-in: {@code -Derrors.it=true}. Runtime can be selected with
 * {@code -Derrors.it.runtime=docker|k8s} (default: docker). In k8s mode it
 * targets the NodePorts used by {@code ClusterIntegrationWithSystemK8sTest}.
 */
@EnabledIfSystemProperty(named = "errors.it", matches = "true")
class UpdatingQuoteErrorsTest {

    private static final String TAG = "ErrorsIT-UpdateQuote";

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
    void restoreServices() throws Exception {
        // Restore every service the case-* tests may have stopped, so each test
        // sees a converged MM cluster and healthy core services.
        ErrorsITSupport.start("exposure-reservation");
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(2));
        ErrorsITSupport.start("exchange");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));
        for (String mm : ErrorsITSupport.MM_PORT_TO_SERVICE.values()) {
            ErrorsITSupport.start(mm);
        }
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not reconverge to " + ErrorsITSupport.MM_PORT_TO_SERVICE.size()
                        + " nodes after restoring services");
    }

    // --- Error Case 4: Market maker goes down before handling the position update ---

    /**
     * Kill one market-maker node. The remaining 6 nodes must
     * reconverge to a single agreed leader, and the killed node must be
     * evicted from the live members set. This is the documented case-4
     * outcome from the cluster's perspective: a missed position update on
     * one node does not block the others.
     */
    @Test
    void killedMarketMakerIsEvictedAndSurvivorsReconverge() throws Exception {
        assertTrue(ErrorsITSupport.MM_PORT_TO_SERVICE.size() >= 2,
                "this test requires at least 2 MM nodes; got "
                        + ErrorsITSupport.MM_PORT_TO_SERVICE.size());
        int leaderPort = ErrorsITSupport.leaderPort();
        assertTrue(leaderPort > 0, "no leader before the test");
        int victimPort = ErrorsITSupport.crashVictimPort(leaderPort);
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);
        String observedSymbol = firstAssignedSymbol(victimPort);
        UUID preCrashQuoteId = awaitQuoteId(observedSymbol, Duration.ofSeconds(30));
        assertNotNull(preCrashQuoteId,
                "expected an active quote before crash for observed symbol " + observedSymbol);

        ErrorsITSupport.kill(victimService);

        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> ErrorsITSupport.mmSurvivorsConverged(victimPort,
                        ErrorsITSupport.MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);

        // Case 4 outcome: the killed node misses updates, but the symbol remains
        // quotable in the cluster while survivors continue serving traffic.
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> ErrorsITSupport.currentExchangeQuoteId(observedSymbol) != null,
                "observed symbol became unquotable after node crash: " + observedSymbol);
    }

    /**
     * Restart the killed node and verify the cluster returns to 7 healthy
     * members on a single leader. This proves the case-4 recovery path:
     * once the node restarts it reconnects to {@code state.stream}, receives
     * a fresh snapshot, and rejoins the cluster.
     */
    @Test
    void restartedMarketMakerRejoinsAndClusterReturnsToSevenMembers() throws Exception {
        assertTrue(ErrorsITSupport.MM_PORT_TO_SERVICE.size() >= 2,
                "this test requires at least 2 MM nodes; got "
                        + ErrorsITSupport.MM_PORT_TO_SERVICE.size());
        int leaderPort = ErrorsITSupport.leaderPort();
        int victimPort = ErrorsITSupport.crashVictimPort(leaderPort);
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);
        String observedSymbol = firstAssignedSymbol(victimPort);
        UUID preCrashQuoteId = awaitQuoteId(observedSymbol, Duration.ofSeconds(30));
        assertNotNull(preCrashQuoteId,
                "expected an active quote before crash for observed symbol " + observedSymbol);

        ErrorsITSupport.kill(victimService);
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> ErrorsITSupport.mmSurvivorsConverged(victimPort,
                        ErrorsITSupport.MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);

        ErrorsITSupport.start(victimService);
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not return to " + ErrorsITSupport.MM_PORT_TO_SERVICE.size()
                        + "-member convergence after restart");

        // Sanity: the restarted node has a non-empty assigned set or shows up
        // in another node's view as a member; both are guaranteed by
        // allMmNodesConverged. We additionally probe its /marketmaker/status
        // endpoint to make sure the application layer is up.
        JsonNode status = ErrorsITSupport.mmStatusOrNull(victimPort);
        assertNotNull(status,
                "restarted MM at port " + victimPort + " did not respond to /marketmaker/status");

        // Case 4 recovery: after restart and fresh traffic, observed symbol
        // should eventually rotate away from the pre-crash quote id.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                ErrorsITSupport.submitOrders(List.of(observedSymbol), 5);
            } catch (Exception ignored) {
                // tolerate transient failures while services settle
            }
            UUID after = ErrorsITSupport.currentExchangeQuoteId(observedSymbol);
            return after != null && !after.equals(preCrashQuoteId);
        }, "observed symbol quote did not refresh after node restart: " + observedSymbol);
    }

    // --- Error Case 5: Crash after reservation but before publishing the quote ---

    /**
     * The doc's case-5 outcome is a bounded exposure leak. We exercise the
     * coarsest end-to-end check that's deterministically observable: even
     * when an MM is killed mid-cycle (which may have left a leaked
     * reservation), the exposure-reservation service never reports
     * {@code bidUsage} or {@code askUsage} above {@code totalCapacity}.
     * Combined with the quote TTL bound documented in the spec, this is the
     * "no over-commit" property the doc cares about.
     */
    @Test
    void killedMarketMakerNeverPushesExposureOverTotalCapacity() throws Exception {
        assertTrue(ErrorsITSupport.MM_PORT_TO_SERVICE.size() >= 2,
                "this test requires at least 2 MM nodes; got "
                        + ErrorsITSupport.MM_PORT_TO_SERVICE.size());
        // Probe before fault injection.
        JsonNode before = awaitExposure(Duration.ofSeconds(30));
        assertNotNull(before, "exposure must be reachable before fault injection");
        int totalCapacity = before.path("totalCapacity").asInt();
        assertTrue(totalCapacity > 0, "totalCapacity must be > 0: " + before);

        // Kill one node mid-flight while traffic is producing fills
        // (and thus driving the reserve/release cycle on every quote refresh).
        int leaderPort = ErrorsITSupport.leaderPort();
        int victimPort = ErrorsITSupport.crashVictimPort(leaderPort);
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);

        for (int wave = 0; wave < 3; wave++) {
            ErrorsITSupport.submitOrders(List.of("AAPL", "GOOG", "MSFT"), 5);
            if (wave == 1) ErrorsITSupport.kill(victimService);
            JsonNode now = awaitExposure(Duration.ofSeconds(30));
            assertNotNull(now, "exposure became unreachable mid-test");
            int bid = now.path("bidUsage").asInt();
            int ask = now.path("askUsage").asInt();
            assertTrue(bid <= totalCapacity,
                    "bidUsage " + bid + " exceeded totalCapacity " + totalCapacity);
            assertTrue(ask <= totalCapacity,
                    "askUsage " + ask + " exceeded totalCapacity " + totalCapacity);
            Thread.sleep(2000);
        }
    }

    // --- Error Case 6: Reservation service goes down ---

    /**
     * Mirrors docs/error-cases.md case 6 exactly:
     * trading-state emits a position update, the market maker generates a new
     * quote, but exposure-reservation has crashed before it can process the
     * reservation request. Since no reservation is granted, the market maker
     * must not publish the new quote and the old exchange quote remains.
     */
    @Test
    void reservationServiceCrashBeforeProcessingPreventsQuoteUpdateUntilRestart() throws Exception {
        String symbol = "AAPL";
        UUID beforeCrash = awaitQuoteId(symbol, Duration.ofSeconds(30));
        assertNotNull(beforeCrash, symbol + " must have an active quote before the test");

        // The reservation service crashes before the position update that will
        // make the MM generate a replacement quote and request a reservation.
        ErrorsITSupport.kill("exposure-reservation");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXPOSURE_RES_PORT),
                "exposure-reservation did not become unhealthy after crash");
        UUID oldQuote = ErrorsITSupport.currentExchangeQuoteId(symbol);
        assertNotNull(oldQuote,
                "old quote must remain visible after exposure-reservation crashes");

        assertTrue(ErrorsITSupport.submitSyntheticFill(symbol),
                "trading-state did not accept the position update for " + symbol);

        // Give the owning MM time to receive the position update, generate the
        // replacement quote, and fail its reservation request. The exchange
        // quoteId must remain exactly the old quoteId: no reservation was
        // granted, so no new quote may become active.
        for (int i = 0; i < 8; i++) {
            Thread.sleep(1000);
            assertEquals(oldQuote, ErrorsITSupport.currentExchangeQuoteId(symbol),
                    "market maker published a quote without a granted reservation");
        }

        ErrorsITSupport.start("exposure-reservation");
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(2));

        // Case 6 recovery: once reservation is available again, the MM retries
        // on the next position update / refresh cycle and can publish a fresh
        // reserved quote.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2), () -> {
            ErrorsITSupport.submitSyntheticFill(symbol);
            UUID after = ErrorsITSupport.currentExchangeQuoteId(symbol);
            return after != null && !after.equals(oldQuote);
        }, symbol + " quoteId did not rotate after exposure-reservation came back");
    }

    // --- Error Case 7: Exchange goes down before the new quote is updated ---

    /**
     * With the exchange down, market-makers can't PUT fresh quotes. Once it
     * comes back, the next position update / refresh cycle must republish
     * fresh quotes — this is the documented "MM republishes on next cycle"
     * outcome.
     */
    @Test
    void exchangeDownFreezesQuoteUpdatesAndResumesAfterRestart() throws Exception {
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(1), () -> {
            for (String s : ErrorsITSupport.SEED_SYMBOLS) {
                if (ErrorsITSupport.currentExchangeQuoteId(s) == null) return false;
            }
            return true;
        }, "not every seed symbol had an active quote before exchange outage");

        Map<String, UUID> before = new HashMap<>();
        for (String s : ErrorsITSupport.SEED_SYMBOLS) {
            UUID id = ErrorsITSupport.currentExchangeQuoteId(s);
            if (id != null) before.put(s, id);
        }
        assertEquals(ErrorsITSupport.SEED_SYMBOLS.size(), before.size(),
                "every seed symbol must have a current quote before the test");

        ErrorsITSupport.stop("exchange");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXCHANGE_PORT),
                "exchange did not become unhealthy after stop");

        // While the exchange is down, currentExchangeQuoteId returns null —
        // and that's the only observable state from outside. We don't fail
        // the test on this; it just confirms the outage.
        for (String s : ErrorsITSupport.SEED_SYMBOLS) {
            assertNull(ErrorsITSupport.currentExchangeQuoteId(s),
                    "exchange must not serve quotes while down: " + s);
        }

        ErrorsITSupport.start("exchange");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(2));

        // After recovery, the next refresh cycle must republish quotes.
        // Allow up to 2 minutes for at least one symbol's quoteId to rotate
        // away from the pre-outage value.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                ErrorsITSupport.submitOrders(List.of("AAPL", "GOOG", "MSFT"), 5);
            } catch (Exception ignored) {
                // tolerate transient failures while services settle
            }
            for (Map.Entry<String, UUID> e : before.entrySet()) {
                UUID after = ErrorsITSupport.currentExchangeQuoteId(e.getKey());
                if (after != null && !after.equals(e.getValue())) return true;
            }
            return false;
        }, "no quoteId rotation observed after exchange restart");
    }

    private static String firstAssignedSymbol(int mmPort) {
        JsonNode status = ErrorsITSupport.mmStatusOrNull(mmPort);
        if (status == null) {
            return "AAPL";
        }
        JsonNode assigned = status.path("assigned");
        if (!assigned.isArray() || assigned.isEmpty()) {
            return "AAPL";
        }
        return assigned.get(0).asText("AAPL");
    }

    private static UUID awaitQuoteId(String symbol, Duration timeout) throws Exception {
        final UUID[] out = new UUID[1];
        ErrorsITSupport.awaitCondition(timeout, () -> {
            out[0] = ErrorsITSupport.currentExchangeQuoteId(symbol);
            return out[0] != null;
        }, "quote did not appear for symbol " + symbol + " within " + timeout);
        return out[0];
    }

    private static JsonNode awaitExposure(Duration timeout) throws Exception {
        final JsonNode[] out = new JsonNode[1];
        ErrorsITSupport.awaitCondition(timeout, () -> {
            out[0] = ErrorsITSupport.getExposureOrNull();
            return out[0] != null;
        }, "exposure did not become reachable within " + timeout);
        return out[0];
    }
}
