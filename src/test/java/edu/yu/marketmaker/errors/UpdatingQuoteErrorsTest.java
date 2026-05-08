package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *   <li><b>Case 6</b>: Reservation service goes down — exchange-side quotes
 *       can no longer be refreshed, and the existing quoteId stops rotating
 *       until the service comes back.</li>
 *   <li><b>Case 7</b>: Exchange goes down — fresh MM-generated quotes can't
 *       be persisted, but once the exchange returns, market-makers republish
 *       on their next refresh cycle.</li>
 * </ul>
 *
 * Opt-in: {@code -Derrors.it=true}, docker required. Boot/teardown mirror
 * {@code ClusterIntegrationWithSystemTest}.
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
        // sees a converged 7-node cluster and healthy core services.
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
                "cluster did not reconverge to 7 nodes after restoring services");
    }

    // --- Error Case 4: Market maker goes down before handling the position update ---

    /**
     * Kill a non-leader market-maker node. The remaining 6 nodes must
     * reconverge to a single agreed leader and the killed node must be
     * evicted from the live members set. This is the documented case-4
     * outcome from the cluster's perspective: a missed position update on
     * one node does not block the others.
     */
    @Test
    void killedMarketMakerIsEvictedAndSurvivorsReconverge() throws Exception {
        int leaderPort = ErrorsITSupport.leaderPort();
        assertTrue(leaderPort > 0, "no leader before the test");
        int victimPort = ErrorsITSupport.MM_PORT_TO_SERVICE.keySet().stream()
                .filter(p -> p != leaderPort)
                .findFirst().orElseThrow();
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);

        ErrorsITSupport.kill(victimService);

        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> ErrorsITSupport.mmSurvivorsConverged(victimPort,
                        ErrorsITSupport.MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);
    }

    /**
     * Restart the killed node and verify the cluster returns to 7 healthy
     * members on a single leader. This proves the case-4 recovery path:
     * once the node restarts it reconnects to {@code state.stream}, receives
     * a fresh snapshot, and rejoins the cluster.
     */
    @Test
    void restartedMarketMakerRejoinsAndClusterReturnsToSevenMembers() throws Exception {
        int leaderPort = ErrorsITSupport.leaderPort();
        int victimPort = ErrorsITSupport.MM_PORT_TO_SERVICE.keySet().stream()
                .filter(p -> p != leaderPort)
                .findFirst().orElseThrow();
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);

        ErrorsITSupport.kill(victimService);
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> ErrorsITSupport.mmSurvivorsConverged(victimPort,
                        ErrorsITSupport.MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);

        ErrorsITSupport.start(victimService);
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not return to 7-member convergence after restart");

        // Sanity: the restarted node has a non-empty assigned set or shows up
        // in another node's view as a member; both are guaranteed by
        // allMmNodesConverged. We additionally probe its /marketmaker/status
        // endpoint to make sure the application layer is up.
        JsonNode status = ErrorsITSupport.mmStatusOrNull(victimPort);
        assertNotNull(status,
                "restarted MM at port " + victimPort + " did not respond to /marketmaker/status");
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
        // Probe before fault injection.
        JsonNode before = ErrorsITSupport.getExposureOrNull();
        assertNotNull(before, "exposure must be reachable before fault injection");
        int totalCapacity = before.path("totalCapacity").asInt();
        assertTrue(totalCapacity > 0, "totalCapacity must be > 0: " + before);

        // Kill a non-leader node mid-flight while traffic is producing fills
        // (and thus driving the reserve/release cycle on every quote refresh).
        int leaderPort = ErrorsITSupport.leaderPort();
        int victimPort = ErrorsITSupport.MM_PORT_TO_SERVICE.keySet().stream()
                .filter(p -> p != leaderPort)
                .findFirst().orElseThrow();
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);

        for (int wave = 0; wave < 3; wave++) {
            ErrorsITSupport.submitOrders(List.of("AAPL", "GOOG", "MSFT"), 5);
            if (wave == 1) ErrorsITSupport.kill(victimService);
            JsonNode now = ErrorsITSupport.getExposureOrNull();
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
     * With exposure-reservation stopped, market-makers can't get fresh
     * reservations and therefore can't publish new quotes. The exchange's
     * existing quoteId for a symbol must stop rotating until the reservation
     * service is restored.
     */
    @Test
    void reservationServiceDownFreezesQuoteIdRotationUntilRestart() throws Exception {
        // Snapshot a stable pre-fault quoteId for AAPL.
        UUID before = ErrorsITSupport.currentExchangeQuoteId("AAPL");
        assertNotNull(before, "AAPL must have an active quote before the test");

        ErrorsITSupport.stop("exposure-reservation");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXPOSURE_RES_PORT),
                "exposure-reservation did not become unhealthy after stop");

        // Drive enough position updates for any working MM to *try* to refresh.
        // Without a reservation service, none of those refreshes can publish
        // a fresh quote, so the AAPL quoteId must not change for the duration
        // of the outage. We sample a few times to catch in-flight rotations.
        Set<UUID> idsSeenDuringOutage = new HashSet<>();
        idsSeenDuringOutage.add(before);
        for (int i = 0; i < 5; i++) {
            ErrorsITSupport.submitOrders(List.of("AAPL"), 5);
            UUID now = ErrorsITSupport.currentExchangeQuoteId("AAPL");
            if (now != null) idsSeenDuringOutage.add(now);
            Thread.sleep(1000);
        }
        // We don't assert exactly one id, because a refresh that started
        // before the kill may still have landed with the pre-existing
        // reservation. We *do* assert there was no proliferation of fresh
        // quote ids — at most one transition.
        assertTrue(idsSeenDuringOutage.size() <= 2,
                "without exposure-reservation, MMs must not generate a stream of fresh quote ids: "
                        + idsSeenDuringOutage);

        ErrorsITSupport.start("exposure-reservation");
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(2));

        // After restart MMs must be able to publish fresh quotes again.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                ErrorsITSupport.submitOrders(List.of("AAPL"), 5);
            } catch (Exception ignored) {
                // tolerate transient failures while services settle
            }
            UUID after = ErrorsITSupport.currentExchangeQuoteId("AAPL");
            return after != null && !idsSeenDuringOutage.contains(after);
        }, "AAPL quoteId did not rotate after exposure-reservation came back");
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
}
