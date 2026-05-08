package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end error-case tests for the "Exposure Lifecycle" workflows, driven
 * against the live compose stack. Mirrors the scenarios in
 * {@code docs/error-cases.md}:
 *
 * <ul>
 *   <li><b>Case 9</b>: A fill arrives but the apply-fill call fails because
 *       the exposure-reservation service is unreachable. The reservation
 *       continues to hold its original capacity (conservative
 *       over-reservation), and the global limits are never exceeded.</li>
 *   <li><b>Case 10</b>: A market-maker crashes mid quote-replacement cycle.
 *       The bounded inconsistency window is observable in two ways: (a) the
 *       cluster still reconverges so other workers can publish fresh quotes,
 *       and (b) global exposure stays under {@code totalCapacity} throughout.</li>
 * </ul>
 *
 * Opt-in: {@code -Derrors.it=true}, docker required.
 */
@EnabledIfSystemProperty(named = "errors.it", matches = "true")
class ExposureLifecycleErrorsTest {

    private static final String TAG = "ErrorsIT-Exposure";

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
        ErrorsITSupport.start("exposure-reservation");
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(2));
        for (String mm : ErrorsITSupport.MM_PORT_TO_SERVICE.values()) {
            ErrorsITSupport.start(mm);
        }
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not reconverge after restoring services");
    }

    // --- Error Case 9: Fill arrives but apply-fill fails or times out ---

    /**
     * Stop the exposure-reservation service. Then drive order traffic, which
     * produces fills and would normally trigger apply-fill calls from each
     * MM. With the reservation service unreachable, those apply-fill calls
     * cannot land — the doc's "conservative over-reservation" outcome.
     * <p>
     * The check we can perform from outside: while the service is down, any
     * exposure read fails (returns null), and once it's back, exposure must
     * never report bid/ask usage above {@code totalCapacity}. That's the
     * "no exposure limit violation" guarantee from the doc.
     */
    @Test
    void applyFillFailureKeepsExposureBelowTotalCapacity() throws Exception {
        JsonNode before = ErrorsITSupport.getExposureOrNull();
        assertNotNull(before, "exposure must be reachable before fault injection");
        int totalCapacity = before.path("totalCapacity").asInt();
        assertTrue(totalCapacity > 0, "totalCapacity must be > 0: " + before);

        ErrorsITSupport.stop("exposure-reservation");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.EXPOSURE_RES_PORT),
                "exposure-reservation did not become unhealthy after stop");

        // Drive order traffic that *would* produce fills and apply-fill calls.
        // Each apply-fill must fail (service down). The exchange itself is
        // up, so the publisher's POST /orders may still 200 (orders that
        // dispatch reservations indirectly will fail at the reservation hop).
        for (int i = 0; i < 4; i++) {
            ErrorsITSupport.submitOrders(new ArrayList<>(ErrorsITSupport.SEED_SYMBOLS), 5);
            assertTrue(ErrorsITSupport.getExposureOrNull() == null,
                    "exposure must be unreachable while exposure-reservation is down");
            Thread.sleep(1000);
        }

        ErrorsITSupport.start("exposure-reservation");
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(2));

        // After restart, the service rebuilds totals from durable storage;
        // it must never report usage above totalCapacity.
        JsonNode after = ErrorsITSupport.getExposureOrNull();
        assertNotNull(after, "exposure must be reachable after restart");
        int bid = after.path("bidUsage").asInt();
        int ask = after.path("askUsage").asInt();
        assertTrue(bid <= totalCapacity,
                "bidUsage " + bid + " > totalCapacity " + totalCapacity + " after recovery");
        assertTrue(ask <= totalCapacity,
                "askUsage " + ask + " > totalCapacity " + totalCapacity + " after recovery");
    }

    // --- Error Case 10: Market maker crashes during quote replacement cycle ---

    /**
     * Kill a non-leader market-maker while it's actively processing fills
     * (i.e. the moments most likely to coincide with a release-old/grant-new
     * quote-replacement cycle). The doc's bounded inconsistency window is
     * "less than the quote TTL". Externally, what we can verify is:
     * <ul>
     *   <li>The cluster reconverges to {@code N-1} members (no deadlock).</li>
     *   <li>Global exposure stays under {@code totalCapacity} throughout
     *       and immediately afterwards.</li>
     *   <li>Once the dead node is restarted, the system returns to 7 members
     *       and exposure is still bounded (no leaked-and-never-released
     *       reservation pushes us over).</li>
     * </ul>
     */
    @Test
    void marketMakerCrashMidReplacementKeepsExposureBoundedAndClusterConverged() throws Exception {
        JsonNode before = ErrorsITSupport.getExposureOrNull();
        assertNotNull(before, "exposure must be reachable before fault injection");
        int totalCapacity = before.path("totalCapacity").asInt();

        int leaderPort = ErrorsITSupport.leaderPort();
        int victimPort = ErrorsITSupport.MM_PORT_TO_SERVICE.keySet().stream()
                .filter(p -> p != leaderPort)
                .findFirst().orElseThrow();
        String victimService = ErrorsITSupport.MM_PORT_TO_SERVICE.get(victimPort);

        // Drive traffic; mid-flight, kill the victim. Any in-progress release
        // followed by a missing grant is the case-10 inconsistency.
        for (int wave = 0; wave < 5; wave++) {
            ErrorsITSupport.submitOrders(List.of("AAPL", "GOOG", "MSFT"), 5);
            if (wave == 2) ErrorsITSupport.kill(victimService);
            JsonNode now = ErrorsITSupport.getExposureOrNull();
            assertNotNull(now, "exposure became unreachable mid-test");
            int bid = now.path("bidUsage").asInt();
            int ask = now.path("askUsage").asInt();
            assertTrue(bid <= totalCapacity,
                    "bidUsage " + bid + " exceeded totalCapacity during case-10 fault");
            assertTrue(ask <= totalCapacity,
                    "askUsage " + ask + " exceeded totalCapacity during case-10 fault");
            Thread.sleep(1000);
        }

        // Survivors reconverge.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2),
                () -> ErrorsITSupport.mmSurvivorsConverged(victimPort,
                        ErrorsITSupport.MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after victim crash");

        // The window is bounded by quote TTL (~30s). Wait through it and
        // re-verify exposure is still bounded.
        Thread.sleep(35_000);
        JsonNode afterTtl = ErrorsITSupport.getExposureOrNull();
        assertNotNull(afterTtl, "exposure unreachable after TTL window");
        assertTrue(afterTtl.path("bidUsage").asInt() <= totalCapacity,
                "bidUsage exceeded totalCapacity after TTL window: " + afterTtl);
        assertTrue(afterTtl.path("askUsage").asInt() <= totalCapacity,
                "askUsage exceeded totalCapacity after TTL window: " + afterTtl);

        // Restart and converge to 7.
        ErrorsITSupport.start(victimService);
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not return to 7-member convergence after victim restart");
    }
}
