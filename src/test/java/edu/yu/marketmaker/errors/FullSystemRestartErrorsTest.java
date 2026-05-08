package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.databind.JsonNode;
import edu.yu.marketmaker.model.Fill;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end error-case test for the "Full System Restart" workflow, driven
 * against the live compose stack. Mirrors the scenario in
 * {@code docs/error-cases.md}:
 *
 * <ul>
 *   <li><b>Case 11</b>: Every service is stopped and restarted. PostgreSQL
 *       remains the durable source of truth — Hazelcast MapStores load
 *       positions, fills, reservations, and quotes back into memory on
 *       startup. After convergence, the system continues serving traffic
 *       without manual intervention.</li>
 * </ul>
 *
 * Concretely we:
 * <ol>
 *   <li>Boot the stack and drive enough traffic so trading-state has fills
 *       on every seed symbol.</li>
 *   <li>Snapshot the fill set and AAPL net position.</li>
 *   <li>Stop everything except postgres, then bring everything back.</li>
 *   <li>Verify trading-state's {@code /state/fills} contains the pre-restart
 *       fill ids (durable storage), AAPL's net position is preserved, and
 *       the cluster reconverges. Then drive traffic again and confirm fresh
 *       fills are recorded — the system is fully operational post-restart.</li>
 * </ol>
 *
 * Opt-in: {@code -Derrors.it=true}, docker required.
 */
@EnabledIfSystemProperty(named = "errors.it", matches = "true")
class FullSystemRestartErrorsTest {

    private static final String TAG = "ErrorsIT-Restart";

    @BeforeAll
    static void bootStack() throws Exception {
        ErrorsITSupport.bootStack(TAG);
        ErrorsITSupport.driveTrafficUntilEverySymbolHasFill(Duration.ofMinutes(3));
    }

    @AfterAll
    static void teardownStack() throws Exception {
        ErrorsITSupport.teardownStack(TAG);
    }

    /**
     * Capture pre-restart state, stop every service except postgres, restart
     * everything, and verify the trading-state fills and AAPL net position
     * survived. Postgres is the durable backing store; volumes are not
     * removed in this test, so MapStore-backed Hazelcast caches reload
     * exactly what was persisted.
     */
    @Test
    void fullRestartReconstructsFillsAndPositionsFromPostgres() throws Exception {
        // 1. Pre-restart snapshot.
        List<Fill> beforeFills = ErrorsITSupport.getAllFills();
        assertTrue(beforeFills.size() >= ErrorsITSupport.SEED_SYMBOLS.size(),
                "baseline must produce at least one fill per seed symbol; got " + beforeFills.size());
        Set<UUID> beforeFillIds = new HashSet<>();
        for (Fill f : beforeFills) beforeFillIds.add(f.getId());

        long aaplBefore = ErrorsITSupport.getNetPositionOrZero("AAPL");
        assertTrue(aaplBefore != 0,
                "AAPL must have a non-zero net position before the restart; got " + aaplBefore);

        // 2. Stop everything except postgres. We avoid `down -v` so the
        //    postgres volume is preserved.
        System.out.println("[" + TAG + "] stopping every service except postgres...");
        List<String> servicesToStop = new ArrayList<>();
        servicesToStop.add("external-publisher");
        servicesToStop.add("exchange");
        servicesToStop.add("exposure-reservation");
        servicesToStop.addAll(ErrorsITSupport.MM_PORT_TO_SERVICE.values());
        servicesToStop.add("trading-state");
        servicesToStop.add("zookeeper1");
        servicesToStop.add("zookeeper2");
        servicesToStop.add("zookeeper3");
        for (String s : servicesToStop) {
            ErrorsITSupport.stop(s);
        }

        // 3. Bring core infra back up. Order matters: zk + postgres first,
        //    then trading-state + exposure-reservation (their MapStores load
        //    on startup), then exchange, then publisher, then market-makers.
        System.out.println("[" + TAG + "] restarting core services...");
        ErrorsITSupport.runDocker(java.util.Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres", "trading-state", "exposure-reservation");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(4));
        ErrorsITSupport.awaitHealthy("exposure-reservation",
                ErrorsITSupport.EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        ErrorsITSupport.start("exchange");
        ErrorsITSupport.awaitHealthy("exchange",
                ErrorsITSupport.EXCHANGE_PORT, Duration.ofMinutes(4));
        ErrorsITSupport.start("external-publisher");
        ErrorsITSupport.awaitHealthy("external-publisher",
                ErrorsITSupport.PUBLISHER_PORT, Duration.ofMinutes(4));
        for (String mm : ErrorsITSupport.MM_PORT_TO_SERVICE.values()) {
            ErrorsITSupport.start(mm);
        }
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(5),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not converge after full restart");

        // 4. Durability check: every pre-restart fill id must reappear in the
        //    post-restart /state/fills response. This proves the MapStore
        //    reloaded the fills from postgres on trading-state startup.
        List<Fill> afterFills = ErrorsITSupport.getAllFills();
        Set<UUID> afterFillIds = new HashSet<>();
        for (Fill f : afterFills) afterFillIds.add(f.getId());

        Set<UUID> missing = new HashSet<>(beforeFillIds);
        missing.removeAll(afterFillIds);
        assertEquals(Set.of(), missing,
                "fills present before restart must reappear from durable storage; missing=" + missing);

        long aaplAfter = ErrorsITSupport.getNetPositionOrZero("AAPL");
        assertEquals(aaplBefore, aaplAfter,
                "AAPL net position must be preserved across full restart");

        // 5. Operational check: the system must accept new orders again and
        //    record fresh fills — the doc's "system converges to correct
        //    state without manual intervention" outcome.
        ErrorsITSupport.seedQuotes(new ArrayList<>(ErrorsITSupport.SEED_SYMBOLS));
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3), () -> {
            try {
                ErrorsITSupport.submitOrders(List.of("AAPL"), 25);
                return ErrorsITSupport.getAllFills().size() > afterFills.size();
            } catch (Exception e) {
                return false;
            }
        }, "no new fills recorded after full restart — system did not resume");
    }

    /**
     * After the full restart, the exposure-reservation service must report
     * a coherent {@code /exposure} (recovered from postgres) with bid/ask
     * usage at most {@code totalCapacity}. This is the doc's "rebuild
     * global exposure totals" guarantee.
     */
    @Test
    void exposureServiceRebuildsCoherentTotalsAfterRestart() throws Exception {
        // The previous test may have already restarted the stack; either way,
        // exposure must be reachable and bounded.
        JsonNode exposure = ErrorsITSupport.getExposureOrNull();
        assertNotNull(exposure, "exposure must be reachable post-restart");
        int total = exposure.path("totalCapacity").asInt();
        int bid = exposure.path("bidUsage").asInt();
        int ask = exposure.path("askUsage").asInt();
        assertTrue(total > 0, "totalCapacity must be > 0: " + exposure);
        assertTrue(bid >= 0 && bid <= total,
                "bidUsage out of [0, totalCapacity]: " + exposure);
        assertTrue(ask >= 0 && ask <= total,
                "askUsage out of [0, totalCapacity]: " + exposure);
    }
}
