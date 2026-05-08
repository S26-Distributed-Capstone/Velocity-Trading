package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end error-case test for the "Streaming Position Data Updates"
 * workflow, driven against the live compose stack. Mirrors the scenario in
 * {@code docs/error-cases.md}:
 *
 * <ul>
 *   <li><b>Case 8</b>: A connected trading-state subscriber (the leader
 *       market-maker, which subscribes to {@code state.stream} and forwards
 *       per-symbol snapshots to workers) sees its connection break when
 *       trading-state goes down. After trading-state restarts and rebuilds
 *       positions from PostgreSQL via Hazelcast MapStore, the leader
 *       reconnects and forwarding resumes.</li>
 * </ul>
 *
 * Observability hook: the leader MM exposes a {@code forwardsBySymbol} count
 * via {@code GET /marketmaker/status}. While trading-state is down the
 * counter cannot grow (no upstream snapshots); after recovery and new fills,
 * it must grow again. This is the same hook the cluster-failover test uses
 * to prove forwarding is live.
 *
 * Opt-in: {@code -Derrors.it=true}, docker required.
 */
@EnabledIfSystemProperty(named = "errors.it", matches = "true")
class StreamingPositionErrorsTest {

    private static final String TAG = "ErrorsIT-Stream";

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
        ErrorsITSupport.start("trading-state");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not reconverge after restoring trading-state");
    }

    /**
     * Stop trading-state. The leader MM's {@code forwardsBySymbol} counts
     * must stop growing for at least 5 seconds — proves the upstream
     * subscription has actually broken rather than silently buffering.
     */
    @Test
    void leaderForwardsFreezeWhileTradingStateIsDown() throws Exception {
        int leaderPort = ErrorsITSupport.leaderPort();
        assertTrue(leaderPort > 0, "no leader before the test");

        Map<String, Long> baseline = readForwardCounts(leaderPort);
        assertNotNull(baseline, "leader status must be reachable before the test");

        ErrorsITSupport.stop("trading-state");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.TRADING_STATE_PORT),
                "trading-state did not become unhealthy after stop");

        // After the disconnect, sample the leader's counts a few times. They
        // may finish flushing one or two in-flight frames immediately after
        // the kill, but should plateau within a few seconds.
        Map<String, Long> stable = null;
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Long> a = readForwardCounts(leaderPort);
            Thread.sleep(2000);
            Map<String, Long> b = readForwardCounts(leaderPort);
            if (a != null && b != null && a.equals(b)) {
                stable = b;
                break;
            }
        }
        assertNotNull(stable, "leader's forwardsBySymbol counts never plateaued while trading-state was down");

        // And remain stable for another few seconds — proves no upstream
        // frames are arriving.
        Thread.sleep(3000);
        Map<String, Long> later = readForwardCounts(leaderPort);
        assertNotNull(later, "leader status became unreachable mid-test");
        assertTrue(stable.equals(later),
                "leader forwarded snapshots while trading-state was down: before="
                        + stable + " after=" + later);
    }

    /**
     * Once trading-state is back and we drive new fills, the leader's
     * subscription must re-attach and the per-symbol forward counter must
     * grow again. This is the doc's "fresh full snapshot on reconnect"
     * guarantee, observed via the side effect of forwarding.
     */
    @Test
    void leaderResumesForwardingAfterTradingStateRestart() throws Exception {
        int leaderPort = ErrorsITSupport.leaderPort();
        assertTrue(leaderPort > 0, "no leader before the test");

        ErrorsITSupport.stop("trading-state");
        ErrorsITSupport.awaitCondition(Duration.ofSeconds(30),
                () -> !ErrorsITSupport.healthy(ErrorsITSupport.TRADING_STATE_PORT),
                "trading-state did not become unhealthy after stop");

        // Snapshot mid-outage counts so we can compare against post-recovery.
        Map<String, Long> midOutage = readForwardCounts(leaderPort);
        assertNotNull(midOutage, "leader status unreachable mid-outage");

        ErrorsITSupport.start("trading-state");
        ErrorsITSupport.awaitHealthy("trading-state",
                ErrorsITSupport.TRADING_STATE_PORT, Duration.ofMinutes(2));
        // The cluster needs a moment for the leader's subscription to
        // re-attach against the new trading-state container.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(3),
                ErrorsITSupport::allMmNodesConverged,
                "cluster did not return to convergence after trading-state restart");

        // Drive fresh fills and wait for at least one count to grow above
        // its mid-outage value, on any symbol — that proves the leader's
        // state.stream subscription is live again.
        ErrorsITSupport.awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                ErrorsITSupport.submitOrders(List.of("AAPL", "GOOG", "MSFT"), 10);
            } catch (Exception ignored) {
                // tolerate transient failures while services settle
            }
            Map<String, Long> now = readForwardCounts(leaderPort);
            if (now == null) return false;
            for (Map.Entry<String, Long> e : now.entrySet()) {
                long was = midOutage.getOrDefault(e.getKey(), 0L);
                if (e.getValue() > was) return true;
            }
            return false;
        }, "leader forwardsBySymbol counts did not grow after trading-state recovery");
    }

    private static Map<String, Long> readForwardCounts(int port) {
        JsonNode status = ErrorsITSupport.mmStatusOrNull(port);
        if (status == null) return null;
        JsonNode map = status.path("forwardsBySymbol");
        if (!map.isObject()) return Map.of();
        Map<String, Long> out = new LinkedHashMap<>();
        map.properties().forEach(e -> out.put(e.getKey(), e.getValue().asLong()));
        return out;
    }
}
