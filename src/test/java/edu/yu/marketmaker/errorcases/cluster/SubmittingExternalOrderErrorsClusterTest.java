package edu.yu.marketmaker.errorcases.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * k3s/Kubernetes variant of
 * {@code edu.yu.marketmaker.errorcases.local.SubmittingExternalOrderErrorsLocalTest}.
 *
 * <p>Same HA failover scenarios for "Submitting External Orders" (error cases
 * 1-3 in {@code docs/error-cases.md}), but run against a live k3s cluster
 * rather than a docker-compose stack. Crash injection is
 * {@code kubectl delete pod <pod>}: the StatefulSet recreates the pod while
 * the surviving replicas keep the system running.
 *
 * <ul>
 *   <li><b>Case 1</b> (exchange goes down): delete one exchange pod
 *       mid-traffic; the {@code exchange} Service routes to the survivors and
 *       orders keep filling. The deleted pod is recreated and rejoins.</li>
 *   <li><b>Case 2</b> (publisher retries): two waves of fresh-UUID orders
 *       produce independent fills — no dedup.</li>
 *   <li><b>Case 3</b> (trading-state goes down): delete one trading-state pod
 *       mid-traffic; Hazelcast replication + leader failover keep fills being
 *       recorded. The deleted pod is recreated and rejoins.</li>
 * </ul>
 *
 * <p>Pre-conditions (run before invoking this test):
 * <ol>
 *   <li>Build the offline image bundle and import it on every k3s node.</li>
 *   <li>{@code kubectl apply -k k8s/}; wait for all pods Ready.</li>
 * </ol>
 *
 * <p>Opt-in: {@code -Dcluster.k8s.it=true}.
 *
 * <p>Tunables (system properties):
 * <ul>
 *   <li>{@code cluster.k8s.host}      host that exposes NodePorts (default: localhost)</li>
 *   <li>{@code cluster.k8s.namespace} k8s namespace (default: market-maker)</li>
 *   <li>{@code kubectl}               kubectl binary path (default: kubectl)</li>
 *   <li>{@code kubectl.ssh}           ssh prefix (default: ssh sack@192.168.8.11)</li>
 *   <li>{@code kubectl.remote}        remote kubectl command
 *       (default: doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "cluster.k8s.it", matches = "true")
class SubmittingExternalOrderErrorsClusterTest {

    private static final Set<String> SEED_SYMBOLS = new TreeSet<>(List.of(
            "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META"));

    // NodePorts defined in k8s/*.yaml — mirror the other k8s tests.
    private static final int TRADING_STATE_PORT = 30180;
    private static final int EXCHANGE_PORT      = 30181;
    private static final int PUBLISHER_PORT     = 30183;

    // StatefulSet pods targeted for crash injection (ordinal 0 of each tier).
    private static final String EXCHANGE_POD      = "exchange-0";
    private static final String TRADING_STATE_POD = "trading-state-0";

    private static final Duration FAILOVER_BUDGET = Duration.ofMinutes(2);

    private static final String HOST = System.getProperty("cluster.k8s.host", "localhost");
    private static final String NS   = System.getProperty("cluster.k8s.namespace", "market-maker");
    private static final String KUBECTL = System.getProperty("kubectl", "kubectl");

    // SSH the kubectl invocations to the control-plane node where KUBECONFIG
    // lives. Set -Dkubectl.ssh="" to run kubectl locally instead.
    private static final String KUBECTL_SSH = System.getProperty("kubectl.ssh",
            "ssh sack@192.168.8.11");
    private static final String KUBECTL_REMOTE = System.getProperty("kubectl.remote",
            "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @BeforeAll
    static void waitForStack() throws Exception {
        System.out.println("[ERR1-3-k8s] host=" + HOST + " namespace=" + NS);
        awaitHealthy("trading-state",      TRADING_STATE_PORT, Duration.ofMinutes(5));
        awaitHealthy("exchange",           EXCHANGE_PORT,      Duration.ofMinutes(5));
        awaitHealthy("external-publisher", PUBLISHER_PORT,     Duration.ofMinutes(5));
        driveTrafficUntilEverySymbolHasFill(Duration.ofMinutes(3));
        System.out.println("[ERR1-3-k8s] stack up, baseline traffic flowing.");
    }

    @AfterEach
    void restoreFullStrength() throws Exception {
        // Each test deletes a pod; the StatefulSet recreates it. Wait for both
        // tiers to be fully rolled out so the next test starts at full strength.
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/exchange", "-n", NS, "--timeout=5m");
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/trading-state", "-n", NS, "--timeout=5m");
        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(3));
        awaitHealthy("exchange",      EXCHANGE_PORT,      Duration.ofMinutes(3));
    }

    // --- Error Case 1: an exchange pod goes down ---

    /**
     * Delete one exchange pod while orders are flowing. The {@code exchange}
     * Service routes around it to the surviving replicas, so new fills keep
     * landing in trading-state — failover, not loss.
     */
    @Test
    void exchangeReplicaFailoverKeepsOrdersFilling() throws Exception {
        int fillsBefore = fillCountOrMinusOne();
        assertTrue(fillsBefore >= 0, "trading-state must be reachable before the test");

        deletePod(EXCHANGE_POD);

        awaitNewFillsAfter(fillsBefore, FAILOVER_BUDGET);
    }

    /**
     * A deleted exchange pod is recreated by the StatefulSet and the exchange
     * tier returns to full strength.
     */
    @Test
    void killedExchangeReplicaRejoinsCleanly() throws Exception {
        deletePod(EXCHANGE_POD);
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/exchange", "-n", NS, "--timeout=5m");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(3));
    }

    // --- Error Case 2: publisher retries with fresh ids (no duplicate fill) ---

    /**
     * Two waves of orders carry fresh UUIDs. They must produce independent
     * fills — the exchange does not dedup — so a publisher's blind retry after
     * a timeout is correctly accounted for as a separate trade.
     */
    @Test
    void retriedOrdersWithFreshIdsProduceIndependentFills() throws Exception {
        reseedQuietly();

        Set<UUID> existingFillIds = new HashSet<>();
        for (Fill f : getAllFills()) existingFillIds.add(f.getId());

        int wave1 = submitOrders(List.of("AAPL"), 5);
        int wave2 = submitOrders(List.of("AAPL"), 5);
        assertTrue(wave1 > 0 && wave2 > 0, "both waves must have accepted orders");

        awaitCondition(Duration.ofSeconds(30), () -> {
            try {
                long newFills = getAllFills().stream()
                        .filter(f -> !existingFillIds.contains(f.getId()))
                        .count();
                return newFills >= 2;
            } catch (Throwable t) {
                return false;
            }
        }, "expected at least two new AAPL fills across the two waves");

        Set<UUID> newOrderIds = new HashSet<>();
        for (Fill f : getAllFills()) {
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

    // --- Error Case 3: a trading-state pod goes down ---

    /**
     * Delete one trading-state pod while orders are flowing. Hazelcast data
     * replication plus leader failover (the exchange resolves the trading-state
     * leader via Zookeeper on every RSocket send) keep fills being recorded.
     */
    @Test
    void tradingStateReplicaFailoverKeepsFillsRecorded() throws Exception {
        int fillsBefore = fillCountOrMinusOne();
        assertTrue(fillsBefore >= 0, "trading-state must be reachable before the test");

        deletePod(TRADING_STATE_POD);

        awaitNewFillsAfter(fillsBefore, FAILOVER_BUDGET);
    }

    /**
     * A deleted trading-state pod is recreated by the StatefulSet and the
     * trading-state tier returns to full strength.
     */
    @Test
    void killedTradingStateReplicaRejoinsCleanly() throws Exception {
        deletePod(TRADING_STATE_POD);
        runKubectl(TimeUnit.MINUTES.toMillis(5),
                "rollout", "status", "sts/trading-state", "-n", NS, "--timeout=5m");
        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(3));
    }

    // --- Sanity: a single pod loss does not take the tier down ---

    /**
     * Deleting one exchange pod must not make the exchange tier unreachable:
     * {@code GET /quotes/{symbol}} keeps returning a quote, served by a
     * surviving replica.
     */
    @Test
    void killingExchangeReplicaKeepsTheTierServingQuotes() throws Exception {
        reseedQuietly();
        awaitCondition(Duration.ofSeconds(30),
                () -> currentExchangeQuoteId("AAPL") != null,
                "AAPL quote did not appear in exchange after seed");

        deletePod(EXCHANGE_POD);

        awaitCondition(Duration.ofSeconds(60),
                () -> currentExchangeQuoteId("AAPL") != null,
                "exchange tier stopped serving /quotes/AAPL after one pod was deleted");
    }

    // ---------- crash injection ----------

    /** Delete {@code pod}; the StatefulSet recreates it under the same name. */
    private static void deletePod(String pod) throws Exception {
        System.out.println("[ERR1-3-k8s] kubectl delete pod " + pod);
        String out = runKubectl(TimeUnit.MINUTES.toMillis(2),
                "delete", "pod", pod, "-n", NS, "--wait=true");
        System.out.println("[ERR1-3-k8s]   " + out.trim());
    }

    // ---------- traffic ----------

    private static List<UUID> seedQuotes(List<String> symbols) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT + "/publisher/seed-quotes"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("seed-quotes returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<UUID>>() {});
    }

    /**
     * Best-effort quote seed — swallows transient failures during a failover
     * window. Catches {@code Throwable} deliberately: {@link #seedQuotes}
     * reports a non-200 via JUnit {@code fail()}, which throws an
     * {@code AssertionError}; mid-failover that is expected, not a failure.
     */
    private static void reseedQuietly() {
        try {
            seedQuotes(new ArrayList<>(SEED_SYMBOLS));
        } catch (Throwable ignored) {
            // exchange/publisher may be momentarily unreachable mid-failover
        }
    }

    /** Submit {@code count} orders per symbol via the publisher; returns the number accepted. */
    private static int submitOrders(List<String> symbols, int count) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + PUBLISHER_PORT
                            + "/publisher/submit-orders?count=" + count))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(60))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            return Integer.parseInt(resp.body().trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + TRADING_STATE_PORT + "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    /**
     * Total recorded fill count, or -1 if trading-state is currently
     * unreachable. Catches {@code Throwable}: {@link #getAllFills} reports a
     * non-200 via JUnit {@code fail()} (an {@code AssertionError}), a tolerable
     * outcome mid-failover.
     */
    private static int fillCountOrMinusOne() {
        try {
            return getAllFills().size();
        } catch (Throwable t) {
            return -1;
        }
    }

    private static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), Quote.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static UUID currentExchangeQuoteId(String symbol) {
        Quote q = currentExchangeQuote(symbol);
        return q == null ? null : q.quoteId();
    }

    /**
     * Drive enough order traffic for trading-state to record a fill on every
     * seed symbol. Baseline "system is healthy" check. Re-seeds each iteration
     * because quotes carry a 30s TTL.
     */
    private static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        Set<String> withFills = new TreeSet<>();
        while (Instant.now().isBefore(deadline)) {
            reseedQuietly();
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            for (Fill f : getAllFills()) {
                if (SEED_SYMBOLS.contains(f.symbol())) withFills.add(f.symbol());
            }
            if (withFills.equals(SEED_SYMBOLS)) return;
            Thread.sleep(1500);
        }
        throw new AssertionError("baseline traffic did not produce fills for every symbol within "
                + timeout + "; got=" + withFills);
    }

    /**
     * Keep submitting orders until trading-state records strictly more fills
     * than {@code baselineFillCount}, proving the order path is alive. Rides
     * out the brief failover window and re-seeds each iteration (30s quote TTL).
     */
    private static void awaitNewFillsAfter(int baselineFillCount, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            reseedQuietly();
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 10);
            int now = fillCountOrMinusOne();
            if (now > baselineFillCount) return;
            sleepQuietly(2000);
        }
        throw new AssertionError("no new fills recorded within " + timeout
                + " after baseline=" + baselineFillCount + "; failover did not keep the order path alive");
    }

    // ---------- health ----------

    private static void awaitHealthy(String workload, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[ERR1-3-k8s] " + workload + " healthy on " + HOST + ":" + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(workload + " not healthy within " + timeout);
    }

    private static boolean healthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + HOST + ":" + port + "/health"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    // ---------- generic helpers ----------

    /**
     * Run kubectl either locally or via ssh to a control-plane node, mirroring
     * the convention in the {@code ClusterError*} tests. Returns combined
     * stdout+stderr; throws if kubectl exits non-zero.
     */
    private static String runKubectl(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        if (KUBECTL_SSH != null && !KUBECTL_SSH.isBlank()) {
            StringBuilder remote = new StringBuilder(KUBECTL_REMOTE);
            for (String a : args) remote.append(' ').append(a);
            for (String token : KUBECTL_SSH.split(" +")) cmd.add(token);
            cmd.add(remote.toString());
        } else {
            cmd.add(KUBECTL);
            Collections.addAll(cmd, args);
        }
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            output.append("[timed out waiting for kubectl command]\n");
        }
        if (p.exitValue() != 0) {
            throw new AssertionError("kubectl " + String.join(" ", args)
                    + " failed (exit=" + p.exitValue() + "): " + output);
        }
        return output.toString();
    }

    private static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) return;
            sleepQuietly(1000);
        }
        throw new AssertionError(failureMessage);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
