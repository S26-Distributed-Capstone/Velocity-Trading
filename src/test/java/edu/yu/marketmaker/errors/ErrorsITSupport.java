package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared compose-stack orchestration and HTTP helpers for the
 * {@code edu.yu.marketmaker.errors} integration tests.
 * <p>
 * Mirrors the boot sequence used by
 * {@link edu.yu.marketmaker.cluster.ClusterIntegrationWithSystemTest} so each
 * errors test class drives the documented failure scenarios in
 * {@code docs/error-cases.md} against the real compose stack:
 * 3-node ZK ensemble + postgres + trading-state + exposure-reservation +
 * exchange + external-publisher + 7 market-maker nodes running the
 * {@code production-quote-generator} profile.
 * <p>
 * This class is a stateless utility: every method is static. Callers invoke
 * {@link #bootStack(String)} from {@code @BeforeAll} and {@link #teardownStack(String)}
 * from {@code @AfterAll}. Crash injection is implemented as docker compose
 * {@code kill}/{@code stop}/{@code start}/{@code up} on individual services.
 */
final class ErrorsITSupport {

    /** Host port -> compose service name, for the 7 MM nodes. */
    static final SortedMap<Integer, String> MM_PORT_TO_SERVICE;
    static {
        SortedMap<Integer, String> m = new TreeMap<>();
        m.put(8081, "market-maker-node-1");
        m.put(8082, "market-maker-node-2");
        m.put(8083, "market-maker-node-3");
        m.put(8084, "market-maker-node-4");
        m.put(8085, "market-maker-node-5");
        m.put(8086, "market-maker-node-6");
        m.put(8087, "market-maker-node-7");
        MM_PORT_TO_SERVICE = Collections.unmodifiableSortedMap(m);
    }

    static final Set<String> SEED_SYMBOLS = Collections.unmodifiableSet(
            new TreeSet<>(List.of("AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META")));

    static final int TRADING_STATE_PORT = 18080;
    static final int EXCHANGE_PORT = 18081;
    static final int EXPOSURE_RES_PORT = 18082;
    static final int PUBLISHER_PORT = 18083;

    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    private ErrorsITSupport() {}

    // ---------- compose lifecycle ----------

    /**
     * Build the image, then bring up the full stack and wait for every service
     * (including the 7-node MM cluster) to be healthy and converged. The
     * {@code production-quote-generator} profile is forced so that quotes go
     * through the real reservation path — that's what every documented error
     * case exercises.
     */
    static void bootStack(String tag) throws Exception {
        System.out.println("[" + tag + "] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3),
                "compose", "down", "-v", "--remove-orphans");

        System.out.println("[" + tag + "] docker compose build market-maker-node-1 (first run may take several minutes)...");
        int buildRc = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[" + tag + "] bringing up core infra (zk + postgres + trading-state + exposure-reservation)...");
        int rcCore = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres", "trading-state", "exposure-reservation");
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up exchange...");
        int rcExchange = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "exchange");
        assertEquals(0, rcExchange, "docker compose up (exchange) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up external-publisher...");
        int rcPub = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "external-publisher");
        assertEquals(0, rcPub, "docker compose up (external-publisher) failed");
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up 7 market-maker nodes...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(MM_PORT_TO_SERVICE.values());
        int rcMm = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(5),
                upCmd.toArray(String[]::new));
        assertEquals(0, rcMm, "docker compose up (market-maker nodes) failed");

        System.out.println("[" + tag + "] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(4), ErrorsITSupport::allMmNodesConverged,
                "cluster did not converge within 4 minutes");
        System.out.println("[" + tag + "] full stack up.");
    }

    /** Tear down the entire stack, including volumes. */
    static void teardownStack(String tag) throws Exception {
        System.out.println("[" + tag + "] docker compose down -v");
        runDocker(null, TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    // ---------- crash injection ----------

    /** Kill {@code service} with SIGKILL — simulates a hard crash. */
    static void kill(String service) throws Exception {
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                "compose", "kill", "-s", "SIGKILL", service);
        assertEquals(0, rc, "docker compose kill failed for " + service);
    }

    /** Stop {@code service} gracefully (SIGTERM, then SIGKILL after timeout). */
    static void stop(String service) throws Exception {
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                "compose", "stop", "-t", "5", service);
        assertEquals(0, rc, "docker compose stop failed for " + service);
    }

    /**
     * Restart {@code service} via {@code compose up -d}, which recreates the
     * container if it was killed and waits for the depends_on chain.
     */
    static void start(String service) throws Exception {
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", service);
        assertEquals(0, rc, "docker compose up failed for " + service);
    }

    // ---------- health & convergence ----------

    /** Block until {@code GET /health} on {@code port} returns 200, else fail loudly. */
    static void awaitHealthy(String serviceName, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[ErrorsIT] " + serviceName + " healthy on port " + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(serviceName + " not healthy within " + timeout);
    }

    static boolean healthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/health"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    /** All 7 MM nodes responding, agreeing on a single non-null leader, and seeing 7 members. */
    static boolean allMmNodesConverged() {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != MM_PORT_TO_SERVICE.size()) return false;
        }
        return responding == MM_PORT_TO_SERVICE.size() && leaders.size() == 1;
    }

    /**
     * Survivor MMs (every node except {@code excludedPort}) agree on one
     * non-null leader, and see exactly {@code expectedMembers} live members.
     */
    static boolean mmSurvivorsConverged(int excludedPort, int expectedMembers) {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            if (port == excludedPort) continue;
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String lid = status.path("leaderId").asText(null);
            if (lid == null) return false;
            leaders.add(lid);
            if (status.path("members").size() != expectedMembers) return false;
        }
        return responding == expectedMembers && leaders.size() == 1;
    }

    static JsonNode clusterStatusOrNull(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/cluster/status"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    /** @return host port for the current leader, or -1 if no leader yet. */
    static int leaderPort() {
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status != null && status.path("leader").asBoolean(false)) {
                return port;
            }
        }
        return -1;
    }

    static JsonNode mmStatusOrNull(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/marketmaker/status"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    // ---------- traffic ----------

    /**
     * PUT a fixed bid=99.50/ask=100.50 quote per symbol via the publisher
     * service. Returns the bootstrap quoteIds so callers can later distinguish
     * publisher-issued quotes from those written by market-makers.
     */
    static List<UUID> seedQuotes(List<String> symbols) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PUBLISHER_PORT + "/publisher/seed-quotes"))
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
     * Submit {@code count} orders per {@code symbol} via the publisher.
     * Returns the number the exchange accepted (HTTP 200); failures are
     * silently tallied as rejected, which is the correct behaviour when the
     * exchange is intentionally down for an error case.
     */
    static int submitOrders(List<String> symbols, int count) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PUBLISHER_PORT
                        + "/publisher/submit-orders?count=" + count))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("submit-orders returned " + resp.statusCode() + ": " + resp.body());
        }
        return Integer.parseInt(resp.body().trim());
    }

    /**
     * Try to submit a single order via the exchange directly, bypassing the
     * publisher. Returns the HTTP status code, or -1 if the request failed
     * outright (connection refused / timeout — exchange is down).
     */
    static int submitOrderDirectly(String symbol, int quantity, double limitPrice, String side) {
        return submitOrderDirectlyWithId(symbol, UUID.randomUUID(), quantity, limitPrice, side);
    }

    /**
     * Same as {@link #submitOrderDirectly} but with a caller-supplied order
     * id, so a test can later check whether that id was reflected in any
     * recorded fill (or, in the case-3 scenario, that it was silently lost).
     */
    static int submitOrderDirectlyWithId(String symbol, UUID orderId, int quantity,
                                         double limitPrice, String side) {
        try {
            Map<String, Object> body = Map.of(
                    "id", orderId.toString(),
                    "symbol", symbol,
                    "quantity", quantity,
                    "limitPrice", limitPrice,
                    "side", side
            );
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/orders"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                    .build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
        } catch (Exception e) {
            return -1;
        }
    }

    /** Every fill currently recorded by trading-state. */
    static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.copy()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    static long getNetPositionOrZero(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/positions/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            JsonNode node = JSON.readTree(resp.body());
            JsonNode pos = node.has("value") ? node.path("value") : node;
            if (pos.isMissingNode() || pos.isNull()) return 0;
            return pos.path("netQuantity").asLong(0);
        } catch (IOException | InterruptedException e) {
            return 0;
        }
    }

    /** @return the current quoteId in the exchange for {@code symbol}, or null on error / 404. */
    static UUID currentExchangeQuoteId(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode node = JSON.readTree(resp.body());
            String id = node.path("quoteId").asText(null);
            return id == null ? null : UUID.fromString(id);
        } catch (Exception e) {
            return null;
        }
    }

    /** Current exposure-reservation {@code GET /exposure} response, or null on error. */
    static JsonNode getExposureOrNull() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXPOSURE_RES_PORT + "/exposure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Drive enough order traffic for trading-state to record a fill on every
     * seed symbol. The end-to-end happy path is identical to
     * {@code ClusterIntegrationWithSystemTest} — we use it here as the
     * "system is working" baseline before injecting a fault.
     */
    static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
        Set<UUID> ignored = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
        Instant deadline = Instant.now().plus(timeout);
        Set<String> withFills = new TreeSet<>();
        while (Instant.now().isBefore(deadline)) {
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            for (String s : SEED_SYMBOLS) {
                if (getNetPositionOrZero(s) != 0) withFills.add(s);
            }
            if (withFills.equals(SEED_SYMBOLS)) return;
            Thread.sleep(1500);
        }
        throw new AssertionError("baseline traffic did not produce fills for every symbol within "
                + timeout + "; got=" + withFills + " (bootstrap quoteIds=" + ignored.size() + ")");
    }

    // ---------- generic helpers ----------

    static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError(failureMessage);
    }

    static int runDocker(Map<String, String> env, long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true)
                .inheritIO();
        if (env != null) {
            pb.environment().putAll(env);
        }
        Process p = pb.start();
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running: " + String.join(" ", cmd));
        }
        return p.exitValue();
    }

    private static Map<String, String> productionEnv() {
        return Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator");
    }
}
