package edu.yu.marketmaker.errors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    private static final int MM_NODES = boundedIntProperty("it.mm.nodes", 3, 1, 7);
    private static final int HA_REPLICAS = boundedIntProperty("it.ha.replicas", 1, 1, 3);

    /** Host port -> compose service name, for the 7 MM nodes. */
    static final SortedMap<Integer, String> MM_PORT_TO_SERVICE;
    static {
        SortedMap<Integer, String> m = new TreeMap<>();
        for (int i = 1; i <= MM_NODES; i++) {
            m.put(8080 + i, "market-maker-node-" + i);
        }
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
    private static final List<String> TRADING_STATE_SERVICES =
            replicaServices("trading-state", HA_REPLICAS);
    private static final List<String> EXCHANGE_SERVICES =
            replicaServices("exchange", HA_REPLICAS);
    private static final List<String> EXPOSURE_RESERVATION_SERVICES =
            replicaServices("exposure-reservation", HA_REPLICAS);
    private static final Map<String, List<String>> LOGICAL_SERVICE_GROUPS = Map.of(
            "trading-state", TRADING_STATE_SERVICES,
            "exchange", EXCHANGE_SERVICES,
            "exposure-reservation", EXPOSURE_RESERVATION_SERVICES
    );
    private static final Set<String> REQUIRED_STACK_SERVICES = requiredStackServices();

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
        List<String> coreServices = new ArrayList<>(List.of(
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres"));
        coreServices.addAll(TRADING_STATE_SERVICES);
        coreServices.addAll(EXPOSURE_RESERVATION_SERVICES);
        coreServices.add("service-lb");
        int rcCore = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(5),
                concat(new String[]{"compose", "up", "-d"}, coreServices));
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up exchange...");
        List<String> exchangeServices = new ArrayList<>(EXCHANGE_SERVICES);
        int rcExchange = runDocker(productionEnv(),
                TimeUnit.MINUTES.toMillis(3),
                concat(new String[]{"compose", "up", "-d"}, exchangeServices));
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

        System.out.println("[" + tag + "] waiting for " + MM_PORT_TO_SERVICE.size() + "-node cluster convergence...");
        awaitMmClusterConverged(tag, Duration.ofMinutes(8));
        awaitCondition(Duration.ofMinutes(2), ErrorsITSupport::requiredServicesRunning,
                "not all expected compose services reached running state");
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
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                concat(new String[]{"compose", "kill", "-s", "SIGKILL"}, targets));
        assertEquals(0, rc, "docker compose kill failed for " + targets);
    }

    /** Stop {@code service} gracefully (SIGTERM, then SIGKILL after timeout). */
    static void stop(String service) throws Exception {
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                concat(new String[]{"compose", "stop", "-t", "5"}, targets));
        assertEquals(0, rc, "docker compose stop failed for " + targets);
    }

    /**
     * Restart {@code service} via {@code compose up -d}, which recreates the
     * container if it was killed and waits for the depends_on chain.
     */
    static void start(String service) throws Exception {
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(3),
                concat(new String[]{"compose", "up", "-d"}, targets));
        assertEquals(0, rc, "docker compose up failed for " + targets);
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
        System.err.println("[ErrorsIT] " + serviceName + " did not respond on /health within " + timeout);
        List<String> targets = resolveComposeTargets(serviceName);
        System.err.println("---- docker compose ps " + String.join(" ", targets) + " ----");
        System.err.println(runDockerCapturing(productionEnv(), TimeUnit.SECONDS.toMillis(30),
                concat(new String[]{"compose", "ps"}, targets)));
        System.err.println("---- docker compose logs --tail 300 " + String.join(" ", targets) + " ----");
        System.err.println(runDockerCapturing(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                concat(new String[]{"compose", "logs", "--tail", "300"}, targets)));
        System.err.println("---- end logs ----");
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
        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        String lastFailure = null;
        Exception lastException = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> resp = postSeedQuotes(symbols);
                if (resp.statusCode() == 200) {
                    return JSON.readValue(resp.body(), new TypeReference<List<UUID>>() {});
                }
                lastFailure = "seed-quotes returned " + resp.statusCode() + ": " + resp.body();
            } catch (IOException | InterruptedException e) {
                lastException = e;
                lastFailure = "seed-quotes request failed: " + e;
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            Thread.sleep(1000);
        }
        if (lastException != null) {
            fail(lastFailure, lastException);
        }
        fail(lastFailure == null
                ? "seed-quotes did not complete before retry deadline"
                : lastFailure);
        return List.of();
    }

    private static HttpResponse<String> postSeedQuotes(List<String> symbols) throws IOException, InterruptedException {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PUBLISHER_PORT + "/publisher/seed-quotes"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
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

    /**
     * Submit a synthetic fill directly to trading-state's HTTP endpoint.
     * Useful in error-path tests where exchange-side order matching may be
     * unavailable but we still need a deterministic position update event.
     *
     * @return true on HTTP 200, false otherwise
     */
    static boolean submitSyntheticFill(String symbol) {
        try {
            Map<String, Object> body = Map.of(
                    "orderId", UUID.randomUUID().toString(),
                    "symbol", symbol,
                    "side", "BUY",
                    "quantity", 1,
                    "price", 100.0,
                    "quoteId", UUID.randomUUID().toString(),
                    "createdAt", System.currentTimeMillis()
            );
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + TRADING_STATE_PORT + "/state/fills"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                    .build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
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
        Instant deadline = Instant.now().plus(timeout);
        AssertionError lastSeedFailure = null;
        Set<UUID> ignored = Set.of();
        while (Instant.now().isBefore(deadline)) {
            try {
                ignored = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
                break;
            } catch (AssertionError e) {
                lastSeedFailure = e;
            }
            Thread.sleep(1000);
        }
        if (ignored.isEmpty()) {
            if (lastSeedFailure == null) {
                throw new AssertionError("seed-quotes did not succeed before " + timeout);
            }
            throw new AssertionError("seed-quotes did not succeed before " + timeout
                    + " (last failure: " + lastSeedFailure.getMessage() + ")", lastSeedFailure);
        }

        Set<String> withFills = new TreeSet<>();
        while (Instant.now().isBefore(deadline)) {
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            try {
                for (Fill fill : getAllFills()) {
                    if (SEED_SYMBOLS.contains(fill.symbol())) {
                        withFills.add(fill.symbol());
                    }
                }
            } catch (Exception ignoredReadError) {
                // trading-state may be briefly unavailable during startup churn;
                // keep driving traffic until the timeout.
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

    private static String runDockerCapturing(Map<String, String> env, long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true);
        if (env != null) {
            pb.environment().putAll(env);
        }
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
            output.append("[timed out waiting for docker command]\n");
        }
        return output.toString();
    }

    private static Map<String, String> productionEnv() {
        return Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator");
    }

    private static List<String> resolveComposeTargets(String service) {
        return LOGICAL_SERVICE_GROUPS.getOrDefault(service, List.of(service));
    }

    private static String[] concat(String[] prefix, List<String> suffix) {
        String[] out = Arrays.copyOf(prefix, prefix.length + suffix.size());
        for (int i = 0; i < suffix.size(); i++) {
            out[prefix.length + i] = suffix.get(i);
        }
        return out;
    }

    private static boolean requiredServicesRunning() {
        try {
            String output = runDockerCapturing(productionEnv(), TimeUnit.SECONDS.toMillis(30),
                    "compose", "ps", "--services", "--filter", "status=running");
            Set<String> running = new LinkedHashSet<>();
            for (String line : output.split("\\R")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) running.add(trimmed);
            }
            return running.containsAll(REQUIRED_STACK_SERVICES);
        } catch (Exception e) {
            return false;
        }
    }

    private static void awaitMmClusterConverged(String tag, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (allMmNodesConverged()) {
                return;
            }
            Thread.sleep(2000);
        }

        System.err.println("[" + tag + "] MM cluster failed to converge within " + timeout);
        System.err.println("---- docker compose ps ----");
        System.err.println(runDockerCapturing(productionEnv(), TimeUnit.SECONDS.toMillis(30),
                "compose", "ps"));

        for (Map.Entry<Integer, String> mm : MM_PORT_TO_SERVICE.entrySet()) {
            int port = mm.getKey();
            String service = mm.getValue();
            JsonNode status = clusterStatusOrNull(port);
            System.err.println("---- /cluster/status for " + service + " on :" + port + " ----");
            if (status == null) {
                System.err.println("(unreachable or non-200)");
            } else {
                System.err.println(status.toPrettyString());
            }
        }

        List<String> mmServices = new ArrayList<>(MM_PORT_TO_SERVICE.values());
        System.err.println("---- docker compose logs --tail 150 market-maker nodes ----");
        System.err.println(runDockerCapturing(productionEnv(), TimeUnit.MINUTES.toMillis(2),
                concat(new String[]{"compose", "logs", "--tail", "150"}, mmServices)));
        System.err.println("---- end diagnostics ----");

        throw new AssertionError("cluster did not converge within " + timeout);
    }

    private static int boundedIntProperty(String name, int defaultValue, int min, int max) {
        int parsed = Integer.getInteger(name, defaultValue);
        if (parsed < min) return min;
        return Math.min(parsed, max);
    }

    private static List<String> replicaServices(String base, int count) {
        List<String> out = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            out.add(base + "-" + i);
        }
        return List.copyOf(out);
    }

    private static Set<String> requiredStackServices() {
        Set<String> services = new LinkedHashSet<>(List.of(
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres",
                "service-lb",
                "external-publisher"));
        services.addAll(TRADING_STATE_SERVICES);
        services.addAll(EXCHANGE_SERVICES);
        services.addAll(EXPOSURE_RESERVATION_SERVICES);
        services.addAll(MM_PORT_TO_SERVICE.values());
        return Collections.unmodifiableSet(services);
    }
}
