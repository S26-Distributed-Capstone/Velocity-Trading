package edu.yu.marketmaker.errorcases.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.ExposureState;
import edu.yu.marketmaker.model.Fill;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Error case 4 local end-to-end: one MM misses a position update, survivors
 * keep the symbol quotable, and the restarted MM rejoins from a fresh snapshot.
 */
@EnabledIfSystemProperty(named = "cluster.it", matches = "true")
class LocalError4MMCrashBeforePositionUpdateTest {

    private static final String TAG = "ERR4";

    @BeforeAll
    static void bootStack() throws Exception {
        bootStack(TAG);
        driveTrafficUntilEverySymbolHasFill(Duration.ofMinutes(3));
    }

    @AfterAll
    static void teardownStack() throws Exception {
        teardownStack(TAG);
    }

    @AfterEach
    void restoreServices() throws Exception {
        restoreServices(TAG);
    }

    @Test
    void killedMarketMakerIsEvictedAndSurvivorsReconverge() throws Exception {
        int leaderPort = leaderPort();
        assertTrue(leaderPort > 0, "no leader before the test");
        int victimPort = crashVictimPort(leaderPort);
        String victimService = MM_PORT_TO_SERVICE.get(victimPort);
        String observedSymbol = firstAssignedSymbol(victimPort);
        UUID preCrashQuoteId = awaitQuoteId(
                observedSymbol, Duration.ofSeconds(30));
        assertNotNull(preCrashQuoteId, "expected an active quote before crash");

        kill(victimService);
        awaitCondition(Duration.ofMinutes(2),
                () -> survivorsConverged(
                        victimPort, MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);
        awaitCondition(Duration.ofSeconds(30),
                () -> currentExchangeQuoteId(observedSymbol) != null,
                "observed symbol became unquotable after node crash: " + observedSymbol);
    }

    @Test
    void restartedMarketMakerRejoinsAndPublishesFreshQuote() throws Exception {
        int leaderPort = leaderPort();
        int victimPort = crashVictimPort(leaderPort);
        String victimService = MM_PORT_TO_SERVICE.get(victimPort);
        String observedSymbol = firstAssignedSymbol(victimPort);
        UUID preCrashQuoteId = awaitQuoteId(
                observedSymbol, Duration.ofSeconds(30));

        kill(victimService);
        awaitCondition(Duration.ofMinutes(2),
                () -> survivorsConverged(
                        victimPort, MM_PORT_TO_SERVICE.size() - 1),
                "survivors did not reconverge after killing " + victimService);

        start(victimService);
        awaitCondition(Duration.ofMinutes(3),
                LocalError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not reconverge after restarting " + victimService);
        JsonNode status = mmStatusOrNull(victimPort);
        assertNotNull(status, "restarted MM did not respond to /marketmaker/status");

        awaitCondition(Duration.ofMinutes(2), () -> {
            try {
                submitOrders(List.of(observedSymbol), 5);
            } catch (Exception ignored) {
                // tolerate transient recovery churn
            }
            UUID after = currentExchangeQuoteId(observedSymbol);
            return after != null && !after.equals(preCrashQuoteId);
        }, "observed symbol quote did not refresh after node restart: " + observedSymbol);
    }


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

    private static final List<String> TRADING_STATE_SERVICES =
            List.of("trading-state-1", "trading-state-2", "trading-state-3");
    private static final List<String> EXCHANGE_SERVICES =
            List.of("exchange-1", "exchange-2", "exchange-3");
    private static final List<String> EXPOSURE_RESERVATION_SERVICES =
            List.of("exposure-reservation-1", "exposure-reservation-2", "exposure-reservation-3");
    private static final Map<String, List<String>> LOGICAL_SERVICE_GROUPS = Map.of(
            "trading-state", TRADING_STATE_SERVICES,
            "exchange", EXCHANGE_SERVICES,
            "exposure-reservation", EXPOSURE_RESERVATION_SERVICES
    );
    private static final Set<String> REQUIRED_STACK_SERVICES = requiredStackServices();

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();
    static void bootStack(String tag) throws Exception {
        System.out.println("[" + tag + "] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3),
                "compose", "down", "-v", "--remove-orphans");

        System.out.println("[" + tag + "] docker compose build...");
        int buildRc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[" + tag + "] bringing up core infra...");
        List<String> core = new ArrayList<>(List.of(
                "zookeeper1", "zookeeper2", "zookeeper3", "postgres"));
        core.addAll(TRADING_STATE_SERVICES);
        core.addAll(EXPOSURE_RESERVATION_SERVICES);
        core.add("service-lb");
        int coreRc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(5),
                concat(new String[]{"compose", "up", "-d"}, core));
        assertEquals(0, coreRc, "docker compose up (core) failed");

        awaitHealthy(tag, "trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy(tag, "exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up exchange + publisher...");
        List<String> exchangeAndPublisher = new ArrayList<>(EXCHANGE_SERVICES);
        exchangeAndPublisher.add("external-publisher");
        int appRc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(3),
                concat(new String[]{"compose", "up", "-d"}, exchangeAndPublisher));
        assertEquals(0, appRc, "docker compose up (exchange/publisher) failed");
        awaitHealthy(tag, "exchange", EXCHANGE_PORT, Duration.ofMinutes(4));
        awaitHealthy(tag, "external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up market-maker nodes...");
        List<String> mmUp = new ArrayList<>(List.of("compose", "up", "-d"));
        mmUp.addAll(MM_PORT_TO_SERVICE.values());
        int mmRc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(5),
                mmUp.toArray(String[]::new));
        assertEquals(0, mmRc, "docker compose up (market-maker nodes) failed");

        System.out.println("[" + tag + "] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8), LocalError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not converge within 8 minutes");
        awaitCondition(Duration.ofMinutes(2), LocalError4MMCrashBeforePositionUpdateTest::requiredServicesRunning,
                "not all expected compose services reached running state");
        System.out.println("[" + tag + "] full stack up.");
    }

    static void teardownStack(String tag) throws Exception {
        System.out.println("[" + tag + "] docker compose down -v");
        runDocker(null, TimeUnit.MINUTES.toMillis(2), "compose", "down", "-v");
    }

    static void restoreServices(String tag) throws Exception {
        start("exposure-reservation");
        awaitHealthy(tag, "exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(2));
        start("exchange");
        awaitHealthy(tag, "exchange", EXCHANGE_PORT, Duration.ofMinutes(2));
        for (String mm : MM_PORT_TO_SERVICE.values()) {
            start(mm);
        }
        awaitCondition(Duration.ofMinutes(3), LocalError4MMCrashBeforePositionUpdateTest::allNodesConverged,
                "cluster did not reconverge to " + MM_PORT_TO_SERVICE.size()
                        + " nodes after restoring services");
    }

    static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        Set<UUID> bootstrapIds = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
        if (bootstrapIds.isEmpty()) {
            fail("seed-quotes returned no bootstrap ids");
        }

        Set<String> symbolsWithFills = new TreeSet<>();
        while (Instant.now().isBefore(deadline)) {
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            for (Fill fill : getAllFills()) {
                if (SEED_SYMBOLS.contains(fill.symbol())) {
                    symbolsWithFills.add(fill.symbol());
                }
            }
            if (symbolsWithFills.equals(SEED_SYMBOLS)) return;
            Thread.sleep(1500);
        }
        throw new AssertionError("baseline traffic did not produce fills for every symbol within "
                + timeout + "; got=" + symbolsWithFills);
    }

    static void kill(String service) throws Exception {
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                concat(new String[]{"compose", "kill", "-s", "SIGKILL"}, targets));
        assertEquals(0, rc, "docker compose kill failed for " + targets);
    }

    static void stop(String service) throws Exception {
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(1),
                concat(new String[]{"compose", "stop", "-t", "5"}, targets));
        assertEquals(0, rc, "docker compose stop failed for " + targets);
    }

    static void start(String service) throws Exception {
        List<String> targets = resolveComposeTargets(service);
        int rc = runDocker(productionEnv(), TimeUnit.MINUTES.toMillis(3),
                concat(new String[]{"compose", "up", "-d"}, targets));
        assertEquals(0, rc, "docker compose up failed for " + targets);
    }

    static int crashVictimPort(int leaderPort) {
        return MM_PORT_TO_SERVICE.keySet().stream()
                .filter(port -> port != leaderPort)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no non-leader MM node available (leaderPort=" + leaderPort + ")"));
    }

    static int leaderPort() {
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status != null && status.path("leader").asBoolean(false)) {
                return port;
            }
        }
        return -1;
    }

    static boolean allNodesConverged() {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String leaderId = status.path("leaderId").asText(null);
            if (leaderId == null) return false;
            leaders.add(leaderId);
            if (status.path("members").size() != MM_PORT_TO_SERVICE.size()) return false;
        }
        return responding == MM_PORT_TO_SERVICE.size() && leaders.size() == 1;
    }

    static boolean survivorsConverged(int excludedPort, int expectedMembers) {
        int responding = 0;
        Set<String> leaders = new HashSet<>();
        for (int port : MM_PORT_TO_SERVICE.keySet()) {
            if (port == excludedPort) continue;
            JsonNode status = clusterStatusOrNull(port);
            if (status == null) return false;
            responding++;
            String leaderId = status.path("leaderId").asText(null);
            if (leaderId == null) return false;
            leaders.add(leaderId);
            if (status.path("members").size() != expectedMembers) return false;
        }
        return responding == expectedMembers && leaders.size() == 1;
    }

    static JsonNode mmStatusOrNull(int port) {
        return jsonOrNull(port, "/marketmaker/status");
    }

    static String firstAssignedSymbol(int port) {
        JsonNode status = mmStatusOrNull(port);
        if (status == null) return "AAPL";
        JsonNode assigned = status.path("assigned");
        if (!assigned.isArray() || assigned.isEmpty()) return "AAPL";
        return assigned.get(0).asText("AAPL");
    }

    static UUID awaitQuoteId(String symbol, Duration timeout) {
        UUID[] out = new UUID[1];
        awaitCondition(timeout, () -> {
            out[0] = currentExchangeQuoteId(symbol);
            return out[0] != null;
        }, "quote did not appear for symbol " + symbol + " within " + timeout);
        return out[0];
    }

    static ExposureState awaitExposure(Duration timeout) {
        ExposureState[] out = new ExposureState[1];
        awaitCondition(timeout, () -> {
            out[0] = currentExposure();
            return out[0] != null;
        }, "exposure did not become reachable within " + timeout);
        return out[0];
    }

    static boolean healthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(port, "/health"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    static int submitOrders(List<String> symbols, int count) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri(PUBLISHER_PORT, "/publisher/submit-orders?count=" + count))
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
                    .uri(uri(TRADING_STATE_PORT, "/state/fills"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                    .build();
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    static UUID currentExchangeQuoteId(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(EXCHANGE_PORT, "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            String raw = JSON.readTree(resp.body()).path("quoteId").asText(null);
            return raw == null ? null : UUID.fromString(raw);
        } catch (Exception e) {
            return null;
        }
    }

    static ExposureState currentExposure() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(EXPOSURE_RES_PORT, "/exposure"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readValue(resp.body(), ExposureState.class);
        } catch (Exception e) {
            return null;
        }
    }

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

    private static JsonNode clusterStatusOrNull(int port) {
        return jsonOrNull(port, "/cluster/status");
    }

    private static JsonNode jsonOrNull(int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri(port, path))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.readTree(resp.body());
        } catch (Exception e) {
            return null;
        }
    }

    private static void awaitHealthy(String tag, String service, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println("[" + tag + "] " + service + " healthy on port " + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(service + " not healthy within " + timeout);
    }

    private static List<UUID> seedQuotes(List<String> symbols) throws Exception {
        String body = JSON.writeValueAsString(symbols);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri(PUBLISHER_PORT, "/publisher/seed-quotes"))
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

    private static List<Fill> getAllFills() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(uri(TRADING_STATE_PORT, "/state/fills"))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            fail("GET /state/fills returned " + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readValue(resp.body(), new TypeReference<List<Fill>>() {});
    }

    private static URI uri(int port, String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private static int runDocker(Map<String, String> env, long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true)
                .inheritIO();
        if (env != null) pb.environment().putAll(env);
        Process p = pb.start();
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running: " + String.join(" ", cmd));
        }
        return p.exitValue();
    }

    private static String runDockerCapturing(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true);
        pb.environment().putAll(productionEnv());
        Process p = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) output.append(line).append('\n');
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
            String output = runDockerCapturing(TimeUnit.SECONDS.toMillis(30),
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

    private static Set<String> requiredStackServices() {
        Set<String> services = new LinkedHashSet<>(List.of(
                "zookeeper1", "zookeeper2", "zookeeper3",
                "postgres", "service-lb", "external-publisher"));
        services.addAll(TRADING_STATE_SERVICES);
        services.addAll(EXCHANGE_SERVICES);
        services.addAll(EXPOSURE_RESERVATION_SERVICES);
        services.addAll(MM_PORT_TO_SERVICE.values());
        return Collections.unmodifiableSet(services);
    }
}
