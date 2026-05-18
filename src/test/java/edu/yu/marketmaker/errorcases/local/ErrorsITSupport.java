package edu.yu.marketmaker.errorcases.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;

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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared single-instance docker-compose orchestration and HTTP helpers for the
 * {@code edu.yu.marketmaker.errorcases.local} integration tests.
 * <p>
 * Boots the stack defined in {@code src/test/docker/errorcases-compose.yml}:
 * one each of postgres, zookeeper, trading-state, exposure-reservation,
 * exchange and external-publisher. One replica per role means a single
 * {@code docker compose stop <service>} is a genuine outage — which is what
 * the failure scenarios in {@code docs/error-cases.md} describe. (The
 * production {@code compose.yml} is a 3-replica HA stack behind a load
 * balancer, where stopping one replica is simply routed around.)
 * <p>
 * This class is a stateless utility: every method is static. Callers invoke
 * {@link #bootStack(String)} from {@code @BeforeAll} and {@link #teardownStack(String)}
 * from {@code @AfterAll}. Crash injection is implemented as docker compose
 * {@code kill}/{@code stop}/{@code start} on individual services.
 */
final class ErrorsITSupport {

    /** Compose file path, relative to the project root (the docker working dir). */
    static final String COMPOSE_FILE = "src/test/docker/errorcases-compose.yml";

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
     * Build the image, then bring up the single-instance stack and wait for
     * every service to report healthy on {@code /health}.
     */
    static void bootStack(String tag) throws Exception {
        System.out.println("[" + tag + "] cleaning any prior stack...");
        compose(TimeUnit.MINUTES.toMillis(3), "down", "-v", "--remove-orphans");

        System.out.println("[" + tag + "] docker compose build (first run may take several minutes)...");
        int buildRc = compose(TimeUnit.MINUTES.toMillis(20), "build", "trading-state");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[" + tag + "] bringing up postgres + zookeeper...");
        int rcInfra = compose(TimeUnit.MINUTES.toMillis(3), "up", "-d", "postgres", "zookeeper");
        assertEquals(0, rcInfra, "docker compose up (postgres + zookeeper) failed");

        System.out.println("[" + tag + "] bringing up trading-state + exposure-reservation + exchange...");
        int rcCore = compose(TimeUnit.MINUTES.toMillis(5),
                "up", "-d", "trading-state", "exposure-reservation", "exchange");
        assertEquals(0, rcCore, "docker compose up (core services) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] bringing up external-publisher...");
        int rcPub = compose(TimeUnit.MINUTES.toMillis(3), "up", "-d", "external-publisher");
        assertEquals(0, rcPub, "docker compose up (external-publisher) failed");
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] full stack up.");
    }

    /** Tear down the entire stack, including volumes. */
    static void teardownStack(String tag) throws Exception {
        System.out.println("[" + tag + "] docker compose down -v");
        compose(TimeUnit.MINUTES.toMillis(2), "down", "-v");
    }

    // ---------- crash injection ----------

    /** Kill {@code service} with SIGKILL — simulates a hard crash. */
    static void kill(String service) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(1), "kill", "-s", "SIGKILL", service);
        assertEquals(0, rc, "docker compose kill failed for " + service);
    }

    /** Stop {@code service} gracefully (SIGTERM, then SIGKILL after timeout). */
    static void stop(String service) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(1), "stop", "-t", "5", service);
        assertEquals(0, rc, "docker compose stop failed for " + service);
    }

    /**
     * Restart {@code service} via {@code compose up -d}, which recreates the
     * container if it was killed and waits for the depends_on chain.
     */
    static void start(String service) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(3), "up", "-d", service);
        assertEquals(0, rc, "docker compose up failed for " + service);
    }

    // ---------- health ----------

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
            java.util.Map<String, Object> body = java.util.Map.of(
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

    /** @return the current quote in the exchange for {@code symbol}, or null on error / 404. */
    static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + EXCHANGE_PORT + "/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return JSON.copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(resp.body(), Quote.class);
        } catch (Exception e) {
            return null;
        }
    }

    /** @return the current quoteId in the exchange for {@code symbol}, or null on error / 404. */
    static UUID currentExchangeQuoteId(String symbol) {
        Quote q = currentExchangeQuote(symbol);
        return q == null ? null : q.quoteId();
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
     * seed symbol. Used as the "system is working" baseline before injecting
     * a fault.
     */
    static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
        Set<UUID> ignored = new HashSet<>(seedQuotes(new ArrayList<>(SEED_SYMBOLS)));
        Instant deadline = Instant.now().plus(timeout);
        Set<String> withFills = new TreeSet<>();
        while (Instant.now().isBefore(deadline)) {
            submitOrders(new ArrayList<>(SEED_SYMBOLS), 25);
            // Probe by actual fill existence — a symbol whose BUY and SELL
            // cancel out has net=0 but does have fills recorded.
            for (Fill f : getAllFills()) {
                if (SEED_SYMBOLS.contains(f.symbol())) withFills.add(f.symbol());
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

    /** Run {@code docker compose -f <errorcases-compose.yml> <args>} from the project root. */
    static int compose(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", COMPOSE_FILE));
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true)
                .inheritIO();
        Process p = pb.start();
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running: " + String.join(" ", cmd));
        }
        return p.exitValue();
    }
}
