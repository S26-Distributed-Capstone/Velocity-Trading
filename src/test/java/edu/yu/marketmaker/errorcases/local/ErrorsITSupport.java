package edu.yu.marketmaker.errorcases.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared docker-compose orchestration and HTTP helpers for the
 * {@code edu.yu.marketmaker.errorcases.local} HA failover tests.
 * <p>
 * Boots the production {@code compose.yml} HA core: a 3-node Zookeeper
 * ensemble, postgres, and three replicas each of trading-state,
 * exposure-reservation and exchange behind the {@code service-lb} nginx
 * load balancer, plus the external-publisher. The 7 market-maker nodes are
 * <em>not</em> started — error cases 1-3 only exercise the
 * exchange/trading-state path and the publisher seeds quotes directly.
 * <p>
 * {@code docs/error-cases.md} was written before the system gained its
 * 3-replica backup design. With backups in place the relevant scenario is
 * <em>failover</em>: one replica is killed and the survivors keep the
 * system running. Crash injection therefore targets a single replica
 * (e.g. {@code exchange-1}), not a whole tier.
 * <p>
 * This class is a stateless utility: every method is static. Callers invoke
 * {@link #bootStack(String)} from {@code @BeforeAll} and
 * {@link #teardownStack(String)} from {@code @AfterAll}.
 */
final class ErrorsITSupport {

    /** Replicas of each HA role, by compose service name. */
    static final List<String> TRADING_STATE_REPLICAS =
            List.of("trading-state-1", "trading-state-2", "trading-state-3");
    static final List<String> EXCHANGE_REPLICAS =
            List.of("exchange-1", "exchange-2", "exchange-3");
    static final List<String> EXPOSURE_RES_REPLICAS =
            List.of("exposure-reservation-1", "exposure-reservation-2", "exposure-reservation-3");

    static final Set<String> SEED_SYMBOLS = Collections.unmodifiableSet(
            new TreeSet<>(List.of("AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META")));

    // Host ports exposed by the service-lb nginx container (and the publisher).
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
     * Build the image, then bring up the HA core (no market-maker nodes) and
     * wait for every load-balanced role plus the publisher to report healthy.
     */
    static void bootStack(String tag) throws Exception {
        System.out.println("[" + tag + "] cleaning any prior stack...");
        compose(TimeUnit.MINUTES.toMillis(3), "down", "-v", "--remove-orphans");

        System.out.println("[" + tag + "] docker compose build (first run may take several minutes)...");
        int buildRc = compose(TimeUnit.MINUTES.toMillis(20), "build", "trading-state-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[" + tag + "] bringing up zookeeper ensemble + postgres + state/reservation tiers...");
        List<String> coreUp = new ArrayList<>(List.of("up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3", "postgres", "service-lb"));
        coreUp.addAll(TRADING_STATE_REPLICAS);
        coreUp.addAll(EXPOSURE_RES_REPLICAS);
        int rcCore = compose(TimeUnit.MINUTES.toMillis(6), coreUp.toArray(String[]::new));
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(5));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(5));

        System.out.println("[" + tag + "] bringing up exchange tier...");
        List<String> exchangeUp = new ArrayList<>(List.of("up", "-d"));
        exchangeUp.addAll(EXCHANGE_REPLICAS);
        int rcExchange = compose(TimeUnit.MINUTES.toMillis(4), exchangeUp.toArray(String[]::new));
        assertEquals(0, rcExchange, "docker compose up (exchange) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(5));

        System.out.println("[" + tag + "] bringing up external-publisher...");
        int rcPub = compose(TimeUnit.MINUTES.toMillis(3), "up", "-d", "external-publisher");
        assertEquals(0, rcPub, "docker compose up (external-publisher) failed");
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[" + tag + "] HA core up.");
    }

    /** Tear down the entire stack, including volumes. */
    static void teardownStack(String tag) throws Exception {
        System.out.println("[" + tag + "] docker compose down -v");
        compose(TimeUnit.MINUTES.toMillis(2), "down", "-v");
    }

    // ---------- crash injection (single replica) ----------

    /** Kill one replica with SIGKILL — simulates a hard crash of that node. */
    static void kill(String replica) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(1), "kill", "-s", "SIGKILL", replica);
        assertEquals(0, rc, "docker compose kill failed for " + replica);
    }

    /** Stop one replica gracefully (SIGTERM, then SIGKILL after a short timeout). */
    static void stop(String replica) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(1), "stop", "-t", "5", replica);
        assertEquals(0, rc, "docker compose stop failed for " + replica);
    }

    /**
     * Restart one replica via {@code compose up -d}, which recreates the
     * container if it was killed and waits for its depends_on chain.
     */
    static void start(String replica) throws Exception {
        int rc = compose(TimeUnit.MINUTES.toMillis(3), "up", "-d", replica);
        assertEquals(0, rc, "docker compose up failed for " + replica);
    }

    /** @return true if {@code replica}'s container is in compose state "running". */
    static boolean isRunning(String replica) throws Exception {
        String out = composeCapturing(TimeUnit.SECONDS.toMillis(20), "ps", "--status", "running", replica);
        return out.contains(replica);
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
     * service. Returns the bootstrap quoteIds.
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
     * Returns the number the exchange accepted (HTTP 200). A transport error
     * is reported as 0 accepted rather than throwing — during a failover
     * window some requests legitimately fail before the survivors take over.
     */
    static int submitOrders(List<String> symbols, int count) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + PUBLISHER_PORT
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

    /** Every fill currently recorded by trading-state (read via the load balancer). */
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

    /**
     * Total recorded fill count, or -1 if trading-state is currently
     * unreachable. Catches {@code Throwable} deliberately: {@link #getAllFills}
     * signals a non-200 response via JUnit {@code fail()}, which throws an
     * {@code AssertionError} — that is an expected, tolerable outcome while a
     * replica is mid-failover, not a test failure.
     */
    static int fillCountOrMinusOne() {
        try {
            return getAllFills().size();
        } catch (Throwable t) {
            return -1;
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

    /**
     * Best-effort quote seed — swallows transient failures during a failover
     * window. Catches {@code Throwable} deliberately: {@link #seedQuotes}
     * signals a non-200 response via JUnit {@code fail()}, which throws an
     * {@code AssertionError}. While a replica is mid-failover the publisher
     * may briefly 500; that is expected and tolerable, not a test failure.
     */
    static void reseedQuietly() {
        try {
            seedQuotes(new ArrayList<>(SEED_SYMBOLS));
        } catch (Throwable ignored) {
            // exchange/publisher may be momentarily unreachable mid-failover
        }
    }

    /**
     * Drive enough order traffic for trading-state to record a fill on every
     * seed symbol. Used as the "system is healthy" baseline before injecting
     * a fault. Re-seeds each iteration because quotes carry a 30s TTL.
     */
    static void driveTrafficUntilEverySymbolHasFill(Duration timeout) throws Exception {
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
     * out the brief load-balancer / DNS settling window after a replica is
     * killed, and re-seeds each iteration to outlast the 30s quote TTL.
     * Fails if no new fill lands within {@code timeout}.
     */
    static void awaitNewFillsAfter(int baselineFillCount, Duration timeout) {
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

    // ---------- generic helpers ----------

    static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) return;
            sleepQuietly(1000);
        }
        throw new AssertionError(failureMessage);
    }

    static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Run {@code docker compose <args>} (production compose.yml) from the project root. */
    static int compose(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose"));
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

    /** Run {@code docker compose <args>} and return its combined stdout/stderr. */
    static String composeCapturing(long timeoutMs, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose"));
        Collections.addAll(cmd, args);
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(PROJECT_ROOT.toFile())
                .redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("timeout running: " + String.join(" ", cmd));
        }
        return output;
    }
}
