package edu.yu.marketmaker.errorcases.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.service.FaultInjector;

public class Common {

    /** Host port -> compose service name, for the 7 MM nodes. */
    private static final SortedMap<Integer, String> MM_PORT_TO_SERVICE;
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

    private static final Path PROJECT_ROOT = Path.of(".").toAbsolutePath().normalize();

    // NodePorts defined in k8s/*.yaml — mirror ClusterIntegrationWithSystemK8sTest.
    public static final int TRADING_STATE_PORT = 18080;
    public static final int EXCHANGE_PORT      = 18081;
    public static final int EXPOSURE_RES_PORT  = 18082;
    public static final int PUBLISHER_PORT     = 18083;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ObjectMapper JSON = new ObjectMapper();
    
    public static void bootStack(int errorCase) throws Exception {
        System.out.println("[ERR" + errorCase + "] cleaning any prior stack...");
        runDocker(null, TimeUnit.MINUTES.toMillis(3), "compose", "down", "-v", "--remove-orphans");

        System.out.println("[ERR" + errorCase + "] docker compose build...");
        int buildRc = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(20),
                "compose", "build", "market-maker-node-1");
        assertEquals(0, buildRc, "docker compose build failed");

        System.out.println("[ERR" + errorCase + "] bringing up core infra...");
        int rcCore = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                "compose", "up", "-d",
                "zookeeper1", "zookeeper2", "zookeeper3", "postgres",
                "trading-state-1", "trading-state-2", "trading-state-3",
                "exposure-reservation-1", "exposure-reservation-2", "exposure-reservation-3",
                "service-lb");
        assertEquals(0, rcCore, "docker compose up (core) failed");

        awaitHealthy("trading-state", TRADING_STATE_PORT, Duration.ofMinutes(4));
        awaitHealthy("exposure-reservation", EXPOSURE_RES_PORT, Duration.ofMinutes(4));

        System.out.println("[ERR" + errorCase + "] bringing up exchange + publisher...");
        int rcExchange = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(3),
                "compose", "up", "-d", "exchange-1", "exchange-2", "exchange-3", "external-publisher");
        assertEquals(0, rcExchange, "docker compose up (exchange/publisher) failed");
        awaitHealthy("exchange", EXCHANGE_PORT, Duration.ofMinutes(4));
        awaitHealthy("external-publisher", PUBLISHER_PORT, Duration.ofMinutes(4));

        System.out.println("[ERR" + errorCase + "] bringing up market-maker nodes (production-quote-generator + fault-injection)...");
        List<String> upCmd = new ArrayList<>(List.of("compose", "up", "-d"));
        upCmd.addAll(MM_PORT_TO_SERVICE.values());
        int rcMm = runDocker(Map.of("QUOTE_GENERATOR_PROFILE", "production-quote-generator"),
                TimeUnit.MINUTES.toMillis(5),
                upCmd.toArray(String[]::new));
        assertEquals(0, rcMm, "docker compose up (market-maker nodes) failed");

        System.out.println("[ERR" + errorCase + "] waiting for 7-node cluster convergence...");
        awaitCondition(Duration.ofMinutes(8),
                Common::allNodesConverged,
                "cluster did not converge within 8 minutes");
        System.out.println("[ERR" + errorCase + "] full stack up.");
    }

    public static int runDocker(Map<String, String> env, long timeoutMs, String... args) throws Exception {
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

    private static void awaitHealthy(String serviceName, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (healthy(port)) {
                System.out.println(serviceName + " healthy on port " + port);
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(serviceName + " not healthy within " + timeout);
    }

    private static boolean healthy(int port) {
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

    private static boolean allNodesConverged() {
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

    private static JsonNode clusterStatusOrNull(int port) {
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

    private static void awaitCondition(Duration timeout, BooleanSupplier condition, String failureMessage) {
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

    public static void armFaultInjector(FaultInjector.Event event, String symbol) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PUBLISHER_PORT
                        + "/test/arm-fault?symbol=" + symbol + "&" + "event=" + event.toString()))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "Fault injection failed: " + resp.body());
    }

    public static Quote currentExchangeQuote(String symbol) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + PUBLISHER_PORT + "/test/quotes/" + symbol))
                    .timeout(Duration.ofSeconds(5))
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

    public static List<UUID> seedQuotes(List<String> symbols) throws Exception {
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

    public static int submitOrdersViaPublisher(int countPerSymbol, List<String> symbols) {
        try {
            String body = JSON.writeValueAsString(symbols);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + PUBLISHER_PORT
                            + "/publisher/submit-orders?count=" + countPerSymbol))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            return Integer.parseInt(resp.body().trim());
        } catch (Exception e) {
            return 0;
        }
    }
}
