package edu.yu.marketmaker.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import edu.yu.marketmaker.service.ServiceHealth;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Test-driven external-order publisher. Under the {@code external-publisher}
 * profile this replaces the standalone {@link OrderPublisherRunner}: the
 * container boots idle and exposes two endpoints the end-to-end test calls
 * to drive traffic deterministically:
 * <ul>
 *   <li>{@code POST /publisher/seed-quotes} — PUTs a fixed, well-formed quote
 *       per symbol into the exchange so orders can match, and returns the
 *       generated quoteIds so the test can distinguish bootstrap quotes from
 *       ones later written by market-maker nodes.</li>
 *   <li>{@code POST /publisher/submit-orders?count=N} — submits {@code N}
 *       orders per symbol, priced to cross both the bootstrap spread and the
 *       tighter spread a {@code ProductionQuoteGenerator} will produce.</li>
 * </ul>
 */
@RestController
@Profile("external-publisher")
public class ExternalOrderPublisherController {

    private static final Logger logger = LoggerFactory.getLogger(ExternalOrderPublisherController.class);
    private static final long QUOTE_TTL_MS = 5 * 60_000L;

    private final String exchangeBaseUrl;
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpClient http;

    public ExternalOrderPublisherController(
            @Value("${exchange.base-url:http://exchange:8080}") String exchangeBaseUrl) {
        this.exchangeBaseUrl = exchangeBaseUrl;
    }

    @PostConstruct
    void init() {
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Seed one quote per symbol in the exchange. Deterministic prices
     * (bid=99.50, ask=100.50, qty=1000 each side) so (a) orders at 99/101
     * always cross and (b) the reservation service has plenty of capacity.
     */
    @PostMapping("/publisher/seed-quotes")
    public ResponseEntity<List<UUID>> seedQuotes(@RequestBody List<String> symbols) throws Exception {
        List<UUID> ids = new ArrayList<>();
        for (String symbol : symbols) {
            UUID quoteId = UUID.randomUUID();
            Quote quote = new Quote(
                    symbol,
                    99.50, 1000,
                    100.50, 1000,
                    quoteId,
                    System.currentTimeMillis() + QUOTE_TTL_MS);
            String body = mapper.writeValueAsString(quote);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(exchangeBaseUrl + "/quotes/" + symbol))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new IllegalStateException(
                        "PUT /quotes/" + symbol + " failed: " + resp.statusCode() + " " + resp.body());
            }
            ids.add(quoteId);
            logger.info("Seeded quote for {} with id {}", symbol, quoteId);
        }
        return ResponseEntity.ok(ids);
    }

    /**
     * Submit {@code count} orders per symbol. Alternating sides with limit
     * prices that cross both the bootstrap ({@code 99.50/100.50}) and the
     * tighter ({@code 99.95/100.05}) production quote.
     * @return number of orders the exchange accepted (HTTP 200); non-200
     *         responses are tallied separately in the logs but not counted,
     *         since stale quotes / empty residual depth legitimately reject.
     */
    @PostMapping("/publisher/submit-orders")
    public ResponseEntity<Integer> submitOrders(
            @RequestParam int count,
            @RequestBody List<String> symbols) {
        int accepted = 0;
        int rejected = 0;
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            for (String symbol : symbols) {
                Side side = (i + symbol.hashCode()) % 2 == 0 ? Side.BUY : Side.SELL;
                double limitPrice = side == Side.BUY ? 101.0 : 99.0;
                int quantity = 1 + rnd.nextInt(3);
                ExternalOrder order = new ExternalOrder(UUID.randomUUID(), symbol, quantity, limitPrice, side);
                try {
                    String body = mapper.writeValueAsString(order);
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create(exchangeBaseUrl + "/orders"))
                            .header("Content-Type", "application/json")
                            .timeout(Duration.ofSeconds(10))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
                    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() == 200) {
                        accepted++;
                    } else {
                        rejected++;
                    }
                } catch (Exception e) {
                    rejected++;
                    logger.debug("order submit failed for {}: {}", symbol, e.getMessage());
                }
            }
        }
        logger.info("Submitted orders: accepted={}, rejected={}", accepted, rejected);
        return ResponseEntity.ok(accepted);
    }

    @GetMapping("/health")
    public ServiceHealth getHealth() {
        return new ServiceHealth(true, 0, "External Order Publisher");
    }
}
