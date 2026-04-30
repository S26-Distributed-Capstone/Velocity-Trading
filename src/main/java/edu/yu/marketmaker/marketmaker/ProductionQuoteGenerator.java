package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.ReservationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Primary;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Primary
@Profile("production-quote-generator")
public class ProductionQuoteGenerator implements QuoteGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ProductionQuoteGenerator.class);

    private final RSocketRequester reservationRequester;
    private final int defaultQuantity;
    private final double targetSpread;
    private final Repository<String, Quote> quoteRepository;

    /**
     * Constructor for production quote generator. 
     * @param rsocketRequesterBuilder
     * @param quoteRepository
     * @param reservationHost
     * @param reservationPort
     * @param defaultQuantity
     * @param targetSpread
     */
    public ProductionQuoteGenerator(
            RSocketRequester.Builder rsocketRequesterBuilder,
            Repository<String, Quote> quoteRepository,
            @Value("${marketmaker.exposure-reservation.host:exposure-reservation}") String reservationHost,
            @Value("${marketmaker.exposure-reservation.port:7000}") int reservationPort,
            @Value("${marketmaker.default-quote-quantity:10}") int defaultQuantity,
            @Value("${marketmaker.target-spread:0.10}") double targetSpread
    ) {
        this.reservationRequester = rsocketRequesterBuilder.tcp(reservationHost, reservationPort);
        this.defaultQuantity = defaultQuantity;
        this.targetSpread = targetSpread;
        this.quoteRepository = quoteRepository;
    }

    /**
     * Generates a quote based on the current position and last fill.
     * Only thread safe assuming one market maker instance per symbol, 
     * as the quote repository is shared and not synchronized. 
     * In a real implementation, we would need to ensure thread safety 
     * at the repository level or use a more sophisticated state management approach.
     *
     * @param position The current position.
     * @param lastFill The last fill.
     * @return The generated quote.
     */
    @Override
    public Quote generateQuote(Position position, Fill lastFill) {
        String symbol = lastFill != null ? lastFill.symbol() : position.symbol();
        Quote current = quoteRepository.get(symbol).orElse(null);

        double referencePrice = current != null ? midPrice(current) : (lastFill != null ? lastFill.price() : 100.0);
        double halfSpread = targetSpread / 2.0;
        int bidQuantity = current != null ? Math.max(0, current.bidQuantity()) : Math.max(1, defaultQuantity);
        int askQuantity = current != null ? Math.max(0, current.askQuantity()) : Math.max(1, defaultQuantity);

        // Simple inventory-aware skew based on the last fill side.
        if (lastFill != null) {
            switch (lastFill.side()) {
                case SELL -> { // raise price
                    referencePrice += 0.01 * lastFill.quantity();
                    askQuantity += 2;
                    bidQuantity = Math.max(0, bidQuantity - 1);
                }
                case BUY -> { // lower price
                    referencePrice -= 0.01 * lastFill.quantity();
                    askQuantity = Math.max(0, askQuantity - 1);
                    bidQuantity += 2;
                }
            }
        }

        // --- NEW LOGIC: Enforce Individual Position Limits (±100) ---
        int maxAllowedBid = Math.max(0, 100 - position.netQuantity());
        bidQuantity = Math.min(bidQuantity, maxAllowedBid);

        int maxAllowedAsk = Math.max(0, 100 + position.netQuantity());
        askQuantity = Math.min(askQuantity, maxAllowedAsk);
        // -----------------------------------------------------------

        double bidPrice = Math.max(0.01, referencePrice - halfSpread);
        double askPrice = Math.max(bidPrice, referencePrice + halfSpread);

        Quote proposed = new Quote(
                symbol,
                bidPrice,
                bidQuantity,
                askPrice,
                askQuantity,
                UUID.randomUUID(),
                System.currentTimeMillis() + 30_000
        );

        ReservationResponse reservation = reservationRequester
                .route("reservations")
                .data(proposed)
                .retrieveMono(ReservationResponse.class)
                .block();

        if (reservation == null) {
            throw new IllegalStateException("Exposure reservation service returned no response");
        }

        Quote reservedQuote = new Quote(
                proposed.symbol(),
                proposed.bidPrice(),
                reservation.grantedBidQuantity(),
                proposed.askPrice(),
                reservation.grantedAskQuantity(),
                proposed.quoteId(),
                proposed.expiresAt()
        );

        quoteRepository.put(reservedQuote);
        logger.info(
                "Generated reserved quote: symbol={}, bidQty={}, askQty={}, reservationStatus={}",
                reservedQuote.symbol(),
                reservedQuote.bidQuantity(),
                reservedQuote.askQuantity(),
                reservation.status()
        );

        return reservedQuote;
    }

    private double midPrice(Quote quote) {
        return (quote.bidPrice() + quote.askPrice()) / 2.0;
    }
}
