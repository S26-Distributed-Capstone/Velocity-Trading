package edu.yu.marketmaker.exchange;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.ReservationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("exchange")
public class ReservationRequester {

    private static final Logger logger = LoggerFactory.getLogger(ReservationRequester.class);

    private final LeaderAwareRSocketClient client;

    public ReservationRequester(LeaderAwareRSocketClient client) {
        this.client = client;
    }

    public void sendReservation(Quote quote) {
        logger.info("Sending initial reservation for: {}", quote.symbol());
        client.requestResponse("exposure-reservation", "reservations", quote, ReservationResponse.class)
                .block();
    }
}
