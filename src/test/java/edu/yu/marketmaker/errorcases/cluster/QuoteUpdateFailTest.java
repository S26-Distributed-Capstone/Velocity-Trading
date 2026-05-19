package edu.yu.marketmaker.errorcases.cluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.service.FaultInjector;

public class QuoteUpdateFailTest {
    
    /**
     * Test for error case #7
     */
    @Test
    void quoteUpdateFails() throws Exception {
        Common.seedQuotes(List.of("test", "next", "other"));
        Common.armFaultInjector(FaultInjector.Event.PROCESS_ORDER, "test");
        Common.submitOrdersViaPublisher(1, List.of("test"));
        Thread.sleep(20000);
        Quote quote = Common.currentExchangeQuote("test");
        System.out.println(quote.askQuantity());
        System.out.println(quote.bidQuantity());
        assertTrue(quote.askQuantity() < 1000 || quote.bidQuantity() < 1000);
    }
}
