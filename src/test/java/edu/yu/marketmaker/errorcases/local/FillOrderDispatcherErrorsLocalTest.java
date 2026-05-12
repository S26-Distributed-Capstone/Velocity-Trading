package edu.yu.marketmaker.errorcases.local;

import edu.yu.marketmaker.exchange.FillOrderDispatcher;
import edu.yu.marketmaker.exchange.FillSender;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Local (no docker / no cluster) unit-level test for the
 * <b>Error Case 3</b> contract at the {@link FillOrderDispatcher} layer.
 *
 * <p>Case 3 (from {@code docs/error-cases.md}): the trading-state service goes
 * down before recording a fill. The RSocket send is fire-and-forget, so the
 * fill is silently lost — but <em>the exchange's quote has already been
 * decremented</em>. That's the documented "critical inconsistency": the
 * quote and the position are out of sync.
 *
 * <p>This test exercises the dispatcher seam directly. It uses the
 * backward-compatible two-arg constructor
 * {@code FillOrderDispatcher(Repository, FillSender)} (no reservation
 * requester, so the reservation release step is skipped — see the constructor
 * comment in {@code FillOrderDispatcher}).
 *
 * <p>Companion HTTP-layer tests for Cases 1 and 2 live in
 * {@link SubmittingExternalOrderErrorsLocalTest}.
 */
class FillOrderDispatcherErrorsLocalTest {

    private Repository<String, Quote> quoteRepository;
    private FillSender fillSender;
    private FillOrderDispatcher dispatcher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        quoteRepository = mock(Repository.class);
        fillSender = mock(FillSender.class);
        dispatcher = new FillOrderDispatcher(quoteRepository, fillSender);
    }

    /**
     * When {@link FillSender#sendFill} throws (the unit-level analogue of the
     * trading-state container being down), the dispatcher's <em>preceding</em>
     * call to {@code quoteRepository.put} with a decremented quote has already
     * happened — proving the documented Case 3 claim that the quote is
     * decremented even though the fill is lost.
     *
     * <p>The current dispatcher implementation propagates the throw; the
     * production {@code RSocketFillSender} hides this via fire-and-forget at
     * the RSocket transport layer. Either way, the central invariant the
     * docs assert — "quote was decremented" — is observable here as a
     * Mockito {@link InOrder} verification.
     */
    @Test
    void fillSendFailure_QuoteAlreadyDecremented_DocumentingCase3Inconsistency() {
        Quote quote = new Quote(
                "AAPL",
                /* bidPrice */ 99.0, /* bidQuantity */ 10,
                /* askPrice */ 101.0, /* askQuantity */ 10,
                UUID.randomUUID(),
                System.currentTimeMillis() + 30_000);
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(quote));

        doThrow(new RuntimeException("rsocket dead — trading-state unreachable"))
                .when(fillSender).sendFill(any(Fill.class));

        ExternalOrder buyFive = new ExternalOrder(UUID.randomUUID(), "AAPL", 5, 101.0, Side.BUY);

        assertThrows(RuntimeException.class, () -> dispatcher.dispatchOrder(buyFive),
                "Dispatcher propagates the throw at the unit level; production hides this via"
                        + " RSocket fire-and-forget. This test pins the invariant that matters"
                        + " regardless of how the failure is handled: the quote was already"
                        + " decremented before the send attempt.");

        // Critical assertion: the decremented quote was put into the repository
        // BEFORE fillSender.sendFill was invoked. This proves the documented
        // "quote was already decremented but fill was lost" inconsistency.
        ArgumentCaptor<Quote> updatedQuote = ArgumentCaptor.forClass(Quote.class);
        InOrder order = inOrder(quoteRepository, fillSender);
        order.verify(quoteRepository).put(updatedQuote.capture());
        order.verify(fillSender).sendFill(any(Fill.class));

        Quote afterDecrement = updatedQuote.getValue();
        assertEquals("AAPL", afterDecrement.symbol(), "decremented quote must be for AAPL");
        assertEquals(5, afterDecrement.askQuantity(),
                "askQuantity must decrement from 10 to 5 BEFORE fillSender.sendFill —"
                        + " the documented Case 3 inconsistency: quote decremented, fill lost");
        assertEquals(10, afterDecrement.bidQuantity(),
                "bidQuantity must be unaffected by a BUY order");
        assertEquals(quote.quoteId(), afterDecrement.quoteId(),
                "quoteId must be preserved across the decrement");
    }
}
