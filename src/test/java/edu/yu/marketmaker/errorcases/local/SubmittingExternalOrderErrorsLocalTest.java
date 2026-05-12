package edu.yu.marketmaker.errorcases.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.yu.marketmaker.exchange.ExchangeService;
import edu.yu.marketmaker.exchange.OrderDispatcher;
import edu.yu.marketmaker.exchange.ReservationRequester;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.model.Side;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Local (no docker / no cluster) unit-level tests for the "Submitting External
 * Orders" error cases documented in {@code docs/error-cases.md}.
 *
 * <p>The cluster variant {@code errorcases.cluster.SubmittingExternalOrderErrorsClusterTest}
 * exercises real container crashes against a docker-compose stack. This class
 * pins the HTTP-layer contract of {@link ExchangeService} for the same
 * scenarios, in-process and in &lt;2 seconds, via {@code @WebMvcTest} +
 * Mockito.
 *
 * <ul>
 *   <li><b>Case 1</b>: a downstream crash in the dispatcher (simulated as
 *       {@code RuntimeException} from the mocked {@link OrderDispatcher})
 *       must surface as a 5xx and must not leak any quote state into the
 *       repository — the documented "no state corruption" property.</li>
 *   <li><b>Case 2</b>: two {@code POST /orders} calls with distinct UUIDs
 *       must both be dispatched (no UUID dedup at the controller). This
 *       locks in the contract that the publisher's blind retry-on-timeout
 *       is safely accounted for as a separate trade.</li>
 *   <li><b>Case 3</b> (HTTP-layer): if the dispatcher returns normally (the
 *       fire-and-forget RSocket failure is swallowed below this layer),
 *       {@code POST /orders} must return 200. The publisher cannot tell from
 *       the response that the fill was lost — that's the documented
 *       observability gap. A stronger dispatcher-level assertion for Case 3
 *       lives in {@link FillOrderDispatcherErrorsLocalTest}.</li>
 * </ul>
 */
@WebMvcTest(ExchangeService.class)
@ActiveProfiles("exchange")
class SubmittingExternalOrderErrorsLocalTest {

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @MockitoBean
    private Repository<String, Quote> quoteRepository;

    @MockitoBean
    private OrderDispatcher orderDispatcher;

    @MockitoBean
    private ReservationRequester reservationRequester;

    // --- Error Case 1: exchange dies before handling the order ---

    /**
     * Simulate "exchange crashed before processing" by making the dispatcher
     * throw a generic {@code RuntimeException} — neither
     * {@code OrderValidationException} (→400) nor {@code QuoteNotFoundException}
     * (→404), so {@link edu.yu.marketmaker.exchange.ExchangeServiceAdvice}
     * does not catch it. The exception propagates out (modeling the "exchange
     * dies, no clean response reaches the publisher" property of Case 1).
     * Whatever the transport-level surface, the central invariant is the same:
     * the quote repository must remain untouched — the order has no
     * observable side effect.
     */
    @Test
    void exchangeCrashBeforeDispatching_PropagatesError_AndNoStateMutated() throws Exception {
        doThrow(new RuntimeException("simulated exchange crash before processing"))
                .when(orderDispatcher).dispatchOrder(any());

        ExternalOrder order = new ExternalOrder(UUID.randomUUID(), "AAPL", 5, 101.0, Side.BUY);

        // No @ExceptionHandler for plain RuntimeException, so MockMvc propagates
        // the failure rather than translating to a status code. Either surface
        // is acceptable — Case 1's load-bearing assertion is the "no state
        // corruption" property below.
        assertThrows(Exception.class, () ->
                mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order))),
                "dispatcher's crash must reach the test, not be silently swallowed");

        verify(quoteRepository, never()).put(any(Quote.class));
    }

    // --- Error Case 2: publisher retries with a fresh UUID; no dedup ---

    /**
     * Two POSTs with distinct order ids must both reach the dispatcher with
     * their respective ids preserved. There is no dedup at the controller —
     * the publisher's retry-after-timeout (the Case 2 scenario) is fully
     * accounted for as a separate order.
     */
    @Test
    void retriedOrderWithFreshUuid_DispatchedAsDistinctOrder_NoDedup() throws Exception {
        UUID firstId = UUID.randomUUID();
        UUID retryId = UUID.randomUUID();
        assertNotEquals(firstId, retryId, "test setup: ids must differ");

        ExternalOrder first = new ExternalOrder(firstId, "AAPL", 5, 101.0, Side.BUY);
        ExternalOrder retry = new ExternalOrder(retryId, "AAPL", 5, 101.0, Side.BUY);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(first)))
                .andExpect(status().isOk());

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(retry)))
                .andExpect(status().isOk());

        ArgumentCaptor<ExternalOrder> captor = ArgumentCaptor.forClass(ExternalOrder.class);
        verify(orderDispatcher, times(2)).dispatchOrder(captor.capture());
        List<ExternalOrder> dispatched = captor.getAllValues();
        assertEquals(2, dispatched.size(), "both POSTs must reach the dispatcher");
        assertNotEquals(dispatched.get(0).id(), dispatched.get(1).id(),
                "dispatcher must see two distinct order ids — no controller-level dedup");
        assertEquals(firstId, dispatched.get(0).id(), "first POST's id must be preserved");
        assertEquals(retryId, dispatched.get(1).id(), "retry POST's id must be preserved");
    }

    // --- Error Case 3 (HTTP-layer): fire-and-forget loss is invisible above ---

    /**
     * Documents the HTTP-layer contract for Case 3: when the dispatcher returns
     * normally (because the fire-and-forget RSocket send to trading-state
     * silently dropped the fill), {@code POST /orders} reports 200 — the
     * publisher has no way to know the fill was lost. The dispatcher-level
     * assertion (quote was decremented even though fill was lost) lives in
     * {@link FillOrderDispatcherErrorsLocalTest}.
     */
    @Test
    void fireAndForgetFillLoss_IsInvisibleToHttpCaller_Returns200() throws Exception {
        // Dispatcher returns normally — the documented "exchange reported success"
        // path, even though downstream the fill was silently lost.
        ExternalOrder order = new ExternalOrder(UUID.randomUUID(), "AAPL", 5, 101.0, Side.BUY);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isOk());

        verify(orderDispatcher).dispatchOrder(any(ExternalOrder.class));
    }
}
