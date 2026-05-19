package edu.yu.marketmaker.marketmaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only fault injector for error cases 5 and 10.
 *
 * <p>Two independent arming slots, each consumed at a different point inside
 * {@link ProductionQuoteGenerator#generateQuote}:
 * <ul>
 *   <li><b>quote-replace</b> ({@link #armQuoteReplaceCrash} /
 *       {@link #consumeIfArmed}) — fires at the top of generateQuote, before
 *       any reservation is acquired. {@link ProductionQuoteGenerator} then
 *       explicitly releases the previous reservation and halts the JVM,
 *       reproducing error case 10.</li>
 *   <li><b>post-reservation</b> ({@link #armPostReservationCrash} /
 *       {@link #consumeIfArmedPostReservation}) — fires after the
 *       reservation is granted but before the quote is written to the
 *       repository. The reservation stays in place (no release), reproducing
 *       error case 5's exposure leak.</li>
 * </ul>
 *
 * <p>This bean only exists when the {@code fault-injection} Spring profile is
 * active. {@link ProductionQuoteGenerator} consumes it via optional injection;
 * when the profile is absent (the default) the field is {@code null} and the
 * production code path is unaffected.
 *
 * <p>Even with the profile active, both slots stay disarmed until an explicit
 * arm call comes in via {@link FaultInjectionController}. Each slot is
 * single-shot: a successful consume clears it.
 *
 * <p>Two independent safety gates therefore stand between this code and a
 * production deployment:
 * <ol>
 *   <li>The {@code fault-injection} profile must be in
 *       {@code SPRING_PROFILES_ACTIVE}.</li>
 *   <li>An operator must POST to one of the arm endpoints with a specific
 *       symbol.</li>
 * </ol>
 */
@Component
@Profile("fault-injection")
public class FaultInjector {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final AtomicReference<String> armedSymbol = new AtomicReference<>(null);
    private final AtomicReference<String> postReservationArmedSymbol = new AtomicReference<>(null);

    /**
     * Arm the injector to crash the next time the quote generator processes
     * a replacement cycle for {@code symbol}. Overwrites any prior armed
     * symbol; passing {@code null} disarms.
     */
    public void armQuoteReplaceCrash(String symbol) {
        String previous = armedSymbol.getAndSet(symbol);
        if (previous != null) {
            log.warn("[FAULT-INJECTION] re-arm: overwriting previously armed symbol {} with {}",
                    previous, symbol);
        } else {
            log.warn("[FAULT-INJECTION] armed: will crash on next quote-replace cycle for symbol={}",
                    symbol);
        }
    }

    /**
     * Arm the injector to crash AFTER the next successful reservation grant
     * for {@code symbol} but BEFORE the resulting quote is written to the
     * repository. Reproduces error case 5: reservation capacity is consumed
     * but the quote never becomes active. Overwrites any prior armed symbol;
     * passing {@code null} disarms.
     */
    public void armPostReservationCrash(String symbol) {
        String previous = postReservationArmedSymbol.getAndSet(symbol);
        if (previous != null) {
            log.warn("[FAULT-INJECTION] re-arm (post-reservation): overwriting previously armed symbol {} with {}",
                    previous, symbol);
        } else {
            log.warn("[FAULT-INJECTION] armed (post-reservation): will crash after next reservation grant for symbol={}",
                    symbol);
        }
    }

    /** @return the currently armed quote-replace symbol, or {@code null} if disarmed. */
    public String currentlyArmedSymbol() {
        return armedSymbol.get();
    }

    /** @return the currently armed post-reservation symbol, or {@code null} if disarmed. */
    public String currentlyArmedPostReservationSymbol() {
        return postReservationArmedSymbol.get();
    }

    /**
     * Consume the quote-replace armed flag if it matches {@code symbol}.
     *
     * <p>Returns {@code true} (and clears the armed state) only when the
     * injector was armed for exactly this symbol. After a successful consume
     * the caller is expected to release the old reservation and halt the JVM
     * — see {@link ProductionQuoteGenerator}.
     */
    public synchronized boolean consumeIfArmed(String symbol) {
        return consumeIfArmed(armedSymbol, symbol);
    }

    /**
     * Consume the post-reservation armed flag if it matches {@code symbol}.
     *
     * <p>Returns {@code true} (and clears the armed state) only when the
     * injector was armed for exactly this symbol. After a successful consume
     * the caller is expected to halt the JVM WITHOUT releasing the
     * just-granted reservation — that orphan is the whole point of error
     * case 5.
     */
    public synchronized boolean consumeIfArmedPostReservation(String symbol) {
        return consumeIfArmed(postReservationArmedSymbol, symbol);
    }

    // Value comparison, not reference: AtomicReference.compareAndSet uses ==
    // under the hood, which fails for equal-but-distinct String instances
    // (the symbol from HTTP query-string vs. the one from a deserialized
    // Position payload). equals() is what we actually want.
    private static boolean consumeIfArmed(AtomicReference<String> slot, String symbol) {
        if (symbol == null) {
            return false;
        }
        if (symbol.equals(slot.get())) {
            slot.set(null);
            return true;
        }
        return false;
    }
}