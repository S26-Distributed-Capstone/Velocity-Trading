package edu.yu.marketmaker.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("fault-injection")
public class FaultInjector {
    
    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final ConcurrentMap<Event, String> armedSymbols = new ConcurrentHashMap<>();

    public void armSymbol(Event event, String symbol) {
        String previous = armedSymbols.put(event, symbol);
        if (previous != null) {
            log.warn("[FAULT-INJECTION] re-arm: overwriting previously armed symbol {} with {} for event {}",
                    previous, symbol, event);
        } else {
            log.warn("[FAULT-INJECTION] armed: will crash on next {} for symbol={}",
                    event, symbol);
        }
    }

    public void triggerFault(Event event, String symbol) {
        log.warn("[FAULT-INJECTION] checking fault for event: {}, on symbol {}", event, symbol);
        if (symbol == null) {
            return;
        }
        if (armedSymbols.get(event).equals(symbol)) {
            log.error("[FAULT-INJECTION] event {}: halting JVM", event);
            Runtime.getRuntime().halt(137);
        }
    }

    public enum Event {
        PROCESS_ORDER, APPLY_FILL
    }
}
