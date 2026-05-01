package edu.yu.marketmaker.marketmaker;

import org.springframework.context.annotation.Profile;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;

import edu.yu.marketmaker.model.StateSnapshot;
import reactor.core.publisher.Flux;

import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Profile("!test-position-tracker")
public class PositionTracker implements SnapshotTracker {

    // Thread-safe set of symbols we're tracking. Use ConcurrentHashMap's keySet for efficiency.
    private final Set<String> trackedSymbols = ConcurrentHashMap.newKeySet();

    private final RSocketRequester requester;

    public PositionTracker(RSocketRequester.Builder rsocketRequesterBuilder) {
        this.requester = rsocketRequesterBuilder.tcp("trading-state", 7000);
    }

    @Override
    public boolean addSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.add(symbol);
    }

    @Override
    public boolean removeSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.remove(symbol);
    }

    @Override
    public boolean handlesSymbol(String symbol) {
        if (symbol == null) return false;
        return trackedSymbols.contains(symbol);
    }

    @Override
    public Set<String> handledSymbols() {
        return Set.copyOf(trackedSymbols);
    }

    public Flux<StateSnapshot> getPositions() {
        // Only forward snapshots for symbols we're tracking. Guard against nulls in the stream.
        return requester
                .route("state.stream")
                .retrieveFlux(StateSnapshot.class)
                .filter(Objects::nonNull)
                .filter(snapshot -> snapshot.position() != null)
                .filter(snapshot -> snapshot.position().symbol() != null)
                .filter(snapshot -> handlesSymbol(snapshot.position().symbol()));
    }
}