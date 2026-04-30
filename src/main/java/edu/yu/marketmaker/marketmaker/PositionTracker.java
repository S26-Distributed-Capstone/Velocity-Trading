package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.model.StateSnapshot;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Profile("market-maker-node & !test-position-tracker")
public class PositionTracker implements SnapshotTracker {

    private final Set<String> trackedSymbols = ConcurrentHashMap.newKeySet();

    private final LeaderAwareRSocketClient client;

    public PositionTracker(LeaderAwareRSocketClient client) {
        this.client = client;
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
    public Flux<StateSnapshot> getPositions() {
        // .repeatWhen reconnects on leader change: if the current leader dies,
        // the stream errors, we resume with empty, then repeat (which re-resolves
        // the leader via the registry cache). Filter to only symbols this node tracks.
        return client.requestStream("trading-state", "state.stream", StateSnapshot.class)
                .filter(Objects::nonNull)
                .filter(snapshot -> snapshot.position() != null)
                .filter(snapshot -> snapshot.position().symbol() != null)
                .filter(snapshot -> handlesSymbol(snapshot.position().symbol()))
                .onErrorResume(e -> Flux.empty())
                .repeatWhen(signals -> signals.delayElements(Duration.ofSeconds(2)));
    }
}
