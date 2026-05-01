package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.model.StateSnapshot;
import reactor.core.publisher.Flux;

import java.util.Set;

public interface SnapshotTracker {

    Flux<StateSnapshot> getPositions();

    boolean addSymbol(String symbol);

    boolean removeSymbol(String symbol);

    boolean handlesSymbol(String symbol);

    /** @return a snapshot of the symbols this tracker currently handles. */
    default Set<String> handledSymbols() {
        return Set.of();
    }
}
