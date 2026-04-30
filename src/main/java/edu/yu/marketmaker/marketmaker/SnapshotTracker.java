package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.model.StateSnapshot;
import reactor.core.publisher.Flux;

public interface SnapshotTracker {

    Flux<StateSnapshot> getPositions();

    boolean addSymbol(String symbol);

    boolean removeSymbol(String symbol);

    boolean handlesSymbol(String symbol);
}
