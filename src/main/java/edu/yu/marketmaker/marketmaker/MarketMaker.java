package edu.yu.marketmaker.marketmaker;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.StateSnapshot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Profile("market-maker-node")
public class MarketMaker implements ApplicationRunner {

    private final SnapshotTracker positionTracker;
    private final QuoteGenerator quoteGenerator;
    private final Map<String, Long> lastProcessedVersionBySymbol = new ConcurrentHashMap<>();

    public MarketMaker(SnapshotTracker positionTracker, QuoteGenerator quoteGenerator) {
        this.positionTracker = positionTracker;
        this.quoteGenerator = quoteGenerator;
    }

    private void handlePosition(StateSnapshot snapshot) {
        if (snapshot == null || snapshot.position() == null || snapshot.position().symbol() == null) {
            return;
        }
        if (!positionTracker.handlesSymbol(snapshot.position().symbol()) || !newVersion(snapshot.position())) {
            return;
        }
        quoteGenerator.generateQuote(snapshot.position(), snapshot.fill());
    }

    private boolean newVersion(Position position) {
        Long previous = lastProcessedVersionBySymbol.put(position.symbol(), position.version());
        return previous == null || position.version() > previous;
    }

    public boolean addSymbol(String symbol) {
        return positionTracker.addSymbol(symbol);
    }

    public boolean removeSymbol(String symbol) {
        return positionTracker.removeSymbol(symbol);
    }

    @Override
    public void run(ApplicationArguments args) {
        // Subscribe once at startup so incoming snapshots are continuously processed.
        positionTracker.getPositions().subscribe(this::handlePosition);
    }
}
