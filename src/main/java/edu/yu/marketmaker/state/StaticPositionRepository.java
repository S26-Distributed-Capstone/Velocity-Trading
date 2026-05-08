package edu.yu.marketmaker.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import edu.yu.marketmaker.memory.Repository;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.marketmaker.model.Position;

/**
 * Static position repository for testing.
 */
@Component
@Profile("testing")
public class StaticPositionRepository implements Repository<String, Position> {

    private final Map<String, Position> map;

    public StaticPositionRepository(Map<String, Position> map) {
        this.map = new HashMap<>(map);
    }

    public StaticPositionRepository() {
        this(generatePositions(new Random(5678)));
    }

    @Override
    public Optional<Position> get(String id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public void put(Position position) {
        map.put(position.symbol(), position);
    }

    @Override
    public java.util.Collection<Position> getAll() {
        return map.values();
    }

    @Override
    public void delete(String id) {
        map.remove(id);
    }

    private static Map<String, Position> generatePositions(Random random) {
        String[] symbols = new String[]{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ABC", "DEF", "XYZ"};
        Map<String, Position> positions = new HashMap<>();
        for (String symbol : symbols) {
            positions.put(symbol, new Position(
                    symbol,
                    random.nextInt(201) - 100, // netQuantity: -100 to +100
                    0L, // initial version
                    new UUID(0, 0)  // initial lastFillId
            ));
        }
        return positions;
    }
}
