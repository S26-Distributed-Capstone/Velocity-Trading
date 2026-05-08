package edu.yu.marketmaker.exchange;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.Quote;

@Component
@Profile("testing")
public class StaticQuoteRepository implements Repository<String, Quote> {

    private final Map<String, Quote> map = new ConcurrentHashMap<>();

    public StaticQuoteRepository() {
        this.map.putAll(generateQuotes(new Random(1234)));
    }

    private static Map<String, Quote> generateQuotes(Random random) {
        String[] symbols = new String[]{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ABC", "DEF", "XYZ"};
        Map<String, Quote> quotes = new ConcurrentHashMap<>();
        for (String symbol : symbols) {
            quotes.put(symbol, new Quote(symbol,
                    random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
                    UUID.randomUUID(), random.nextInt(1000)
            ));
        }
        return quotes;
    }

    @Override
    public Optional<Quote> get(String id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public void put(Quote entity) {
        map.put(entity.getId(), entity);
    }

    @Override
    public Collection<Quote> getAll() {
        return map.values();
    }

    @Override
    public void delete(String id) {
        map.remove(id);
    }
    
}
