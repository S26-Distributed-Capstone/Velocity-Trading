package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.model.Fill;
import edu.yu.marketmaker.model.Position;
import edu.yu.marketmaker.model.Quote;

public interface QuoteGenerator {
    
    Quote generateQuote(Position position, Fill lastFill);
}
