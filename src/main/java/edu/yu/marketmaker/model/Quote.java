package edu.yu.marketmaker.model;

import java.io.Serializable;
import java.util.UUID;

public record Quote(String symbol, double bidPrice, int bidQuantity, double askPrice, int askQuantity, UUID quoteId, long expiresAt) implements Identifiable<String>, Serializable {

    @Override
    @com.fasterxml.jackson.annotation.JsonIgnore
    public String getId() {
        return symbol;
    }
}
