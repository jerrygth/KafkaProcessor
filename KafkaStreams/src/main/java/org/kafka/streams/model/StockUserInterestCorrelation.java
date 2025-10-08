package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StockUserInterestCorrelation {
    private final String symbol;
    private final double stockPrice;
    private final String userId;
    private final long userEventTimestamp;
    private final long stockTimestamp;

    public StockUserInterestCorrelation(String symbol, double stockPrice, String userId, long userEventTimestamp, long stockTimestamp) {
        this.symbol = symbol;
        this.stockPrice = stockPrice;
        this.userId = userId;
        this.userEventTimestamp = userEventTimestamp;
        this.stockTimestamp = stockTimestamp;
    }

    public long getTimeLag() {
        return Math.abs(stockTimestamp - userEventTimestamp);
    }

    // Getters
    public String getSymbol() { return symbol; }
    public double getStockPrice() { return stockPrice; }
    public String getUserId() { return userId; }
}
