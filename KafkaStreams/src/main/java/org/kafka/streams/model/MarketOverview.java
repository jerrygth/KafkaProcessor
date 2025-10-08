package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.kafka.producer.model.protobuf.StockPriceProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketOverview {
    private long totalVolume = 0;
    private double totalValue = 0;
    private int symbolCount = 0;
    private List<String> symbols = new ArrayList<>();
    private Map<String, Double> prices = new HashMap<>();

    public MarketOverview updateWithStock(StockPriceProto.StockPrice stock) {
        totalVolume += stock.getVolume();
        totalValue += stock.getPrice() * stock.getVolume();

        if (!symbols.contains(stock.getSymbol())) {
            symbols.add(stock.getSymbol());
            symbolCount++;
        }
        prices.put(stock.getSymbol(), stock.getPrice());
        return this;
    }

    public double getAveragePrice() {
        return symbolCount > 0 ? totalValue / totalVolume : 0;
    }

    public List<String> getTopGainers() {
        return symbols.subList(0, Math.min(3, symbols.size()));
    }

    public List<String> getTopLosers() {
        return symbols.subList(Math.max(0, symbols.size() - 3), symbols.size());
    }

    public String getMarketTrend() {
        return "NEUTRAL";
    }

    // Getters
    public long getTotalVolume() { return totalVolume; }
    public int getActiveSymbols() { return symbolCount; }
}
