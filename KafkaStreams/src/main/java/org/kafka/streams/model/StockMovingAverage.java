package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StockMovingAverage {
    private String symbol;
    private double currentAverage;
    private double previousAverage;
    private int dataPoints;

    public StockMovingAverage updateAverage(double price, long timestamp) {
        this.previousAverage = this.currentAverage;
        this.currentAverage = ((currentAverage * dataPoints) + price) / (dataPoints + 1);
        this.dataPoints++;
        return this;
    }

    public boolean hasSignificantChange() {
        return Math.abs(currentAverage - previousAverage) / previousAverage > 0.05; // 5% change
    }

    public double getChangePercent() {
        return previousAverage != 0 ? ((currentAverage - previousAverage) / previousAverage) * 100 : 0;
    }

    // Getters
    public String getSymbol() { return symbol; }
    public double getCurrentAverage() { return currentAverage; }
    public double getPreviousAverage() { return previousAverage; }
}
