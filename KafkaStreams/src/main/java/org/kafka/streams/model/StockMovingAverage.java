package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
        isGetterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
        setterVisibility = JsonAutoDetect.Visibility.NONE
)
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
        if(previousAverage == 0) {
            return false; // Avoid division by zero
        }
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
