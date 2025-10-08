package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorStatistics {
    private double sum = 0.0;
    private double sumSquares = 0.0;
    private int count = 0;
    private double currentValue = 0.0;

    public SensorStatistics addReading(double value) {
        this.currentValue = value;
        this.sum += value;
        this.sumSquares += value * value;
        this.count++;
        return this;
    }

    public boolean hasAnomaly() {
        return Math.abs(getZScore()) > 2.5; // 2.5 standard deviations
    }

    public double getMean() {
        return count > 0 ? sum / count : 0.0;
    }

    public double getStandardDeviation() {
        if (count < 2) return 0.0;
        double mean = getMean();
        return Math.sqrt((sumSquares - 2 * mean * sum + count * mean * mean) / (count - 1));
    }

    public double getZScore() {
        double std = getStandardDeviation();
        return std != 0 ? (currentValue - getMean()) / std : 0.0;
    }

    // Getters
    public double getCurrentValue() { return currentValue; }
}