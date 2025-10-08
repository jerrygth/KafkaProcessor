package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WeatherSensorCorrelation {
    private final String sensorId;
    private final double sensorTemperature;
    private final double weatherTemperature;
    private final long timestamp;

    public WeatherSensorCorrelation(String sensorId, double sensorTemperature, double weatherTemperature, long timestamp) {
        this.sensorId = sensorId;
        this.sensorTemperature = sensorTemperature;
        this.weatherTemperature = weatherTemperature;
        this.timestamp = timestamp;
    }

    public boolean hasSignificantDeviation() {
        return Math.abs(sensorTemperature - weatherTemperature) > 10.0; // 10 degree difference
    }

    public double getDeviation() {
        return Math.abs(sensorTemperature - weatherTemperature);
    }

    // Getters
    public String getSensorId() { return sensorId; }
    public double getSensorTemperature() { return sensorTemperature; }
    public double getWeatherTemperature() { return weatherTemperature; }
    public long getTimestamp() { return timestamp; }
}