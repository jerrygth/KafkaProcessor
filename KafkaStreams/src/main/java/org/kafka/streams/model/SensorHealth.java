package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorHealth {
    private final String sensorId;
    private final int batteryLevel;
    private final long lastSeen;

    public SensorHealth(String sensorId, int batteryLevel, long lastSeen) {
        this.sensorId = sensorId;
        this.batteryLevel = batteryLevel;
        this.lastSeen = lastSeen;
    }

    public boolean needsAttention() {
        return batteryLevel < 20 || (System.currentTimeMillis() - lastSeen) > 300000; // 5 minutes
    }

    public String getMaintenanceType() {
        if (batteryLevel < 20) return "LOW_BATTERY";
        if ((System.currentTimeMillis() - lastSeen) > 300000) return "OFFLINE";
        return "UNKNOWN";
    }

    // Getters
    public String getSensorId() { return sensorId; }
    public int getBatteryLevel() { return batteryLevel; }
    public long getLastSeen() { return lastSeen; }
}
