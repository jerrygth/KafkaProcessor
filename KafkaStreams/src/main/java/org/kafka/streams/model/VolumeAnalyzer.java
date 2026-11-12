package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
        isGetterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
        setterVisibility = JsonAutoDetect.Visibility.NONE
)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolumeAnalyzer {
    private String symbol;
    private long currentVolume;
    private double averageVolume;
    private int dataPoints;

    public VolumeAnalyzer addVolume(long volume) {
        this.currentVolume = volume;
        this.averageVolume = ((averageVolume * dataPoints) + volume) / (dataPoints + 1);
        this.dataPoints++;
        return this;
    }

    public boolean hasVolumeSurge() {
        return currentVolume > averageVolume * 3; // 3x average volume
    }

    public double getSurgeMultiplier() {
        return averageVolume != 0 ? currentVolume / averageVolume : 0;
    }

    // Getters
    public String getSymbol() { return symbol; }
    public long getCurrentVolume() { return currentVolume; }
    public double getAverageVolume() { return averageVolume; }
}
