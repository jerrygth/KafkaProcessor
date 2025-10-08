package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.kafka.producer.model.protobuf.UserEventProto;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConversionFunnel {
    private String userId;
    private List<String> steps = new ArrayList<>();
    private long startTime;
    private long conversionTime;

    public ConversionFunnel addStep(UserEventProto.UserEvent event) {
        if (userId == null) {
            userId = event.getUserId();
            startTime = event.getTimestamp();
        }
        steps.add(event.getEventType());
        if ("PURCHASE".equals(event.getEventType())) {
            conversionTime = event.getTimestamp();
        }
        return this;
    }

    public boolean isConverted() {
        return steps.contains("PURCHASE");
    }

    public long getTimeToConvertMinutes() {
        return conversionTime > 0 ? (conversionTime - startTime) / (1000 * 60) : 0;
    }

    public String getConversionType() {
        return "PURCHASE";
    }

    public int getTouchpoints() {
        return steps.size();
    }

    // Getters
    public String getUserId() { return userId; }
    public List<String> getSteps() { return new ArrayList<>(steps); }
}