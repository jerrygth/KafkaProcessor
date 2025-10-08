package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.kafka.producer.model.protobuf.UserEventProto;

import java.util.HashSet;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserBehaviorProfile {
    private String userId;
    private int eventCount = 0;
    private long lastActiveTime = 0;
    private Set<String> eventTypes = new HashSet<>();

    public UserBehaviorProfile updateWithEvent(UserEventProto.UserEvent event) {
        if (userId == null) userId = event.getUserId();
        eventCount++;
        lastActiveTime = event.getTimestamp();
        eventTypes.add(event.getEventType());
        return this;
    }

    public String getSegment() {
        if (eventTypes.contains("PURCHASE")) return "CUSTOMER";
        if (eventCount > 10) return "HIGH_ENGAGEMENT";
        if (eventCount > 3) return "MEDIUM_ENGAGEMENT";
        return "LOW_ENGAGEMENT";
    }

    public double getActivityScore() {
        return Math.min(eventCount * eventTypes.size() / 10.0, 10.0);
    }

    public String getEngagementLevel() {
        double score = getActivityScore();
        if (score > 7) return "HIGH";
        if (score > 4) return "MEDIUM";
        return "LOW";
    }

    // Getters
    public String getUserId() { return userId; }
    public long getLastActiveTime() { return lastActiveTime; }
}
