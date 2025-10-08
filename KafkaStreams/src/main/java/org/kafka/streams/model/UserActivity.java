package org.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.kafka.producer.model.protobuf.UserEventProto;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserActivity {
    private String sessionId;
    private String userId;
    private List<String> events ;
    private long startTime;
    private long endTime;

    public UserActivity() {
        this.events = new ArrayList<>();
    }



    public UserActivity addEvent(UserEventProto.UserEvent event) {
        if (events == null) {
            events = new ArrayList<>();
        }
        if (sessionId == null) {
            sessionId = event.getSessionId();
            userId = event.getUserId();
            startTime = event.getTimestamp();
        }
        events.add(event.getEventType());
        endTime = event.getTimestamp();
        return this;
    }

    public UserActivity merge(UserActivity other) {
        this.events.addAll(other.events);
        this.endTime = Math.max(this.endTime, other.endTime);
        return this;
    }

    public boolean isComplete() {
        return events != null && events.contains("PURCHASE") || events.size() > 5;
    }

    public boolean isConverted() {
        return events.contains("PURCHASE");
    }

    public long getDurationMinutes() {
        return (endTime - startTime) / (1000 * 60);
    }

    public String getSessionId() { return sessionId; }
    public String getUserId() { return userId; }
    public int getEventCount() { return events.size(); }
    public List<String> getConversionPath() { return new ArrayList<>(events); }

    public List<String> getEvents() { return events; }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }

    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setEvents(List<String> events) { this.events = events; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

}