package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.kafka.streams.model.ConversionFunnel;
import org.kafka.streams.model.UserActivity;
import org.kafka.streams.model.UserBehaviorProfile;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class UserActivityAnalyticsTest {

    @InjectMocks
    private UserActivityAnalytics userActivityAnalytics;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        userActivityAnalytics = new UserActivityAnalytics();
        setField(userActivityAnalytics, "userEventsProtobufTopic", "user-events-protobuf");
        setField(userActivityAnalytics, "userJourneyAnalytics", "user-journey-analytics");
        setField(userActivityAnalytics, "userSegmentation", "user-segmentation");
        setField(userActivityAnalytics, "conversions", "conversions");
    }

    @Test
    void testAnalyzeUserActivity() throws Exception {
        UserActivity activity = new UserActivity();
        setField(activity, "sessionId", "session123");
        setField(activity, "userId", "user456");
        setField(activity, "events", List.of("VIEW","CART","PURCHASE"));

        Method method = UserActivityAnalytics.class.getDeclaredMethod("analyzeUserActivity", UserActivity.class);
        method.setAccessible(true);

        String result = (String) method.invoke(userActivityAnalytics, activity);

        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("session123", resultMap.get("sessionId"));
        assertEquals("user456", resultMap.get("userId"));
        assertEquals(3, ((List<String>)resultMap.get("conversionPath")).size());
        assertTrue((Boolean) resultMap.get("isConverted"));
        assertNotNull(resultMap.get("timestamp"));
    }

    @Test
    void testSegmentUser() throws Exception {
        UserBehaviorProfile profile = new UserBehaviorProfile();
        setField(profile, "userId", "user789");
        setField(profile, "lastActiveTime", 1634567890000L);
        setField(profile, "eventCount", 171);
        setField(profile, "eventTypes", Set.of("VIEW", "CLICK", "PURCHASE", "SIGNUP", "SUBSCRIPTION"));

        Method method = UserActivityAnalytics.class.getDeclaredMethod("segmentUser", UserBehaviorProfile.class);
        method.setAccessible(true);

        String result = (String) method.invoke(userActivityAnalytics, profile);

        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("user789", resultMap.get("userId"));
        assertEquals("CUSTOMER", resultMap.get("segment"));
        assertEquals(10.0, resultMap.get("activityScore"));
        assertEquals("HIGH", resultMap.get("engagementLevel"));
        assertEquals(1634567890000L, resultMap.get("lastActiveTime"));
        assertNotNull(resultMap.get("timestamp"));
    }

    @Test
    void testIsConversionEvent() throws Exception {
        String[] conversionEvents = {"PURCHASE", "SIGNUP", "SUBSCRIPTION", "ADD_TO_CART"};
        String[] nonConversionEvents = {"VIEW", "CLICK", "SEARCH"};

        Method method = UserActivityAnalytics.class.getDeclaredMethod("isConversionEvent", String.class);
        method.setAccessible(true);

        for (String event : conversionEvents) {
            Boolean result = (Boolean) method.invoke(userActivityAnalytics, event);
            assertTrue(result, "Event " + event + " should be a conversion event");
        }
        for (String event : nonConversionEvents) {
            Boolean result = (Boolean) method.invoke(userActivityAnalytics, event);
            assertFalse(result, "Event " + event + " should not be a conversion event");
        }
    }

    @Test
    void testCreateConversionEvent() throws Exception {
        ConversionFunnel funnel = new ConversionFunnel();
        setField(funnel, "userId", "user101");
        setField(funnel, "steps", List.of("view", "cart", "checkout"));

        Method method = UserActivityAnalytics.class.getDeclaredMethod("createConversionEvent", ConversionFunnel.class);
        method.setAccessible(true);

        String result = (String) method.invoke(userActivityAnalytics, funnel);

        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("user101", resultMap.get("userId"));
        assertEquals("PURCHASE", resultMap.get("conversionType"));
        assertEquals(3, ((List<String>) resultMap.get("funnelSteps")).size());
        assertEquals(3, resultMap.get("touchpoints"));
        assertNotNull(resultMap.get("timestamp"));
    }

    // Helper method to set private fields for testing
    private void setField(Object object, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }
}