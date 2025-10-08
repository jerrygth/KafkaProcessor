package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.kafka.streams.model.ConversionFunnel;
import org.kafka.streams.model.UserActivity;
import org.kafka.streams.model.UserBehaviorProfile;
import org.kafka.streams.model.VolumeAnalyzer;
import org.kafka.streams.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

@Component
public class UserActivityAnalytics {

    private static final Logger logger = LoggerFactory.getLogger(UserActivityAnalytics.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${producer.topics.user-events-protobuf}")
    private String userEventsProtobufTopic;

    @Value("${output.topics.user-journey-analytics}")
    private String userJourneyAnalytics;

    @Value("${output.topics.user-segmentation}")
    private String userSegmentation;


    @Value("${output.topics.conversions}")
    private String conversions;



    public void processUserJourneyAnalytics(StreamsBuilder builder,
                                            KafkaProtobufSerde<UserEventProto.UserEvent> userEventSerde) {
        KStream<String, UserEventProto.UserEvent> userStream =
                builder.stream(userEventsProtobufTopic, Consumed.with(Serdes.String(), userEventSerde));

        // User session-based mapping
        userStream
                .groupBy((key, event) -> event.getSessionId())
                .windowedBy(SessionWindows.with(Duration.ofMinutes(30)))
                .aggregate(
                        () -> new UserActivity(),
                        (key, event, useractivity) -> useractivity.addEvent(event),
                        (key, useractivity1, useractivity2) -> useractivity1.merge(useractivity2),
                        Materialized.<String, UserActivity, SessionStore<Bytes, byte[]>>as("user-journeys")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(UserActivity.class))
                )
                .toStream()
                .peek((key, useractivity) -> {
                    if (useractivity == null) {
                        logger.warn("WARNING: useractivity is null for key=" + key);
                    }else if (useractivity.getEvents() == null) {
                        logger.warn("Events list is null for session=" + key);
                    }else {
                        logger.warn("WARNING: useractivity events=" + useractivity.getEvents());
                    }
                })
                .filter((key, useractivity) -> useractivity != null && useractivity.isComplete())
                .mapValues(this::analyzeUserActivity)
                .to(userJourneyAnalytics);

        // Real-time user behaviour
        userStream
                .groupBy((key, event) -> event.getUserId())
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        () -> new UserBehaviorProfile(),
                        (key, event, profile) -> profile.updateWithEvent(event),
                        Materialized.<String,UserBehaviorProfile, WindowStore<Bytes,byte[]>>as("user-profiles")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(UserBehaviorProfile.class))
                )
                .toStream()
                .mapValues(this::segmentUser)
                .to(userSegmentation);

        //Conversion funnel analysis
        userStream
                .filter((key, event) -> isConversionEvent(event.getEventType()))
                .groupBy((key, event) -> event.getUserId())
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .aggregate(
                        () -> new ConversionFunnel(),
                        (key, event, funnel) -> funnel.addStep(event),
                        Materialized.<String,ConversionFunnel, WindowStore<Bytes,byte[]>>as("conversion-funnels")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(ConversionFunnel.class))
                )
                .toStream()
                .filter((key, funnel) -> funnel.isConverted())
                .mapValues(this::createConversionEvent)
                .to(conversions);

        logger.info("User journey analytics topology configured");
    }

    private String analyzeUserActivity(UserActivity activity) {
        try {
            Map<String, Object> analysis = Map.of(
                    "sessionId", activity.getSessionId(),
                    "userId", activity.getUserId(),
                    "totalEvents", activity.getEventCount(),
                    "duration", activity.getDurationMinutes(),
                    "conversionPath", activity.getConversionPath(),
                    "isConverted", activity.isConverted(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(analysis);
        } catch (Exception e) {
            logger.error("Error analyzing user activity", e);
            return "{}";
        }
    }

    private String segmentUser(UserBehaviorProfile profile) {
        try {
            Map<String, Object> segmentation = Map.of(
                    "userId", profile.getUserId(),
                    "segment", profile.getSegment(),
                    "activityScore", profile.getActivityScore(),
                    "engagementLevel", profile.getEngagementLevel(),
                    "lastActiveTime", profile.getLastActiveTime(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(segmentation);
        } catch (Exception e) {
            logger.error("Error segmenting user", e);
            return "{}";
        }
    }

    private boolean isConversionEvent(String eventType) {
        return Set.of("PURCHASE", "SIGNUP", "SUBSCRIPTION", "ADD_TO_CART").contains(eventType);
    }

    private String createConversionEvent(ConversionFunnel funnel) {
        try {
            Map<String, Object> conversion = Map.of(
                    "userId", funnel.getUserId(),
                    "conversionType", funnel.getConversionType(),
                    "funnelSteps", funnel.getSteps(),
                    "timeToConvert", funnel.getTimeToConvertMinutes(),
                    "touchpoints", funnel.getTouchpoints(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(conversion);
        } catch (Exception e) {
            logger.error("Error creating conversion event", e);
            return "{}";
        }
    }



}
