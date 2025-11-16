package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.kafka.streams.model.StockUserInterestCorrelation;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class CrossFormatJoinsTest {

    @InjectMocks
    private CrossFormatJoins crossFormatJoins;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        crossFormatJoins = new CrossFormatJoins();
        setField(crossFormatJoins, "stockProtobufTopic", "stock-protobuf");
        setField(crossFormatJoins, "userEventsProtobufTopic", "user-events-protobuf");
        setField(crossFormatJoins, "stockInterestCorrelations", "stock-interest-correlations");
    }

    @Test
    void testExtractStockSymbolFromSearch() throws Exception {
        UserEventProto.UserEvent event = UserEventProto.UserEvent.newBuilder()
                .setEventType("SEARCH")
                .setPage("AAPL stock price")
                .build();

        Method method = CrossFormatJoins.class.getDeclaredMethod("extractStockSymbolFromSearch", UserEventProto.UserEvent.class);
        method.setAccessible(true);

        String result = (String) method.invoke(crossFormatJoins, event);

        assertEquals("AAPL", result);
    }

    @Test
    void testExtractStockSymbolFromSearch_NullEvent() throws Exception {
        Method method = CrossFormatJoins.class.getDeclaredMethod("extractStockSymbolFromSearch", UserEventProto.UserEvent.class);
        method.setAccessible(true);

        String result = (String) method.invoke(crossFormatJoins, (UserEventProto.UserEvent) null);

        assertNull(result);
    }

    @Test
    void testCorrelateStockPriceWithUserInterest() throws Exception {
        StockPriceProto.StockPrice stock = StockPriceProto.StockPrice.newBuilder()
                .setSymbol("AAPL")
                .setPrice(150.0)
                .setTimestamp(1634567890000L)
                .build();
        UserEventProto.UserEvent event = UserEventProto.UserEvent.newBuilder()
                .setUserId("user123")
                .setTimestamp(1634567891000L)
                .build();

        Method method = CrossFormatJoins.class.getDeclaredMethod("correlateStockPriceWithUserInterest", StockPriceProto.StockPrice.class, UserEventProto.UserEvent.class);
        method.setAccessible(true);

        StockUserInterestCorrelation correlation = (StockUserInterestCorrelation) method.invoke(crossFormatJoins, stock, event);

        assertEquals("AAPL", correlation.getSymbol());
        assertEquals(150.0, correlation.getStockPrice());
        assertEquals("user123", correlation.getUserId());
        assertEquals(1000L, correlation.getTimeLag());
    }

    @Test
    void testCreateInterestCorrelation() throws Exception {
        StockUserInterestCorrelation correlation = new StockUserInterestCorrelation("AAPL", 150.0, "user123", 1634567891000L, 1634567890000L);

        Method method = CrossFormatJoins.class.getDeclaredMethod("createInterestCorrelation", StockUserInterestCorrelation.class);
        method.setAccessible(true);

        String result = (String) method.invoke(crossFormatJoins, correlation);

        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("AAPL", resultMap.get("symbol"));
        assertEquals(150.0, resultMap.get("stockPrice"));
        assertEquals("user123", resultMap.get("userId"));
        assertEquals(1000L, ((Integer)resultMap.get("timeLag")).longValue());
        assertEquals("USER_SEARCH_STOCK_PRICE", resultMap.get("correlationType"));
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