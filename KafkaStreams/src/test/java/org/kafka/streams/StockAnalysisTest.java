package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.streams.model.StockMovingAverage;
import org.kafka.streams.model.VolumeAnalyzer;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class StockAnalysisTest {

    @InjectMocks
    private StockAnalysis stockAnalysis;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        stockAnalysis = new StockAnalysis();
        setField(stockAnalysis, "stockProtobufTopic", "stock-protobuf");
        setField(stockAnalysis, "stockMovingAverageAlerts", "stock-moving-average-alerts");
        setField(stockAnalysis, "volumeSurgeAlerts", "volume-surge-alerts");
    }

    @Test
    void testCreateMovingAverageAlert() throws Exception {
        StockMovingAverage movingAvg = new StockMovingAverage();
        setField(movingAvg, "symbol", "AAPL");
        setField(movingAvg, "currentAverage", 150.0);
        setField(movingAvg, "previousAverage", 145.0);

        Method method = StockAnalysis.class.getDeclaredMethod("createMovingAverageAlert", StockMovingAverage.class);
        method.setAccessible(true);

        // Act
        String result = (String) method.invoke(stockAnalysis, movingAvg);

        // Assert
        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("MOVING_AVERAGE_CHANGE", resultMap.get("alertType"));
        assertEquals("AAPL", resultMap.get("symbol"));
        assertEquals(150.0, resultMap.get("currentAverage"));
        assertEquals(145.0, resultMap.get("previousAverage"));
        Double changePercent = (Double)resultMap.get("changePercent");
        double roundedValue =Math.round(changePercent*100.0)/100.0;
        assertEquals(3.45, roundedValue);
        assertNotNull(resultMap.get("timestamp"));
    }

    @Test
    void testCreateVolumeSurgeAlert() throws Exception {
        // Arrange
        VolumeAnalyzer analyzer = new VolumeAnalyzer();
        setField(analyzer, "symbol", "GOOGL");
        setField(analyzer, "currentVolume", 1000000L);
        setField(analyzer, "averageVolume", 500000L);

        Method method = StockAnalysis.class.getDeclaredMethod("createVolumeSurgeAlert", VolumeAnalyzer.class);
        method.setAccessible(true);

        // Act
        String result = (String) method.invoke(stockAnalysis, analyzer);

        // Assert
        Map<String, Object> resultMap = objectMapper.readValue(result, Map.class);
        assertEquals("VOLUME_SURGE", resultMap.get("alertType"));
        assertEquals("GOOGL", resultMap.get("symbol"));
        assertEquals(1000000L, ((Integer)resultMap.get("currentVolume")).longValue());
        assertEquals(500000L, ((Double)resultMap.get("averageVolume")).longValue());
        Double surgeMultiplier = (Double)resultMap.get("surgeMultiplier");
        double roundedValue =Math.round(surgeMultiplier*100.0)/100.0;
        assertEquals(2.0, roundedValue);
        assertNotNull(resultMap.get("timestamp"));
    }

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