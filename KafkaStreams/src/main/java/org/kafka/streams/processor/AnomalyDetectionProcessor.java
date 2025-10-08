package org.kafka.streams.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.kafka.streams.model.SensorStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

public class AnomalyDetectionProcessor implements Processor<Windowed<String>, SensorStatistics, String, String> {

    private static final Logger logger = LoggerFactory.getLogger(AnomalyDetectionProcessor.class);
    private ProcessorContext<String, String> context;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<Windowed<String>, SensorStatistics> record) {
        try {
            SensorStatistics stats = record.value();
            String sensorKey = record.key().key();

            if (stats.hasAnomaly()) {
                String alert = createAnomalyAlert(sensorKey, stats);
                context.forward(new Record<>(sensorKey, alert, record.timestamp()));
                logger.warn("Sensor anomaly detected for {}: Z-score {}", sensorKey, stats.getZScore());
            }

        } catch (Exception e) {
            logger.error("Error processing anomaly detection: {}", record, e);
        }
    }

    private String createAnomalyAlert(String sensorKey, SensorStatistics stats) {
        try {
            Map<String, Object> alert = Map.of(
                    "alertType", "SENSOR_ANOMALY",
                    "sensorKey", sensorKey,
                    "currentValue", stats.getCurrentValue(),
                    "mean", stats.getMean(),
                    "standardDeviation", stats.getStandardDeviation(),
                    "zScore", stats.getZScore(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating anomaly alert", e);
            return "{}";
        }
    }
}
