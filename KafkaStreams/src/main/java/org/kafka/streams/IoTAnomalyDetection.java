package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.kafka.streams.model.ConversionFunnel;
import org.kafka.streams.model.SensorHealth;
import org.kafka.streams.model.SensorStatistics;
import org.kafka.streams.model.WeatherSensorCorrelation;
import org.kafka.streams.processor.AnomalyDetectionProcessor;
import org.kafka.streams.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class IoTAnomalyDetection {
    private static final Logger logger = LoggerFactory.getLogger(IoTAnomalyDetection.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${producer.topics.sensors-avro}")
    private String sensorsAvroTopic;

    @Value("${producer.topics.weather-avro}")
    private String weatherAvroTopic;

    @Value("${output.topics.sensor-weather-correlations}")
    private String sensorWeatherCorrelations;

    @Value("${output.topics.sensor-maintenance-alerts}")
    private String sensorMaintenanceAlerts;



    public void processIoTAnomalyDetection(StreamsBuilder builder, Serde<GenericRecord> avroSerde) {

        KStream<String, GenericRecord> sensorStream =
                builder.stream(sensorsAvroTopic, Consumed.with(Serdes.String(), avroSerde));

        StoreBuilder<KeyValueStore<String, GenericRecord>> volatilityStateStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("anomaly-detection-store"),
                        Serdes.String(),
                        avroSerde
                );
        builder.addStateStore(volatilityStateStoreBuilder);

        //Statistical anomaly detection using Z-score
        sensorStream
                .groupBy((key, sensor) -> buildSensorGroupKey(sensor))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)).advanceBy(Duration.ofMinutes(1)))
                .aggregate(
                        () -> new SensorStatistics(),
                        (key, sensor, stats) -> stats.addReading(getNumericValue(sensor)),
                        Materialized.<String, SensorStatistics, WindowStore<Bytes,byte[]>>as("sensor-statistics")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(SensorStatistics.class))
                )
                .toStream()
                .process(() -> new AnomalyDetectionProcessor(), "anomaly-detection-store");

        //Sensor health monitoring
        sensorStream
                .mapValues(this::extractSensorHealth)
                .filter((key, health) -> health != null && health.needsAttention())
                .mapValues(this::createSensorMaintenanceAlert)
                .to(sensorMaintenanceAlerts);

        //Environmental correlation analysis
        KStream<String, GenericRecord> weatherStream =
                builder.stream(weatherAvroTopic, Consumed.with(Serdes.String(), avroSerde));

        sensorStream
                .filter((key, sensor) -> "temperature".equals(sensor.get("sensorType").toString()))
                .join(
                        weatherStream,
                        this::correlateSensorWithWeather,
                        JoinWindows.of(Duration.ofMinutes(2)),
                        StreamJoined.with(Serdes.String(), avroSerde, avroSerde)
                )
               .filter((key, correlation) -> correlation.hasSignificantDeviation())
               .mapValues(this::createCorrelationAlert)
                .to(sensorWeatherCorrelations);

        logger.info("IoT anomaly detection topology configured");

    }


    private String buildSensorGroupKey(GenericRecord sensor) {
        return sensor.get("sensorId") + "_" + sensor.get("sensorType");
    }

    private double getNumericValue(GenericRecord sensor) {
        return (Double) sensor.get("value");
    }

    private SensorHealth extractSensorHealth(GenericRecord sensor) {
        try {
            return new SensorHealth(
                    sensor.get("sensorId").toString(),
                    (Integer) sensor.get("batteryLevel"),
                    (Long) sensor.get("timestamp")
            );
        } catch (Exception e) {
            logger.warn("Failed to extract sensor health from record", e);
            return null;
        }
    }

    private String createSensorMaintenanceAlert(SensorHealth health) {
        try {
            Map<String, Object> alert = Map.of(
                    "alertType", "SENSOR_MAINTENANCE",
                    "sensorId", health.getSensorId(),
                    "batteryLevel", health.getBatteryLevel(),
                    "lastSeen", health.getLastSeen(),
                    "maintenanceType", health.getMaintenanceType(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating sensor maintenance alert", e);
            return "{}";
        }
    }

    private WeatherSensorCorrelation correlateSensorWithWeather(GenericRecord sensor, GenericRecord weather) {
        try {
            double sensorTemp = (Double) sensor.get("value");
            double weatherTemp = (Double) weather.get("temperature");
            long timestamp = (Long) sensor.get("timestamp");

            return new WeatherSensorCorrelation(
                    sensor.get("sensorId").toString(),
                    sensorTemp,
                    weatherTemp,
                    timestamp
            );
        } catch (Exception e) {
            logger.warn("Failed to correlate sensor with weather data", e);
            return new WeatherSensorCorrelation("unknown", 0.0, 0.0, System.currentTimeMillis());
        }
    }

    private String createCorrelationAlert(WeatherSensorCorrelation correlation) {
        try {
            Map<String, Object> alert = Map.of(
                    "alertType", "WEATHER_SENSOR_CORRELATION",
                    "sensorId", correlation.getSensorId(),
                    "sensorTemperature", correlation.getSensorTemperature(),
                    "weatherTemperature", correlation.getWeatherTemperature(),
                    "deviation", correlation.getDeviation(),
                    "timestamp", correlation.getTimestamp()
            );
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating correlation alert", e);
            return "{}";
        }
    }

}
