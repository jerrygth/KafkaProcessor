package org.kafka.consumer.listener;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private MeterRegistry meterRegistry;
    private final Counter protobufMessagesConsumed;
    private final Counter avroMessagesConsumed;
    private final Timer messageProcessingTime;
    private final Timer consumerLagTimer;

    private
    KafkaConsumer(MeterRegistry meterRegistry){
        this.meterRegistry = meterRegistry;
        this.protobufMessagesConsumed = Counter.builder("kafka.consumer.protobuf.messages")
                .description("Total number of protobuf messages consumed")
                .register(meterRegistry);
        this.avroMessagesConsumed = Counter.builder("kafka.consumer.avro.messages")
                .description("Total number of avro messages consumed")
                .register(meterRegistry);
        this.messageProcessingTime = Timer.builder("kafka.consumer.processing.time")
                .description("Time taken to process messages")
                .register(meterRegistry);
        this.consumerLagTimer = Timer.builder("kafka.consumer.lag")
                .description("Time taken to process consumer lag")
                .register(meterRegistry);
    }


    @KafkaListener(topics = "${producer.topics.stock-protobuf}",
                   groupId = "${consumer.group-id.stock:stock-consumer-group}",
                   containerFactory = "cccccccccc")
    public void consumeStockPriceProtoBuffMessage(@Payload StockPriceProto.StockPrice stockPrice,
                                                  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                                  @Header(KafkaHeaders.OFFSET) Long offset,
                                                  @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                                                  @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                                  @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long ts,
                                                  Acknowledgment ack ) {
        Timer.Sample sample = Timer.start();
        long startTime = System.currentTimeMillis();

        try {
            // Set MDC for structured logging
            MDC.put("messageType", "stock-protobuf");
            MDC.put("format", "protobuf");
            MDC.put("topic", topic);
            MDC.put("partition", String.valueOf(partition));
            MDC.put("offset", String.valueOf(offset));
            MDC.put("key", key);

            // Producer to Consumer latency
            long endToEndLatency = startTime - stockPrice.getTimestamp();
            consumerLagTimer.record(endToEndLatency, java.util.concurrent.TimeUnit.MILLISECONDS);

            // Process the stock price message
            processStockPrice(stockPrice);

            // Increment success counter
            protobufMessagesConsumed.increment();

            logger.info("Processed stock price: symbol={}, price=${}, volume={}, exchange={}, latency={}ms",
                    stockPrice.getSymbol(),
                    stockPrice.getPrice(),
                    stockPrice.getVolume(),
                    stockPrice.getExchange(),
                    endToEndLatency);
            ack.acknowledge();

        } catch (Exception e) {
            logger.error("Error processing stock price message: topic={}, partition={}, offset={}, key={}",
                    topic, partition, offset, key, e);
            throw e;
        } finally {
            sample.stop(messageProcessingTime);
            MDC.clear();
        }

    }

    @KafkaListener(
            topics = "${producer.topics.user-events-protobuf}",
            groupId = "${consumer.group-id.user-events:user-events-consumer-group}",
            containerFactory = "protobufUserEventKafkaListenerContainerFactory"
    )
    public void consumeUserEvent(
            @Payload UserEventProto.UserEvent userEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            ConsumerRecord<String, UserEventProto.UserEvent> record,
            Acknowledgment ack) {

        Timer.Sample sample = Timer.start();
        long startTime = System.currentTimeMillis();

        try {
            // Set MDC for structured logging
            MDC.put("messageType", "user-event-protobuf");
            MDC.put("format", "protobuf");
            MDC.put("topic", topic);
            MDC.put("partition", String.valueOf(partition));
            MDC.put("offset", String.valueOf(offset));
            MDC.put("key", key);

            // Producer to Consumer latency
            long endToEndLatency = startTime - userEvent.getTimestamp();
            consumerLagTimer.record(endToEndLatency, java.util.concurrent.TimeUnit.MILLISECONDS);

            // Process the user event message
            processUserEvent(userEvent,endToEndLatency);

            // Increment success counter
            protobufMessagesConsumed.increment();

            logger.info("Processed user event: eventId={}, userId={}, eventType={}, page={}, latency={}ms",
                    userEvent.getEventId(),
                    userEvent.getUserId(),
                    userEvent.getEventType(),
                    userEvent.getPage(),
                    endToEndLatency);
            ack.acknowledge();

        } catch (Exception e) {
            logger.error("Error processing user event message: topic={}, partition={}, offset={}, key={}",
                    topic, partition, offset, key, e);
            throw e;
        } finally {
            sample.stop(messageProcessingTime);
            MDC.clear();
        }
    }

    @KafkaListener(
            topics = "${producer.topics.sensors-avro}",
            groupId = "${consumer.group-id.sensors:sensors-consumer-group}",
            containerFactory = "avroSensorKafkaListenerContainerFactory"
    )
    public void consumeSensorDataAvro(
            @Payload GenericRecord sensorData,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            Acknowledgment ack) {

        Timer.Sample sample = Timer.start(meterRegistry);
        long consumeTime = System.currentTimeMillis();

        try {
            MDC.put("messageType", "sensor-avro");
            MDC.put("format", "avro");
            MDC.put("topic", topic);
            MDC.put("partition", String.valueOf(partition));
            MDC.put("offset", String.valueOf(offset));

            // Calculate and record lag
            long producerTimestamp = (Long) sensorData.get("timestamp");
            long lag = consumeTime - producerTimestamp;

            consumerLagTimer.record(lag, TimeUnit.MILLISECONDS);

            processSensorData(sensorData, lag);

            // Update metrics
            avroMessagesConsumed.increment();

            logger.info("Processed sensor data avro - Sensor: {}, Type: {}, Value: {}, Location: {}, Lag: {}ms",
                    sensorData.get("sensorId"), sensorData.get("sensorType"),
                    sensorData.get("value"), sensorData.get("location"), lag);
            ack.acknowledge();

        } catch (Exception e) {
            logger.error("Error processing sensor data avro message", e);
            meterRegistry.counter("kafka.consumer.errors", "format", "avro", "topic", topic).increment();
        } finally {
            sample.stop(messageProcessingTime);
            MDC.clear();
        }
    }

    // Avro Weather Data Consumer
    @KafkaListener(
            topics = "${producer.topics.weather-avro}",
            groupId = "${consumer.group-id.weather:weather-consumer-group}",
            containerFactory = "avroWeatherKafkaListenerContainerFactory"
    )
    public void consumeWeatherDataAvro(
            @Payload GenericRecord weatherData,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            Acknowledgment ack) {

        Timer.Sample sample = Timer.start(meterRegistry);
        long consumeTime = System.currentTimeMillis();

        try {
            MDC.put("messageType", "weather-avro");
            MDC.put("format", "avro");
            MDC.put("topic", topic);
            MDC.put("partition", String.valueOf(partition));
            MDC.put("offset", String.valueOf(offset));

            // Calculate and record lag
            long producerTimestamp = (Long) weatherData.get("timestamp");
            long lag = consumeTime - producerTimestamp;

            consumerLagTimer.record(lag, TimeUnit.MILLISECONDS);

            processWeatherData(weatherData, lag);

            avroMessagesConsumed.increment();

            GenericRecord location = (GenericRecord) weatherData.get("location");
            logger.info("Processed weather data avro - Station: {}, Temp: {}°C, Humidity: {}%, Lag: {}ms",
                    weatherData.get("stationId"), weatherData.get("temperature"),
                    weatherData.get("humidity"), lag);
            ack.acknowledge();


        } catch (Exception e) {
            logger.error("Error processing weather data avro message", e);
            meterRegistry.counter("kafka.consumer.errors", "format", "avro", "topic", topic).increment();
        } finally {
            sample.stop(messageProcessingTime);
            MDC.clear();
        }
    }

    private void processStockPrice(StockPriceProto.StockPrice stockPrice) {
        if (stockPrice.getPrice() <= 0) {
            throw new IllegalArgumentException("Invalid stock price: " + stockPrice.getPrice());
        }
        double bidAskSpread = stockPrice.getAsk() - stockPrice.getBid();
        double spreadPercentage = (bidAskSpread / stockPrice.getPrice()) * 100;
        if (stockPrice.getVolume() > 100000) {
            logger.info("High volume trade detected: {} volume={}",
                    stockPrice.getSymbol(), stockPrice.getVolume());
        }
        if (spreadPercentage > 1.0) {
            logger.warn("Wide bid-ask spread: {} spread={}%",
                    stockPrice.getSymbol(), String.format("%.2f", spreadPercentage));
        }
    }

    private void processWeatherData(GenericRecord weatherData, long lag) {


        double temperature = (Double) weatherData.get("temperature");
        double windSpeed = (Double) weatherData.get("windSpeed");

        if (temperature > 50 || temperature < -20) {
            logger.warn("EXTREME TEMPERATURE: Station {} reading {}°C",
                    weatherData.get("stationId"), temperature);
        }
        if (windSpeed > 50) {
            logger.warn("HIGH WIND ALERT: Station {} wind speed {}mph",
                    weatherData.get("stationId"), windSpeed);
        }
        if (lag > 5000) {
            logger.warn("HIGH LAG WARNING: Weather data processing lag {}ms for station {}",
                    lag, weatherData.get("stationId"));
        }
    }

    private void processSensorData(GenericRecord sensorData, long lag) {

        double value = (Double) sensorData.get("value");
        String sensorType = sensorData.get("sensorType").toString();
        int batteryLevel = (Integer) sensorData.get("batteryLevel");

        if ("temperature".equals(sensorType) && (value > 50 || value < -5)) {
            logger.warn("TEMPERATURE ANOMALY: Sensor {} reading {}°C",
                    sensorData.get("sensorId"), value);
        }

        if (batteryLevel < 10) {
            logger.warn("LOW BATTERY: Sensor {} battery at {}%",
                    sensorData.get("sensorId"), batteryLevel);

        }

        if (lag > 2000) {
            logger.warn("HIGH LAG WARNING: Sensor data processing lag {}ms for sensor {}",
                    lag, sensorData.get("sensorId"));
        }
    }

    private void processUserEvent(UserEventProto.UserEvent userEvent, long lag) {
        // Realistic user event processing
        if ("PURCHASE".equals(userEvent.getEventType())) {
            logger.info("PURCHASE EVENT: User {} made a purchase in session {}",
                    userEvent.getUserId(), userEvent.getSessionId());
        }

        if ("LOGIN".equals(userEvent.getEventType())) {
            logger.info("LOGIN EVENT: User {} logged in from page {}",
                    userEvent.getUserId(), userEvent.getPage());
        }

        if (lag > 500) {
            logger.warn("HIGH LAG WARNING: User event processing lag {}ms for user {}",
                    lag, userEvent.getUserId());
        }
    }

}
