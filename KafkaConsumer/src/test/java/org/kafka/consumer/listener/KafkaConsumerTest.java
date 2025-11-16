package org.kafka.consumer.listener;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterRegistryConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private SimpleMeterRegistry meterRegistry;

    @Mock
    private Counter protobufMessagesConsumed;

    @Mock
    private Counter avroMessagesConsumed;

    @Mock
    private Timer messageProcessingTime;

    @Mock
    private Timer consumerLagTimer;

    @Mock
    private Acknowledgment acknowledgment;

    @Mock
    private Timer.Sample timerSample;

   private KafkaConsumer kafkaConsumer;

    @BeforeEach
    void setUp() {

        meterRegistry = new SimpleMeterRegistry();
        kafkaConsumer = new KafkaConsumer(meterRegistry);
        ReflectionTestUtils.setField(kafkaConsumer, "consumerLagTimer", consumerLagTimer);

       /* when(meterRegistry.counter(eq("kafka.consumer.protobuf.messages"))).thenReturn(protobufMessagesConsumed);
        when(meterRegistry.counter(eq("kafka.consumer.avro.messages"))).thenReturn(avroMessagesConsumed);
        when(meterRegistry.timer(eq("kafka.consumer.processing.time"))).thenReturn(messageProcessingTime);
        when(meterRegistry.timer(eq("kafka.consumer.lag"))).thenReturn(consumerLagTimer);
        when(meterRegistry.counter(eq("kafka.consumer.errors"), any(String[].class))).thenReturn(mock(Counter.class));

        lenient().when(meterRegistry.timer(anyString())).thenReturn(messageProcessingTime);
        lenient().doNothing().when(timerSample).stop(messageProcessingTime);*/
    }

    @Test
    void testConsumeStockPriceProtoBuffMessage_ValidPrice() {
        StockPriceProto.StockPrice stockPrice = StockPriceProto.StockPrice.newBuilder()
                .setSymbol("AAPL")
                .setPrice(150.0)
                .setVolume(50000)
                .setExchange("NASDAQ")
                .setAsk(151.0)
                .setBid(149.0)
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "stock-prices-protobuf";
        String key = "AAPL";
        Long offset = 1L;
        Integer partition = 0;
        Long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeStockPriceProtoBuffMessage(stockPrice, topic, offset, partition, key, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
        verify(consumerLagTimer).record(anyLong(), any());
    }

    @Test
    void testConsumeStockPriceProtoBuffMessage_HighVolume() {
        StockPriceProto.StockPrice stockPrice = StockPriceProto.StockPrice.newBuilder()
                .setSymbol("AAPL")
                .setPrice(150.0)
                .setVolume(150000)
                .setExchange("NASDAQ")
                .setAsk(151.0)
                .setBid(149.0)
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "stock-prices-protobuf";
        String key = "AAPL";
        Long offset = 1L;
        Integer partition = 0;
        Long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeStockPriceProtoBuffMessage(stockPrice, topic, offset, partition, key, timestamp, acknowledgment);
        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testConsumeStockPriceProtoBuffMessage_WideBidAskSpread() {
        // Arrange
        StockPriceProto.StockPrice stockPrice = StockPriceProto.StockPrice.newBuilder()
                .setSymbol("AAPL")
                .setPrice(150.0)
                .setVolume(50000)
                .setExchange("NASDAQ")
                .setAsk(153.0)
                .setBid(147.0)
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "stock-prices-protobuf";
        String key = "AAPL";
        Long offset = 1L;
        Integer partition = 0;
        Long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeStockPriceProtoBuffMessage(stockPrice, topic, offset, partition, key, timestamp, acknowledgment);
        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testConsumeStockPriceProtoBuffMessage_InvalidPrice() {
        StockPriceProto.StockPrice stockPrice = StockPriceProto.StockPrice.newBuilder()
                .setSymbol("AAPL")
                .setPrice(-10.0)
                .setVolume(50000)
                .setExchange("NASDAQ")
                .setAsk(151.0)
                .setBid(149.0)
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "stock-prices-protobuf";
        String key = "AAPL";
        Long offset = 1L;
        Integer partition = 0;
        Long timestamp = System.currentTimeMillis();

        assertThrows(IllegalArgumentException.class, () ->
                kafkaConsumer.consumeStockPriceProtoBuffMessage(stockPrice, topic, offset, partition, key, timestamp, acknowledgment));
        verify(protobufMessagesConsumed, never()).increment();
        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isZero();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void testConsumeUserEvent_PurchaseEvent() {
        UserEventProto.UserEvent userEvent = UserEventProto.UserEvent.newBuilder()
                .setEventId("evt123")
                .setUserId("user123")
                .setEventType("PURCHASE")
                .setPage("checkout")
                .setSessionId("sess123")
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "user-events-protobuf";
        String key = "user123";
        long offset = 1L;
        int partition = 0;
        long timestamp = System.currentTimeMillis();
        ConsumerRecord<String, UserEventProto.UserEvent> record = mock(ConsumerRecord.class);

        kafkaConsumer.consumeUserEvent(userEvent, topic, partition, key, offset, timestamp, record, acknowledgment);
        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testConsumeUserEvent_LoginEvent() {
        UserEventProto.UserEvent userEvent = UserEventProto.UserEvent.newBuilder()
                .setEventId("evt124")
                .setUserId("user124")
                .setEventType("LOGIN")
                .setPage("login")
                .setSessionId("sess124")
                .setTimestamp(System.currentTimeMillis() - 100)
                .build();
        String topic = "user-events-protobuf";
        String key = "user124";
        long offset = 1L;
        int partition = 0;
        long timestamp = System.currentTimeMillis();
        ConsumerRecord<String, UserEventProto.UserEvent> record = mock(ConsumerRecord.class);
        kafkaConsumer.consumeUserEvent(userEvent, topic, partition, key, offset, timestamp, record, acknowledgment);
        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }

    @Test
    void testConsumeUserEvent_HighLag() {
        UserEventProto.UserEvent userEvent = UserEventProto.UserEvent.newBuilder()
                .setEventId("evt125")
                .setUserId("user125")
                .setEventType("PURCHASE")
                .setPage("checkout")
                .setSessionId("sess125")
                .setTimestamp(System.currentTimeMillis() - 600)
                .build();
        String topic = "user-events-protobuf";
        String key = "user125";
        long offset = 1L;
        int partition = 0;
        long timestamp = System.currentTimeMillis();
        ConsumerRecord<String, UserEventProto.UserEvent> record = mock(ConsumerRecord.class);

        kafkaConsumer.consumeUserEvent(userEvent, topic, partition, key, offset, timestamp, record, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.protobuf.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testConsumeSensorDataAvro_TemperatureAnomaly() {
        GenericRecord sensorData = mock(GenericRecord.class);
        when(sensorData.get("sensorId")).thenReturn("sensor123");
        when(sensorData.get("sensorType")).thenReturn("temperature");
        when(sensorData.get("value")).thenReturn(60.0);
        when(sensorData.get("batteryLevel")).thenReturn(50);
        when(sensorData.get("timestamp")).thenReturn(System.currentTimeMillis() - 100);
        String topic = "iot-sensors-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeSensorDataAvro(sensorData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }

    @Test
    void testConsumeSensorDataAvro_LowBattery() {
        // Arrange
        GenericRecord sensorData = mock(GenericRecord.class);
        when(sensorData.get("sensorId")).thenReturn("sensor124");
        when(sensorData.get("sensorType")).thenReturn("temperature");
        when(sensorData.get("value")).thenReturn(20.0);
        when(sensorData.get("batteryLevel")).thenReturn(5);
        when(sensorData.get("timestamp")).thenReturn(System.currentTimeMillis() - 100);
        String topic = "iot-sensors-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeSensorDataAvro(sensorData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }

    @Test
    void testConsumeSensorDataAvro_HighLag() {
        // Arrange
        GenericRecord sensorData = mock(GenericRecord.class);
        when(sensorData.get("sensorId")).thenReturn("sensor125");
        when(sensorData.get("sensorType")).thenReturn("temperature");
        when(sensorData.get("value")).thenReturn(20.0);
        when(sensorData.get("batteryLevel")).thenReturn(50);
        when(sensorData.get("timestamp")).thenReturn(System.currentTimeMillis() - 3000);
        String topic = "iot-sensors-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeSensorDataAvro(sensorData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testConsumeWeatherDataAvro_ExtremeTemperature() {
        GenericRecord weatherData = mock(GenericRecord.class);
        when(weatherData.get("stationId")).thenReturn("station123");
        when(weatherData.get("temperature")).thenReturn(55.0);
        when(weatherData.get("windSpeed")).thenReturn(20.0);
        when(weatherData.get("timestamp")).thenReturn(System.currentTimeMillis() - 100);
        String topic = "weather-data-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeWeatherDataAvro(weatherData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }

    @Test
    void testConsumeWeatherDataAvro_HighWindSpeed() {
        GenericRecord weatherData = mock(GenericRecord.class);
        when(weatherData.get("stationId")).thenReturn("station124");
        when(weatherData.get("temperature")).thenReturn(20.0);
        when(weatherData.get("windSpeed")).thenReturn(60.0);
        when(weatherData.get("timestamp")).thenReturn(System.currentTimeMillis() - 100);
        String topic = "weather-data-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeWeatherDataAvro(weatherData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }

    @Test
    void testConsumeWeatherDataAvro_HighLag() {
        GenericRecord weatherData = mock(GenericRecord.class);
        when(weatherData.get("stationId")).thenReturn("station125");
        when(weatherData.get("temperature")).thenReturn(20.0);
        when(weatherData.get("windSpeed")).thenReturn(20.0);
        when(weatherData.get("timestamp")).thenReturn(System.currentTimeMillis() - 6000);
        String topic = "weather-data-avro";
        int partition = 0;
        long offset = 1L;
        long timestamp = System.currentTimeMillis();

        kafkaConsumer.consumeWeatherDataAvro(weatherData, topic, partition, offset, timestamp, acknowledgment);

        Counter counter =meterRegistry.find("kafka.consumer.avro.messages").counter();
        assertThat(counter.count()).isGreaterThan(0);
        verify(acknowledgment).acknowledge();

    }
}