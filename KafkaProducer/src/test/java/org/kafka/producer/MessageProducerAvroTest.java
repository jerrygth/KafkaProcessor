package org.kafka.producer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageProducerAvroTest {


    private SimpleMeterRegistry meterRegistry;

    @Mock
    private Counter messageCounter;

    @Mock
    private Timer messageProductionTime;

    @Mock
    private Timer.Sample timerSample;

    @Mock
    private KafkaTemplate<String, GenericRecord> avroTemplate;

    @Mock
    private ExecutorService executorService;

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    private MessageProducerAvro producer;

    @BeforeEach
    void setUp() {

        meterRegistry = new SimpleMeterRegistry();
        producer = new MessageProducerAvro(meterRegistry);
        ReflectionTestUtils.setField(producer, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(producer, "schemaRegistryUrl", "http://localhost:8081");
        ReflectionTestUtils.setField(producer, "sensorsAvroTopic", "iot-sensors-avro");
        ReflectionTestUtils.setField(producer, "weatherAvroTopic", "weather-data-avro");
        producer.initializeKafkaTemplate();

        ReflectionTestUtils.setField(producer, "executorService", executorService);
        ReflectionTestUtils.setField(producer, "messageCounter", messageCounter);
        ReflectionTestUtils.setField(producer, "avroSensorTemplate", avroTemplate);
        ReflectionTestUtils.setField(producer, "avroWeatherTemplate", avroTemplate);

        /*when(meterRegistry.counter(anyString())).thenReturn(messageCunter);
        when(meterRegistry.timer(anyString())).thenReturn(messageProductionTime);
        when(Timer.start(meterRegistry)).thenReturn(timerSample);*/


    }

    @Test
    void testProduceSensorDataAvro_Success() {
        CompletableFuture<SendResult<String, GenericRecord>> future = new CompletableFuture<>();
        when(avroTemplate.send(eq("iot-sensors-avro"), anyString(), any(GenericRecord.class)))
                .thenReturn(future);
        producer.produceSensorDataAvro();
        verify(executorService, times(1)).submit(runnableCaptor.capture());
        Runnable submittedTask = runnableCaptor.getValue();
        assertNotNull(submittedTask, "Submitted task should not be null");
        submittedTask.run();
        verify(messageCounter, times(1)).increment();
        Timer timer =meterRegistry.find("kafka.producer.message.time").timer();
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0);
    }

    @Test
    void testProduceSensorDataAvro_Error() {
        when(avroTemplate.send(eq("iot-sensors-avro"), anyString(), any(GenericRecord.class)))
                .thenThrow(new RuntimeException("Kafka send failed"));
        producer.produceSensorDataAvro();
        verify(executorService, times(1)).submit(runnableCaptor.capture());
        Runnable submittedTask = runnableCaptor.getValue();
        assertNotNull(submittedTask, "Submitted task should not be null");
        submittedTask.run();
        Counter counter =meterRegistry.find("kafka.producer.errors").counter();
        assertThat(counter.count()).isGreaterThan(0);
        Timer timer =meterRegistry.find("kafka.producer.message.time").timer();
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0);
    }

    @Test
    void testProduceWeatherDataAvro_Success() {

        CompletableFuture<SendResult<String, GenericRecord>> future = new CompletableFuture<>();
        when(avroTemplate.send(eq("weather-data-avro"), anyString(), any(GenericRecord.class)))
                .thenReturn(future);
        producer.produceWeatherDataAvro();
        verify(executorService, times(1)).submit(runnableCaptor.capture());
        Runnable submittedTask = runnableCaptor.getValue();
        assertNotNull(submittedTask, "Submitted task should not be null");
        submittedTask.run();
        verify(messageCounter, times(1)).increment();
        Timer timer =meterRegistry.find("kafka.producer.message.time").timer();
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0);
    }

    @Test
    void testGenerateSensorValue() {
        double value = ReflectionTestUtils.invokeMethod(producer, "generateSensorValue");

        assert value >= 15.0 && value <= 50.0 : "Sensor value should be within reasonable range";
    }

    @Test
    void testInitializeAvroSchemas() {
        // Act
        ReflectionTestUtils.invokeMethod(producer, "initializeAvroSchemas");

        // Assert
        Schema sensorSchema = (Schema) ReflectionTestUtils.getField(producer, "sensorSchema");
        Schema weatherSchema = (Schema) ReflectionTestUtils.getField(producer, "weatherSchema");
        assertNotNull(sensorSchema, "Sensor schema should be initialized");
        assertNotNull(weatherSchema, "Weather schema should be initialized");
    }
}