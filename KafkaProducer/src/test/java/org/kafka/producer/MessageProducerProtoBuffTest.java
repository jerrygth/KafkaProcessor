package org.kafka.producer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
class MessageProducerProtoBuffTest {


    @Mock
    private Counter messageCounter;

    @Mock
    private KafkaTemplate<String, StockPriceProto.StockPrice> stockPriceKafkaTemplate;

    @Mock
    private KafkaTemplate<String, UserEventProto.UserEvent> userEventsKafkaTemplate;

    @Mock
    private ExecutorService executorService;

    SimpleMeterRegistry meterRegistry;

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;


    private MessageProducerProtoBuff producer;

    @BeforeEach
    void setUp() {

        meterRegistry = new SimpleMeterRegistry();
       producer = new MessageProducerProtoBuff(meterRegistry);

        ReflectionTestUtils.setField(producer, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(producer, "schemaRegistryUrl", "http://localhost:8081");
        ReflectionTestUtils.setField(producer, "stockProtobufTopic", "stock-prices-protobuf");
        ReflectionTestUtils.setField(producer, "userEventsProtobufTopic", "user-events-protobuf");

        ReflectionTestUtils.setField(producer, "executorService", executorService);
        ReflectionTestUtils.setField(producer, "messageCounter", messageCounter);
        ReflectionTestUtils.setField(producer, "stockPriceKafkaTemplate", stockPriceKafkaTemplate);

    }

    @Test
    void testProduceStockPriceMessage_Success() {
        CompletableFuture<SendResult<String, StockPriceProto.StockPrice>> future = new CompletableFuture<>();
        when(stockPriceKafkaTemplate.send(eq("stock-prices-protobuf"), anyString(), any(StockPriceProto.StockPrice.class)))
                .thenReturn(future);
        producer.produceStockPriceMessage();
        verify(executorService, times(1)).submit(runnableCaptor.capture());
        Runnable submittedTask = runnableCaptor.getValue();
        assertNotNull(submittedTask, "Submitted task should not be null");
        submittedTask.run();
        verify(messageCounter, times(1)).increment();
        Timer timer =meterRegistry.find("kafka.producer.message.time").timer();
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0);
    }

    @Test
    void testProduceStockPriceMessage_Error() {
        when(stockPriceKafkaTemplate.send(eq("stock-prices-protobuf"), anyString(), any(StockPriceProto.StockPrice.class)))
                .thenThrow(new RuntimeException("Kafka send failed"));

        producer.produceStockPriceMessage();

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
    void testProduceUserEventsProtobuf_Success() {
        ReflectionTestUtils.setField(producer, "userEventsKafkaTemplate", userEventsKafkaTemplate);
        CompletableFuture<SendResult<String, UserEventProto.UserEvent>> future = new CompletableFuture<>();
        when(userEventsKafkaTemplate.send(eq("user-events-protobuf"), anyString(), any(UserEventProto.UserEvent.class)))
                .thenReturn(future);

        producer.produceUserEventsProtobuf();

        verify(executorService, times(1)).submit(runnableCaptor.capture());
        Runnable submittedTask = runnableCaptor.getValue();
        assertNotNull(submittedTask, "Submitted task should not be null");
        submittedTask.run();

        verify(messageCounter, times(1)).increment();
        Timer timer =meterRegistry.find("kafka.producer.message.time").timer();
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0);
    }

    @Test
    void testGenerateStockPrice_ValidSymbol() {
        double price = ReflectionTestUtils.invokeMethod(producer, "generateStockPrice", "TCS");

        assert price >= 135.0 && price <= 165.0 : "Price should be within reasonable range for TCS";
    }

    @Test
    void testInitializeKafkaTemplate() {
        producer.initializeKafkaTemplate();

        assert producer != null;
        verify(stockPriceKafkaTemplate, never()).send(anyString(), anyString(), any()); // Ensure no messages sent during initialization
    }


}