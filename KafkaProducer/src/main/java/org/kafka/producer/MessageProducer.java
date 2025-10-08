package org.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.kafka.producer.model.WeatherResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

//@Component
//@EnableScheduling
public class MessageProducer {

   /* private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final AtomicInteger currentMessageId = new AtomicInteger(0);

    @Value("${producer.max-messages}")
    private int maxMessages;

    @Value("${producer.kafka-topic}")
    private String topic;

    @Value("${producer.weather-api-url}")
    private String weatherApiUrl;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                           RestTemplate restTemplate,
                           ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.executorService = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                CORE_POOL_SIZE * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

    }

    @PostConstruct
    public void logConfig() {
        logger.info("Kafka Producer initialized with topic='{}', maxMessages={}, API URL={}",
                topic, maxMessages, weatherApiUrl);
    }

   // @Scheduled(fixedDelayString = "${producer.interval-ms}")
    public void produceMessage() {
        if (currentMessageId.get() >= maxMessages) return;

        try {
            ResponseEntity<WeatherResponse> response = restTemplate.getForEntity(weatherApiUrl, WeatherResponse.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                WeatherResponse weather = response.getBody();
                String key = String.valueOf(weather.getGenerationTimeMs());
                String value = objectMapper.writeValueAsString(weather.getCurrentWeather());

                executorService.submit(() -> sendMessage(key, value));
                int id = currentMessageId.incrementAndGet();
                logger.info("Submitted message #{}: key={}, value={}", id, key, value);
            } else {
                logger.warn("Non-success response from weather API: {}", response.getStatusCode());
            }
        } catch (JsonProcessingException e) {
            logger.error("JSON serialization failed", e);
        } catch (Exception e) {
            logger.error("Error fetching weather data or submitting message", e);
        }
    }

    private void sendMessage(String key, String value) {
        long startTime = System.nanoTime();
        try {
            int count = messageCount.incrementAndGet();
            if (count <= maxMessages) {
                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);

                future.thenAccept(result -> {
                    logger.info("Sent message: key={} (partition: {}, offset: {})",
                            key, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                }).exceptionally(ex -> {
                    logger.error("Kafka send failed: key={}", key, ex);
                    return null;
                });

                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                logger.info("Processed message #{} in {} ms", count, durationMs);
            }
        } catch (Exception e) {
            logger.error("Error sending message to Kafka: key={}", key, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                logger.warn("Executor did not terminate in time, forced shutdown");
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            logger.error("Shutdown interrupted", e);
        }
    }*/
}
