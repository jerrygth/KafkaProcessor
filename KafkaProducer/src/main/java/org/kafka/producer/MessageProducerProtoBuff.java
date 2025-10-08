package org.kafka.producer;


import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

@Component
@EnableScheduling
public class MessageProducerProtoBuff {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerProtoBuff.class);

    private final MeterRegistry meterRegistry;
    private final Counter messageCounter;

    private final Timer messageProductionTime;

    private final ExecutorService executorService;

    private KafkaTemplate<String, StockPriceProto.StockPrice> stockPriceKafkaTemplate;

    private KafkaTemplate<String, UserEventProto.UserEvent> userEventsKafkaTemplate;

    private final Random random = new Random();

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${producer.topics.stock-protobuf}")
    private String stockProtobufTopic;

    @Value("${producer.topics.user-events-protobuf}")
    private String userEventsProtobufTopic;

    public MessageProducerProtoBuff(MeterRegistry meterRegistry){
        this.meterRegistry = meterRegistry;
        this.messageCounter =  Counter.builder("kafka.producer.protobuf.messages")
                .description("Number of protobuf messages produced")
                .register(meterRegistry);
        this.messageProductionTime = Timer.builder("kafka.producer.message.time")
                .description("Time taken to produce protobuf messages")
                .register(meterRegistry);
        this.executorService = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors() * 2,
                60L, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(1000),
                new ThreadFactory() {
                    private AtomicLong threadNumber = new AtomicLong(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "kafka-protobuf-producer-" + threadNumber.getAndIncrement());
                        t.setDaemon(false);
                        return t;
                    }
                },
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @PostConstruct
    public void  initializeKafkaTemplate(){
        initializeProtobufTemplates();
        logger.info("ProtoBuff format Kafka Producer initialized with Schema Registry: {}", schemaRegistryUrl);
    }

    private void initializeProtobufTemplates(){
        Map<String, Object> protobufProps = createBaseProducerProps();
        protobufProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        protobufProps.put("schema.registry.url", schemaRegistryUrl);
        protobufProps.put("auto.register.schemas", true);
        protobufProps.put("use.latest.version", true);

        DefaultKafkaProducerFactory<String, StockPriceProto.StockPrice> protobufProducerFactory =
                new DefaultKafkaProducerFactory<>(protobufProps);
        this.stockPriceKafkaTemplate = new KafkaTemplate<>(protobufProducerFactory);

        DefaultKafkaProducerFactory<String, UserEventProto.UserEvent> userEventsProducerFactory =
                new DefaultKafkaProducerFactory<>(protobufProps);
        this.userEventsKafkaTemplate = new KafkaTemplate<>(userEventsProducerFactory);
    }


    private Map<String, Object> createBaseProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // Enable idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return props;
    }

    // High-frequency stock price updates using Protobuf
    @Scheduled(fixedRate = 1000)
    public void produceStockPriceMessage(){
        executorService.submit(() -> {
            Timer.Sample timer = Timer.start(meterRegistry);
           try{
               MDC.put("messageType", "stock-protobuf");
               MDC.put("format", "protobuf");

               String[] symbols = {"TCS", "INFY", "TSLA", "TATAMOTROS", "META", "NVDA", "NFLX"};
               String symbol = symbols[random.nextInt(symbols.length)];
               StockPriceProto.StockPrice stockPrice = StockPriceProto.StockPrice.newBuilder()
                       .setSymbol(symbol)
                       .setPrice(generateStockPrice(symbol))
                       .setVolume(random.nextInt(1000000) + 10000)
                       .setTimestamp(System.currentTimeMillis())
                       .setExchange("NSD")
                       .setBid(generateStockPrice(symbol) - 0.01)
                       .setAsk(generateStockPrice(symbol) + 0.01)
                       .setMarketCap(random.nextLong() % 1000000000000L)
                       .build();
               sendProtobufMessage(stockPriceKafkaTemplate, stockProtobufTopic, symbol, stockPrice);
               messageCounter.increment();
           }catch(Exception e) {
               logger.error("Error producing stock price protobuf message", e);
           } finally {
              timer.stop(messageProductionTime);
               MDC.clear();
           }

        });

    }

    @Scheduled(fixedRate = 5000)
    public void produceUserEventsProtobuf() {
        executorService.submit(() -> {
            Timer.Sample timer = Timer.start(meterRegistry);
            try {
                MDC.put("messageType", "user-event-protobuf");
                MDC.put("format", "protobuf");

                String[] eventTypes = {"PAGE_VIEW", "CLICK", "PURCHASE", "LOGIN", "LOGOUT", "SEARCH", "ADD_TO_CART"};
                String userId = "user_" + (random.nextInt(50000) + 1);
                UserEventProto.UserEvent userEvent = UserEventProto.UserEvent.newBuilder()
                        .setUserId(userId)
                        .setEventType(eventTypes[random.nextInt(eventTypes.length)])
                        .setTimestamp(System.currentTimeMillis())
                        .setSessionId("session_" + random.nextInt(10000))
                        .setPage("/page/" + random.nextInt(1000))
                        .setUserAgent("Mozilla/5.0 (compatible; KafkaBot/1.0)")
                        .setIpAddress("192.168.1." + random.nextInt(255))
                        .setCountry("IND")
                        .setDevice("desktop")
                        .build();
                sendProtobufMessage(userEventsKafkaTemplate, userEventsProtobufTopic, userId, userEvent);
                messageCounter.increment();
            } catch (Exception e) {
                logger.error("Error producing user event protobuf message", e);
            } finally {
                timer.stop(messageProductionTime);
                MDC.clear();
            }
        });
    }

    private <T> void sendProtobufMessage(KafkaTemplate<String, T> template, String topic,
                                         String key, T message) {
       try{
           CompletableFuture<SendResult<String, T>> future = template.send(topic, key, message);
           future.thenAccept((result) -> {
               logger.info("Sent protobuf message to topic {} partition {} with offset {}",
                       result.getRecordMetadata().topic(),
                       result.getRecordMetadata().partition(),
                       result.getRecordMetadata().offset());

           }).exceptionally(ex -> {
               logger.error("Failed to send protobuf message to topic {}", topic, ex);
               meterRegistry.counter("kafka.producer.failures", "format", "protobuf", "topic", topic).increment();
               return null;
           });
       }catch(Exception ex) {
           logger.error("Error sending protobuf message to topic {}: key {}", topic,key, ex);
           meterRegistry.counter("kafka.producer.errors", "format", "protobuf", "topic", topic).increment();
       }
    }


    private double generateStockPrice(String symbol) {
        double basePrice = switch (symbol) {
            case "TCS" -> 150.0;
            case "INFY" -> 2800.0;
            case "TSLA" -> 800.0;
            case "META" -> 250.0;
            case "NVDA" -> 400.0;
            case "NFLX" -> 450.0;
            default -> 100.0;
        };

        double variation = (random.nextGaussian() * 0.02 + 1);
        return Math.round(basePrice * variation * 100.0) / 100.0;
    }

    private double generateSensorValue() {
        return Math.round((random.nextGaussian() * 10 + 25) * 100.0) / 100.0;
    }

    @PreDestroy
    public void shutdown(){
        executorService.shutdown();
        try {
            if(!executorService.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)){
              executorService.shutdownNow();
              logger.warn("Executor did not terminate in time, forced shutdown");
            }
        }catch (Exception e){
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            logger.error("Shutdown interrupted", e);
        }
    }


}
