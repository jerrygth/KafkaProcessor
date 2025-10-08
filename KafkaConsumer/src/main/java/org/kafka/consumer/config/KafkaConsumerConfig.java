package org.kafka.consumer.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${consumer.group-id.stock:stock-consumer-group}")
    private String stockConsumerGroupId;

    @Value("${consumer.group-id.user-events:user-events-consumer-group}")
    private String userEventsConsumerGroupId;

    @Value("${consumer.group-id.sensors:sensors-consumer-group}")
    private String sensorsConsumerGroupId;

    @Value("${consumer.group-id.weather:weather-consumer-group}")
    private String weatherConsumerGroupId;


    private final MeterRegistry meterRegistry;

    public KafkaConsumerConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // Create a default generic consumer factory for Spring Boot
    @Bean
    @Primary
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "default-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Override Spring Boot's default factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    @Bean
    public ConsumerFactory<String, StockPriceProto.StockPrice> protobufStockConsumerFactory() {
        Map<String, Object> props = createBaseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, stockConsumerGroupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaProtobufDeserializer.class.getName());
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockPriceProto.StockPrice.class.getName());

        // Schema Registry configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("use.latest.version", true);

        DefaultKafkaConsumerFactory<String, StockPriceProto.StockPrice> factory =
                new DefaultKafkaConsumerFactory<>(props);

        // Add Micrometer metrics
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        logger.info("Created Protobuf Stock Consumer Factory with group: {}", stockConsumerGroupId);
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, StockPriceProto.StockPrice> protobufStockKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StockPriceProto.StockPrice> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(protobufStockConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        logger.info("Created Protobuf Stock Kafka Listener Container Factory");
        return factory;
    }

    @Bean
    public ConsumerFactory<String, UserEventProto.UserEvent> protobufUserEventConsumerFactory() {
        Map<String, Object> props = createBaseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, userEventsConsumerGroupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaProtobufDeserializer.class.getName());
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserEventProto.UserEvent.class.getName());

        // Schema Registry configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("use.latest.version", true);

        DefaultKafkaConsumerFactory<String, UserEventProto.UserEvent> factory =
                new DefaultKafkaConsumerFactory<>(props);

        // Add Micrometer metrics
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        logger.info("Created Protobuf User Event Consumer Factory with group: {}", userEventsConsumerGroupId);
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, UserEventProto.UserEvent> protobufUserEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, UserEventProto.UserEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(protobufUserEventConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        logger.info("Created Protobuf User Event Kafka Listener Container Factory");
        return factory;
    }


    @Bean
    public ConsumerFactory<String, GenericRecord> avroSensorConsumerFactory() {
        Map<String, Object> props = createBaseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, sensorsConsumerGroupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false); // Use GenericRecord

        // Schema Registry configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("use.latest.version", true);

        DefaultKafkaConsumerFactory<String, GenericRecord> factory =
                new DefaultKafkaConsumerFactory<>(props);

        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        logger.info("Created Avro Sensor Consumer Factory with group: {}", sensorsConsumerGroupId);
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> avroSensorKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(avroSensorConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        logger.info("Created Avro Sensor Listener Container Factory ");
        return factory;
    }

    @Bean
    public ConsumerFactory<String, GenericRecord> avroWeatherConsumerFactory() {
        Map<String, Object> props = createBaseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, weatherConsumerGroupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false); // Use GenericRecord

        // Schema Registry configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("use.latest.version", true);

        DefaultKafkaConsumerFactory<String, GenericRecord> factory =
                new DefaultKafkaConsumerFactory<>(props);

        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        logger.info("Created Avro Weather Consumer Factory with group: {}", weatherConsumerGroupId);
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> avroWeatherKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(avroWeatherConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        logger.info("Created Avro Weather Listener Container Factory");
        return factory;
    }

    private Map<String, Object> createBaseConsumerProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

}
