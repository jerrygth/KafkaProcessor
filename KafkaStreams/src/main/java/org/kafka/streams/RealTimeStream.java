package org.kafka.streams;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import jakarta.annotation.PostConstruct;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Component
public class RealTimeStream {

    private static final Logger logger = LoggerFactory.getLogger(RealTimeStream.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    StockAnalysis stockAnalysisProcessor;

    @Autowired
    UserActivityAnalytics userActivityAnalytics;

    @Autowired
    IoTAnomalyDetection ioTAnomalyDetection;

    @Autowired
    CrossFormatJoins crossFormatJoins;

    @Autowired
    RealTimeAggregations  realTimeAggregations;

    private KafkaStreams streams;
    private final CountDownLatch latch = new CountDownLatch(1);
    @PostConstruct
    public void startStreaming() throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "realtime-analytics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Schema Registry configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", false);

        StreamsBuilder builder = new StreamsBuilder();
        buildMultiFormatTopology(builder);

        Topology topology = builder.build();
        logger.info("Advanced Topology description: {}", topology.describe());

        streams = new KafkaStreams(topology, props);

        // Add state listener with metrics
        streams.setStateListener((newState, oldState) -> {
            logger.info("Kafka Streams state changed from {} to {}", oldState, newState);

            if (newState == KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        // Add uncaught exception handler
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            logger.error("Uncaught exception in stream processing", throwable);
        });

        streams.start();
        latch.await();
        logger.info("Advanced Kafka Streams with Schema Registry started");
    }

    private void buildMultiFormatTopology(StreamsBuilder builder){
        Map serdePropsProtoBufStock = createBaseSerdeProps();
        serdePropsProtoBufStock.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockPriceProto.StockPrice.class.getName());
        KafkaProtobufSerde<StockPriceProto.StockPrice> stockProtobufSerde = new KafkaProtobufSerde<>();
        stockProtobufSerde.configure(serdePropsProtoBufStock,false);

        Map serdePropsProtoBufUserEvent = createBaseSerdeProps();
        serdePropsProtoBufUserEvent.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserEventProto.UserEvent.class.getName());
        KafkaProtobufSerde<UserEventProto.UserEvent> userEventProtobufSerde = new KafkaProtobufSerde<>();
        userEventProtobufSerde.configure(serdePropsProtoBufUserEvent, false);


        Map serdePropsAvro = createBaseSerdeProps();
        Serde<GenericRecord> avroSerde = new GenericAvroSerde();
        avroSerde.configure(serdePropsAvro, false);

        stockAnalysisProcessor.processAdvancedStockAnalytics(builder,stockProtobufSerde);
        userActivityAnalytics.processUserJourneyAnalytics(builder,userEventProtobufSerde);
        ioTAnomalyDetection.processIoTAnomalyDetection(builder,avroSerde);
        crossFormatJoins.processCrossFormatJoins(builder,stockProtobufSerde,userEventProtobufSerde,avroSerde);
        realTimeAggregations.processRealTimeAggregations(builder,stockProtobufSerde,avroSerde);

    }

    private Map<String, Object> createBaseSerdeProps() {
        Map<String, Object> baseProps = new HashMap<>();
        baseProps.put("schema.registry.url", schemaRegistryUrl);
        return baseProps;
    }


}
