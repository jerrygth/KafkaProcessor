package org.kafka.producer;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

@Configuration
@EnableScheduling
public class MessageProducerAvro {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerAvro.class);
    private MeterRegistry meterRegistry;
    private Counter messageCounter;
    private Timer messageProductionTime;
    private ExecutorService executorService;

    private KafkaTemplate<String, GenericRecord> avroSensorTemplate;

    private KafkaTemplate<String, GenericRecord> avroWeatherTemplate;

    // Avro schemas
    private Schema sensorSchema;
    private Schema weatherSchema;

    private Random random = new Random();

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${producer.topics.sensors-avro}")
    private String sensorsAvroTopic;

    @Value("${producer.topics.weather-avro}")
    private String weatherAvroTopic;




    MessageProducerAvro(MeterRegistry meterRegistry){
        this.meterRegistry = meterRegistry;
        this.messageCounter =  Counter.builder("kafka.producer.avro.messages")
                .description("Number of avro messages produced")
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
                        Thread t = new Thread(r, "kafka-avro-producer-" + threadNumber.getAndIncrement());
                        t.setDaemon(false);
                        return t;
                    }
                },
                new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @PostConstruct
    public void initializeKafkaTemplate(){
        initializeAvroTemplates();
        logger.info("Avro format Kafka Producer initialized with Schema Registry: {}", schemaRegistryUrl);
    }

    private void initializeAvroTemplates() {
        Map<String, Object> avroProps = createBaseProducerProps();
        avroProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        avroProps.put("schema.registry.url", schemaRegistryUrl);
        avroProps.put("auto.register.schemas", true);
        avroProps.put("use.latest.version", true);

        // Sensor Avro template
        DefaultKafkaProducerFactory<String, GenericRecord> sensorFactory =
                new DefaultKafkaProducerFactory<>(avroProps);
        this.avroSensorTemplate = new KafkaTemplate<>(sensorFactory);

        // Weather Avro template
        DefaultKafkaProducerFactory<String, GenericRecord> weatherFactory =
                new DefaultKafkaProducerFactory<>(avroProps);
        this.avroWeatherTemplate = new KafkaTemplate<>(weatherFactory);

        // Initialize Avro schemas
        initializeAvroSchemas();
    }

    private void initializeAvroSchemas() {
        // IoT Sensor Schema
        String sensorSchemaString = """
        {
          "type": "record",
          "name": "IoTSensorData",
          "namespace": "org.kafka.avro.sensor",
          "fields": [
            {"name": "sensorId", "type": "string"},
            {"name": "sensorType", "type": "string"},
            {"name": "location", "type": "string"},
            {"name": "value", "type": "double"},
            {"name": "unit", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "batteryLevel", "type": "int"},
            {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
          ]
        }
        """;

        // Weather Schema
        String weatherSchemaString = """
        {
          "type": "record",
          "name": "WeatherData",
          "namespace": "org.kafka.avro.weather",
          "fields": [
            {"name": "stationId", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "humidity", "type": "double"},
            {"name": "pressure", "type": "double"},
            {"name": "windSpeed", "type": "double"},
            {"name": "windDirection", "type": "int"},
            {"name": "timestamp", "type": "long"},
            {"name": "location", "type": {
              "type": "record",
              "name": "GeoLocation",
              "fields": [
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"}
              ]
            }}
          ]
        }
        """;

        this.sensorSchema = new Schema.Parser().parse(sensorSchemaString);
        this.weatherSchema = new Schema.Parser().parse(weatherSchemaString);
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

    @Scheduled(fixedRate = 5000)
    public void produceSensorDataAvro(){
        this.executorService.submit(()->{
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                MDC.put("messageType", "sensor-avro");
                MDC.put("format", "avro");

                String[] sensorTypes = {"temperature", "humidity", "pressure", "light", "motion", "air_quality"};
                String[] locations = {"warehouse_1", "warehouse_2", "office_floor_1", "office_floor_2", "parking_lot"};

                GenericRecord sensorData = new GenericData.Record(sensorSchema);
                String sensorId = "sensor_" + random.nextInt(500);

                sensorData.put("sensorId", sensorId);
                sensorData.put("sensorType", sensorTypes[random.nextInt(sensorTypes.length)]);
                sensorData.put("location", locations[random.nextInt(locations.length)]);
                sensorData.put("value", generateSensorValue());
                sensorData.put("unit", "°C");
                sensorData.put("timestamp", System.currentTimeMillis());
                sensorData.put("batteryLevel", random.nextInt(100) + 1);

                Map<String, String> metadata = new HashMap<>();
                metadata.put("firmware_version", "1.2." + random.nextInt(10));
                metadata.put("calibration_date", "2024-01-15");
                sensorData.put("metadata", metadata);

                sendAvroMessage(avroSensorTemplate, sensorsAvroTopic, sensorId, sensorData);
                messageCounter.increment();

            } catch (Exception e) {
                logger.error("Error producing avro weather data", e);
            } finally {
                sample.stop(messageProductionTime);
                MDC.clear();
            }
        });
    }

    // Weather data using Avro (every 5 seconds)
    @Scheduled(fixedRate = 5000)
    public void produceWeatherDataAvro() {
        executorService.submit(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);

            try {
                MDC.put("messageType", "weather-avro");
                MDC.put("format", "avro");

                GenericRecord weatherData = new GenericData.Record(weatherSchema);
                GenericRecord location = new GenericData.Record(weatherSchema.getField("location").schema());

                String stationId = "station_" + random.nextInt(100);

                // Location
                location.put("latitude", 40.7128 + (random.nextGaussian() * 0.1));
                location.put("longitude", -74.0060 + (random.nextGaussian() * 0.1));

                // Weather data
                weatherData.put("stationId", stationId);
                weatherData.put("temperature", 20.0 + (random.nextGaussian() * 15));
                weatherData.put("humidity", 50.0 + (random.nextGaussian() * 20));
                weatherData.put("pressure", 1013.25 + (random.nextGaussian() * 10));
                weatherData.put("windSpeed", Math.abs(random.nextGaussian() * 10));
                weatherData.put("windDirection", random.nextInt(360));
                weatherData.put("timestamp", System.currentTimeMillis());
                weatherData.put("location", location);

                sendAvroMessage(avroWeatherTemplate, weatherAvroTopic, stationId, weatherData);
                messageCounter.increment();

            } catch (Exception e) {
                logger.error("Error producing avro weather data", e);
            } finally {
                sample.stop(messageProductionTime);
                MDC.clear();
            }
        });
    }

    private void sendAvroMessage(KafkaTemplate<String, GenericRecord> kafkaTemplate, String topic, String key, GenericRecord value) {
          try {
            CompletableFuture<SendResult<String,GenericRecord>> future =kafkaTemplate.send(topic, key, value);
            future.thenAccept(result -> {
                logger.info("Sent avro message to topic={} partition={} offset={} : key={} value={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key, value);
            }).exceptionally(ex -> {
                logger.error("Failed to send avro message to {}: key={}", topic, key, ex);
                meterRegistry.counter("kafka.producer.failures", "format", "avro", "topic", topic).increment();
                return null;
            });
        } catch (Exception e) {
            logger.error("Exception while sending avro message with key={} value={}", key, value, e);
              meterRegistry.counter("kafka.producer.errors","format", "avro", "topic", topic).increment();
        }

    }

    private double generateSensorValue() {
        return Math.round((random.nextGaussian() * 10 + 25) * 100.0) / 100.0;
    }

}
