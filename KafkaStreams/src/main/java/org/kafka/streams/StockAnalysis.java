package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.streams.model.StockMovingAverage;
import org.kafka.streams.model.VolumeAnalyzer;
import org.kafka.streams.processor.VolatilityProcessor;
import org.kafka.streams.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Component
public class StockAnalysis {

    private static final Logger logger = LoggerFactory.getLogger(StockAnalysis.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${producer.topics.stock-protobuf}")
    private String stockProtobufTopic;

    @Value("${output.topics.stock-moving-average-alerts}")
    private String stockMovingAverageAlerts;

    @Value("${output.topics.volume-surge-alerts}")
    private String volumeSurgeAlerts;

    public void processAdvancedStockAnalytics(StreamsBuilder builder, KafkaProtobufSerde<StockPriceProto.StockPrice> stockProtobufSerde){
        KStream<String,StockPriceProto.StockPrice> stockPriceStream =builder.stream(stockProtobufTopic, Consumed.with(Serdes.String(), stockProtobufSerde));

        StoreBuilder<KeyValueStore<String, String>> volatilityStateStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("volatility-state-store"),
                        Serdes.String(),
                        Serdes.String()
                );
        builder.addStateStore(volatilityStateStoreBuilder);

        //Stock price moving average calculation
        stockPriceStream.groupBy((key,stock) -> stock.getSymbol()).windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1))).
                aggregate(()-> new StockMovingAverage(),(key,stock,aggregate) -> aggregate.updateAverage(stock.getPrice(),stock.getTimestamp()),
                        Materialized.<String, StockMovingAverage, WindowStore<Bytes, byte[]>>as("stock-moving-averages")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(StockMovingAverage.class))
                ).
                toStream().
                filter((key,average) -> average.hasSignificantChange()).
                mapValues(this::createMovingAverageAlert)
                .to(stockMovingAverageAlerts);

        stockPriceStream
                .groupBy((key, stock) -> stock.getSymbol())
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        () -> new VolumeAnalyzer(),
                        (key, stock, analyzer) -> analyzer.addVolume(stock.getVolume()),
                        Materialized.<String,VolumeAnalyzer,WindowStore<Bytes,byte[]>>as("volume-analysis")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(VolumeAnalyzer.class))
                )
                .toStream()
                .filter((key, analyzer) -> analyzer.hasVolumeSurge())
                .mapValues(this::createVolumeSurgeAlert)
                .to(volumeSurgeAlerts);

        stockPriceStream
                .process(() -> new VolatilityProcessor(), "volatility-state-store");

    }

    private String createMovingAverageAlert(StockMovingAverage movingAvg) {
        logger.info("Check if null movingAvg"+movingAvg);
        try {
            Map<String, Object> alert = new HashMap<>();
            alert.put("alertType", "MOVING_AVERAGE_CHANGE");
            alert.put("symbol", movingAvg.getSymbol());
            alert.put("currentAverage", movingAvg.getCurrentAverage());
            alert.put("previousAverage", movingAvg.getPreviousAverage());
            alert.put("changePercent", movingAvg.getChangePercent());
            alert.put("timestamp", System.currentTimeMillis());

            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating moving average alert", e);
            return "{}";
        }
    }

    private String createVolumeSurgeAlert(VolumeAnalyzer analyzer) {
        try {
            Map<String, Object> alert = Map.of(
                    "alertType", "VOLUME_SURGE",
                    "symbol", analyzer.getSymbol(),
                    "currentVolume", analyzer.getCurrentVolume(),
                    "averageVolume", analyzer.getAverageVolume(),
                    "surgeMultiplier", analyzer.getSurgeMultiplier(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating volume surge alert", e);
            return "{}";
        }
    }



}
