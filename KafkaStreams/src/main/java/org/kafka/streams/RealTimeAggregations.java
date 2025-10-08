package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
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
import org.kafka.streams.model.MarketOverview;
import org.kafka.streams.model.SensorStatistics;
import org.kafka.streams.serde.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class RealTimeAggregations {

    private static final Logger logger = LoggerFactory.getLogger(RealTimeAggregations.class);
    @Value("${producer.topics.stock-protobuf}")
    private String stockProtobufTopic;

    @Value("${output.topics.market-overview-feed}")
    private String marketOverviewFeed;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void processRealTimeAggregations(StreamsBuilder builder,
                                          KafkaProtobufSerde<StockPriceProto.StockPrice> stockSerde,
                                          Serde<GenericRecord> avroSerde) {

        StoreBuilder<KeyValueStore<String, GenericRecord>> complexAggregateStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("complex-aggregates"),
                        Serdes.String(),
                        avroSerde
                );
        builder.addStateStore(complexAggregateStoreBuilder);

        // Real-time market overview aggregation
        KStream<String, StockPriceProto.StockPrice> stockStream =
                builder.stream(stockProtobufTopic, Consumed.with(Serdes.String(), stockSerde));

        stockStream
                .groupBy((key, stock) -> "market-overview")
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        () -> new MarketOverview(),
                        (key, stock, overview) -> overview.updateWithStock(stock),
                        Materialized.<String, MarketOverview, WindowStore<Bytes,byte[]>>as("market-overview")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(MarketOverview.class))
                )
                .toStream()
                .mapValues(this::createMarketSummary)
                .to(marketOverviewFeed);

        logger.info("Real-time aggregations topology configured");
    }

    private String createMarketSummary(MarketOverview overview) {
        try {
            Map<String, Object> summary = Map.of(
                    "totalVolume", overview.getTotalVolume(),
                    "averagePrice", overview.getAveragePrice(),
                    "topGainers", overview.getTopGainers(),
                    "topLosers", overview.getTopLosers(),
                    "marketTrend", overview.getMarketTrend(),
                    "activeSymbols", overview.getActiveSymbols(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(summary);
        } catch (Exception e) {
            logger.error("Error creating market summary", e);
            return "{}";
        }
    }
}
