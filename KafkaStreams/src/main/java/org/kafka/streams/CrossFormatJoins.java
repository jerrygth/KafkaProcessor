package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.kafka.streams.model.StockUserInterestCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;


@Component
public class CrossFormatJoins {

    private static final Logger logger = LoggerFactory.getLogger(CrossFormatJoins.class);

    @Value("${producer.topics.stock-protobuf}")
    private String stockProtobufTopic;

    @Value("${producer.topics.user-events-protobuf}")
    private String userEventsProtobufTopic;

    @Value("${output.topics.stock-interest-correlations}")
    private String stockInterestCorrelations;



    private final ObjectMapper objectMapper = new ObjectMapper();


    public void processCrossFormatJoins(StreamsBuilder builder,
                                         KafkaProtobufSerde<StockPriceProto.StockPrice> stockSerde,
                                         KafkaProtobufSerde<UserEventProto.UserEvent> userEventSerde,
                                        Serde<GenericRecord> avroSerde) {

        KStream<String, StockPriceProto.StockPrice> stockStream =
                builder.stream(stockProtobufTopic, Consumed.with(Serdes.String(), stockSerde));

        KStream<String, UserEventProto.UserEvent> userStream =
                builder.stream(userEventsProtobufTopic, Consumed.with(Serdes.String(), userEventSerde));

        // Join user search events with stock price movements
        KStream<String, UserEventProto.UserEvent> searchEvents = userStream
                .filter((key, event) -> "SEARCH".equals(event.getEventType()))
                .selectKey((key, event) -> extractStockSymbolFromSearch(event));

        stockStream
            .selectKey((key, stock) -> stock.getSymbol())
            .join(
                searchEvents,
                this::correlateStockPriceWithUserInterest,
                JoinWindows.of(Duration.ofMinutes(2)),
                StreamJoined.with(Serdes.String(), stockSerde, userEventSerde)
            )
            .mapValues(this::createInterestCorrelation)
            .to(stockInterestCorrelations);

        logger.info("Cross-format joins topology configured");
    }

    private String extractStockSymbolFromSearch(UserEventProto.UserEvent event) {
        // Extract stock symbol from search query - simplified implementation
        String[] symbols = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX"};
        if(event == null || event.getPage() == null) {
            logger.warn("Null event or page received");
            return null;
        }
        String query = event.getPage().toUpperCase(); // Simplified - assuming page contains symbol
        return Arrays.stream(symbols).filter(s->query.contains(s)).findFirst().orElse(null);
    }

    private StockUserInterestCorrelation correlateStockPriceWithUserInterest(
            StockPriceProto.StockPrice stock, UserEventProto.UserEvent userEvent) {
        return new StockUserInterestCorrelation(
                stock.getSymbol(),
                stock.getPrice(),
                userEvent.getUserId(),
                userEvent.getTimestamp(),
                stock.getTimestamp()
        );
    }

    private String createInterestCorrelation(StockUserInterestCorrelation correlation) {
        try {
            Map<String, Object> corr = Map.of(
                    "symbol", correlation.getSymbol(),
                    "stockPrice", correlation.getStockPrice(),
                    "userId", correlation.getUserId(),
                    "timeLag", correlation.getTimeLag(),
                    "correlationType", "USER_SEARCH_STOCK_PRICE",
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(corr);
        } catch (Exception e) {
            logger.error("Error creating interest correlation", e);
            return "{}";
        }
    }

}
