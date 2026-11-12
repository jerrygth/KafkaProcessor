package org.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.kafka.producer.model.protobuf.UserEventProto;
import org.kafka.streams.model.StockUserInterestCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Component
public class CrossFormatJoins {

    private static final Logger logger = LoggerFactory.getLogger(CrossFormatJoins.class);

    @Value("${producer.topics.stock-protobuf}")
    private String stockProtobufTopic;

    @Value("${producer.topics.user-events-protobuf}")
    private String userEventsProtobufTopic;

    @Value("${output.topics.stock-interest-correlations}")
    private String stockInterestCorrelations;

    private static final Pattern SEARCH_QUERY = Pattern.compile("[?&]q=([A-Za-z0-9.\\-]+)");


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
                .selectKey((key, event) -> extractStockSymbolFromSearch(event))
                .filter((symbol, event) -> symbol != null);

        stockStream
                .join(
                        searchEvents,
                        this::correlateStockPriceWithUserInterest,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)),
                        StreamJoined.with(Serdes.String(), stockSerde, userEventSerde)
                                .withName("stock-search-join")
                                .withStoreName("stock-search-join-store"))
                .mapValues(this::createInterestCorrelation)
                .to(stockInterestCorrelations);

        logger.info("Cross-format joins topology configured");
    }

    private String extractStockSymbolFromSearch(UserEventProto.UserEvent event) {
        // Extract stock symbol from search query - simplified implementation
        if (event == null) {
            logger.warn("Null event received");
            return null;
        }
        Matcher m = SEARCH_QUERY.matcher(event.getPage());
        return m.find() ? m.group(1).toUpperCase(Locale.ROOT) : null;
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
