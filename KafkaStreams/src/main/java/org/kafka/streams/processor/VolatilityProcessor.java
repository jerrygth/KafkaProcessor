package org.kafka.streams.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafka.producer.model.protobuf.StockPriceProto;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VolatilityProcessor implements Processor<String, StockPriceProto.StockPrice, String, String> {

    private static Logger logger = LoggerFactory.getLogger(VolatilityProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> volatilityStore;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.volatilityStore = context.getStateStore("volatility-state-store");
    }

    @Override
    public void process(Record<String, StockPriceProto.StockPrice> record) {

        try {
            StockPriceProto.StockPrice stock = record.value();
            String symbol = stock.getSymbol();

            // Calculate volatility
            VolatilityCalculator calculator = getOrCreateVolatilityCalculator(symbol);
            calculator.addPrice(stock.getPrice(), stock.getTimestamp());

            if (calculator.isHighVolatility()) {
                String alert = createVolatilityAlert(symbol, calculator);
                context.forward(new Record<>(symbol, alert, record.timestamp()));
                logger.warn("High volatility detected for {}: {}", symbol, calculator.getVolatilityScore());
            }

            // Store updated calculator
            storeVolatilityCalculator(symbol, calculator);

        } catch (Exception e) {
            logger.error("Error processing volatility for record: {}", record, e);

        }
    }

    private VolatilityCalculator getOrCreateVolatilityCalculator(String symbol) {
        String stored = volatilityStore.get(symbol);
        return stored != null ? VolatilityCalculator.fromJson(stored) : new VolatilityCalculator();
    }

    private void storeVolatilityCalculator(String symbol, VolatilityCalculator calculator) {
        volatilityStore.put(symbol, calculator.toJson());
    }

    private String createVolatilityAlert(String symbol, VolatilityCalculator calculator) {
        try {
            Map<String, Object> alert = Map.of(
                    "alertType", "HIGH_VOLATILITY",
                    "symbol", symbol,
                    "volatilityScore", calculator.getVolatilityScore(),
                    "threshold", calculator.getVolatilityThreshold(),
                    "period", calculator.getPeriodMinutes(),
                    "timestamp", System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            logger.error("Error creating volatility alert", e);
            return "{}";
        }
    }

    public static class VolatilityCalculator {
        private List<Double> prices = new ArrayList<>();
        private List<Long> timestamps = new ArrayList<>();

        public void addPrice(double price, long timestamp) {
            prices.add(price);
            timestamps.add(timestamp);
            // Keep only last 20 prices for calculation
            if (prices.size() > 20) {
                prices.remove(0);
                timestamps.remove(0);
            }
        }

        public boolean isHighVolatility() {
            return getVolatilityScore() > getVolatilityThreshold();
        }

        public double getVolatilityScore() {
            if (prices.size() < 2) return 0.0;

            double sum = 0.0;
            for (int i = 1; i < prices.size(); i++) {
                double change = Math.abs(prices.get(i) - prices.get(i-1)) / prices.get(i-1);
                sum += change;
            }
            return sum / (prices.size() - 1);
        }

        public double getVolatilityThreshold() { return 0.02; } // 2% volatility threshold
        public int getPeriodMinutes() { return 5; }

        public String toJson() {
            try {
                return VolatilityProcessor.objectMapper.writeValueAsString(this);
            } catch (Exception e) {
                return "{}";
            }
        }

        public static VolatilityCalculator fromJson(String json) {
            try {
                return VolatilityProcessor.objectMapper.readValue(json, VolatilityCalculator.class);
            } catch (Exception e) {
                logger.error("Failed to deserialize VolatilityCalculator from: {}", json, e);
                // Alert operations team
                throw new RuntimeException("State store corruption detected", e);
            }
        }
    }

}
