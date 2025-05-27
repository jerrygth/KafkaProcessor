package org.kafka.consumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);


    @KafkaListener(topics = "high-frequency-data", groupId = "data-group")
    public void consume(String message){
        try {
            logger.info("Received message: {}", message);
            // Simulate processing

        } catch (Exception e) {
            logger.error("Error processing message: {}", message, e);
        }
    }
}
