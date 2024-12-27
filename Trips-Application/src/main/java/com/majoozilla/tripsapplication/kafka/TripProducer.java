package com.majoozilla.tripsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.tripsapplication.trips.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TripProducer {
    private static final Logger logger = LoggerFactory.getLogger(TripProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.trips}")
    private String topic;

    public TripProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTrip(Trip trip) {
        try {
            String tripJson = objectMapper.writeValueAsString(trip);
            String key = trip.getRouteId();  // Use route_id as the Kafka message key
            kafkaTemplate.send(topic, key, tripJson);
            logger.info("Sending trip to Kafka with key {}: {}", key, tripJson);
        } catch (Exception e) {
            logger.error("Error sending trip data to Kafka: ", e);
        }
    }
}
