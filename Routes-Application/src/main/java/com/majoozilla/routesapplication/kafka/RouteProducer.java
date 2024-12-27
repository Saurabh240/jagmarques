package com.majoozilla.routesapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.routesapplication.routes.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RouteProducer {
    private static final Logger logger = LoggerFactory.getLogger(RouteProducer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.routes}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate; // Changed to KafkaTemplate<String, String>

    public RouteProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendRoute(Route route) {
        if (route.getId() == null) {
            throw new IllegalStateException("Route ID cannot be null");
        }
        try {
            String routeJson = objectMapper.writeValueAsString(route);
            kafkaTemplate.send(topic, route.getId().toString(), routeJson); // Send serialized JSON
            logger.info("Sending route to Kafka: {}", routeJson);
        } catch (Exception e) {
            logger.error("Failed to send route to Kafka: ", e);
        }
    }

    public void sendRouteInfoToDBInfoTopic(Route route) {
        try {
            String routeJson = objectMapper.writeValueAsString(route);
            kafkaTemplate.send("DBInfo", route.getId().toString(), routeJson);
            logger.info("Sent update to DBInfo topic: {}", routeJson);
        } catch (Exception e) {
            logger.error("Failed to send route update to DBInfo topic: ", e);
        }
    }
}
