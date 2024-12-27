package com.majoozilla.tripsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.tripsapplication.route.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class RouteCache {
    private static final Logger log = LoggerFactory.getLogger(RouteCache.class);
    private final ConcurrentHashMap<String, Route> routes = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "Routes", groupId = "trip-group")
    public void receiveRoute(String message) {
        try {
            Route route = objectMapper.readValue(message, Route.class);
            if (route.getId() != null) {
                routes.put(route.getId().toString(), route);
            } else {
                log.error("Received route with null ID, skipping...");
            }
        } catch (Exception e) {
            log.error("Failed to deserialize route message: " + message, e);
        }
    }

    public Route getRandomRoute() {
        return routes.values().stream()
                .skip((int) (routes.size() * Math.random()))
                .findFirst()
                .orElse(null);
    }
}
