package com.majoozilla.streamsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.models.Route;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RouteSerializer implements Deserializer<Route> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public Route deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Route.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Trip object", e);
        }
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
