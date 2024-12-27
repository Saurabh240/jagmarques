package com.majoozilla.streamsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TripSerializer implements Serializer<Trip> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, Trip trip) {
        try {
            return objectMapper.writeValueAsBytes(trip);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Trip object", e);
        }
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
