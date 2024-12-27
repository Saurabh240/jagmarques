package com.majoozilla.streamsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TripDeserializer implements Deserializer<Trip> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public Trip deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Trip.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Trip object", e);
        }
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
