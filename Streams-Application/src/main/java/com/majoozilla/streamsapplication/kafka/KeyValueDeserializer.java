package com.majoozilla.streamsapplication.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueDeserializer implements Deserializer<KeyValue<String, Double>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public KeyValue<String, Double> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(data);
            String key = node.get("key").asText();
            Double value = node.get("value").asDouble();
            return new KeyValue<>(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing KeyValue", e);
        }
    }
}
