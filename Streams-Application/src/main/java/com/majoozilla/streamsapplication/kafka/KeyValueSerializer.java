package com.majoozilla.streamsapplication.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

import java.util.Map;

public class KeyValueSerializer implements Serializer<KeyValue<String, Double>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, KeyValue<String, Double> data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing KeyValue", e);
        }
    }

    @Override
    public void close() {}
}
