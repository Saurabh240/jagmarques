package com.majoozilla.streamsapplication.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

import java.nio.ByteBuffer;
import java.util.Map;

public class TripsKeyValueSerde implements Serde<KeyValue<String, Long>> {

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Long> longSerde = Serdes.Long();

    @Override
    public Serializer<KeyValue<String, Long>> serializer() {
        return (topic, data) -> {
            byte[] keyBytes = stringSerde.serializer().serialize(topic, data.key);
            byte[] valueBytes = longSerde.serializer().serialize(topic, data.value);

            ByteBuffer buffer = ByteBuffer.allocate(keyBytes.length + valueBytes.length + Integer.BYTES * 2);
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);

            return buffer.array();
        };
    }

    @Override
    public Deserializer<KeyValue<String, Long>> deserializer() {
        return (topic, data) -> {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);

            int valueLength = buffer.getInt();
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            String key = stringSerde.deserializer().deserialize(topic, keyBytes);
            Long value = longSerde.deserializer().deserialize(topic, valueBytes);

            return new KeyValue<>(key, value);
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration
    }

    @Override
    public void close() {
        // No resources to close
    }
}
