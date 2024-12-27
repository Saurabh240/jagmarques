package com.majoozilla.streamsapplication.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A custom Serde implementation for KeyValue<String, Long>.
 */
public class KeyValueSerdeLong implements Serde<KeyValue<String, Long>> {

    private final Serde<KeyValue<String, Long>> inner;

    /**
     * Constructor initializes custom serializer and deserializer.
     */
    public KeyValueSerdeLong() {
        this.inner = Serdes.serdeFrom(new KeyValueSerializer(), new KeyValueDeserializer());
    }

    @Override
    public Serializer<KeyValue<String, Long>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<KeyValue<String, Long>> deserializer() {
        return inner.deserializer();
    }

    /**
     * Serializer implementation for KeyValue<String, Long>.
     */
    static class KeyValueSerializer implements Serializer<KeyValue<String, Long>> {

        @Override
        public byte[] serialize(String topic, KeyValue<String, Long> data) {
            if (data == null) {
                return null;
            }

            byte[] keyBytes = Serdes.String().serializer().serialize(topic, data.key);
            byte[] valueBytes = Serdes.Long().serializer().serialize(topic, data.value);

            if (keyBytes == null || valueBytes == null) {
                throw new IllegalArgumentException("Key or Value serialization resulted in null.");
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + keyBytes.length + valueBytes.length);
            buffer.putInt(keyBytes.length); // Store the length of the key
            buffer.put(keyBytes);          // Store the key bytes
            buffer.put(valueBytes);        // Store the value bytes

            return buffer.array();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No specific configuration required
        }

        @Override
        public void close() {
            // No resources to close
        }
    }

    /**
     * Deserializer implementation for KeyValue<String, Long>.
     */
    static class KeyValueDeserializer implements Deserializer<KeyValue<String, Long>> {

        private static final int LONG_SIZE = Long.BYTES; // 8 bytes for a Long

        @Override
        public KeyValue<String, Long> deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            if (data.length == Long.BYTES) {
                // Handle case where only a Long is present (legacy or unexpected data)
                Long value = buffer.getLong();
                return new KeyValue<>(null, value);
            }

            if (data.length < Integer.BYTES) {
                throw new IllegalArgumentException("Invalid data size: " + data.length);
            }

            int keySize = buffer.getInt();
            if (data.length < Integer.BYTES + keySize + Long.BYTES) {
                throw new IllegalArgumentException("Invalid data size: " + data.length);
            }

            byte[] keyBytes = new byte[keySize];
            buffer.get(keyBytes);
            byte[] valueBytes = new byte[buffer.remaining()];
            buffer.get(valueBytes);

            String key = Serdes.String().deserializer().deserialize(topic, keyBytes);
            Long value = Serdes.Long().deserializer().deserialize(topic, valueBytes);

            return new KeyValue<>(key, value);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No specific configuration required
        }

        @Override
        public void close() {
            // No resources to close
        }
    }
}
