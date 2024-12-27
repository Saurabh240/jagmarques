package com.majoozilla.streamsapplication.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerde {
    public static Serde<KeyValue<String, Double>> getKeyValueSerde() {
        return Serdes.serdeFrom(new KeyValueSerializer(), new KeyValueDeserializer());
    }
}
