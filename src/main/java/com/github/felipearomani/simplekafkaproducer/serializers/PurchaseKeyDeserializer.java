package com.github.felipearomani.simplekafkaproducer.serializers;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PurchaseKeyDeserializer implements Deserializer<PurchaseKey> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public PurchaseKey deserialize(String topic, byte[] data) {
        return SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {}
}
