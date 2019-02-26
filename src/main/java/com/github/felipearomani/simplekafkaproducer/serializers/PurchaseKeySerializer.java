package com.github.felipearomani.simplekafkaproducer.serializers;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PurchaseKeySerializer implements Serializer<PurchaseKey> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, PurchaseKey data) {
        return SerializationUtils.serialize(data);
    }

    public void close() {

    }
}
