package com.github.felipearomani.simplekafkaproducer.partitioners;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

public class PurchaseKeyPartitioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Object newKey = null;

        if (key != null) {
            PurchaseKey purchaseKey = (PurchaseKey) key;
            newKey = purchaseKey.getCostumerId();
            keyBytes = ((String) newKey).getBytes();
        }

        return super.partition(topic, newKey, keyBytes, value, valueBytes, cluster);
    }
}
