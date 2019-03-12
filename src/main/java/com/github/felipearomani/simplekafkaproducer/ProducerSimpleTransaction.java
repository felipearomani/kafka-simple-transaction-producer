package com.github.felipearomani.simplekafkaproducer;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import com.github.felipearomani.simplekafkaproducer.partitioners.PurchaseKeyPartitioner;
import com.github.felipearomani.simplekafkaproducer.serializers.PurchaseKeySerializer;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerSimpleTransaction {

    private Logger logger = LoggerFactory.getLogger(ProducerSimpleTransaction.class);

    public static void main(String[] args) {
        new ProducerSimpleTransaction().run();
    }

    private void run() {

        Properties config = getProducerConfig();
        PurchaseKey key = new PurchaseKey("12334568", new Date());

        try(KafkaProducer<PurchaseKey, String> producer = new KafkaProducer<>(config)) {

            ProducerRecord<PurchaseKey, String> record =
                    new ProducerRecord<>("transactions", key, "{\"item\":\"book\",\"price\":10.99}");


            Future<RecordMetadata> resultFuture = producer.send(record, (recordMetadata, error) -> {
                if (error != null) {
                    logger.error(error.getMessage());
                }

                logger.info(recordMetadata.toString());
            });


        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    private Properties getProducerConfig() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PurchaseKeySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG,  "3");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PurchaseKeyPartitioner.class.getName());

        return config;
    }

}
