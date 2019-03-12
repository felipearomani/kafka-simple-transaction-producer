package com.github.felipearomani.simplekafkaproducer;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerSimpleTransaction {

    private int numberOfPartition;
    private volatile boolean doneConsuming = false;
    private ExecutorService executorService;

    public static void main(String[] args) {

    }

    public ConsumerSimpleTransaction(int numberOfPartition) {
        this.numberOfPartition = numberOfPartition;
    }

    public void startConsuming() {
        executorService = Executors.newFixedThreadPool(numberOfPartition);
        Properties consumerProperties = getConsumerProperties();

        // crias as threads de acordo com o numberOfPartitions
    }

    private Runnable getConsumerThread(Properties properties) {
        return () -> {
            try (Consumer<PurchaseKey, String> consumer = new KafkaConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList("transactions"));

                while (!doneConsuming) {
                    ConsumerRecords<PurchaseKey, String> records = consumer.poll(5000);
                    records.forEach(System.out::println);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    public Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhosto:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-print-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // TODO preciso criar o deserializer aqui
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return properties;
    }
}
