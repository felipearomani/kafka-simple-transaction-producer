package com.github.felipearomani.simplekafkaproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ConsumerSimpleTransaction {
    public static void main(String[] args) {

    }

    public void run() {

    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhosto:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-print-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return properties;

    }
}
