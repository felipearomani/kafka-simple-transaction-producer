package com.github.felipearomani.simplekafkaproducer;

import com.github.felipearomani.simplekafkaproducer.models.PurchaseKey;
import com.github.felipearomani.simplekafkaproducer.serializers.PurchaseKeyDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ConsumerSimpleTransaction {

    private int numberOfPartition;
    private volatile boolean doneConsuming = false;
    private ExecutorService executorService;
    private static Logger logger = LoggerFactory.getLogger(ConsumerSimpleTransaction.class);

    public static void main(String[] args) throws InterruptedException {

        ConsumerSimpleTransaction consumers = new ConsumerSimpleTransaction(3);
        consumers.startConsuming();
        Thread.sleep(20000);
        consumers.stopConsuming();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown is comming!");
            try {
                consumers.stopConsuming();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Bye bye!");
        }));
    }

    private ConsumerSimpleTransaction(int numberOfPartition) {
        this.numberOfPartition = numberOfPartition;
    }

    private void startConsuming() {
        executorService = Executors.newFixedThreadPool(numberOfPartition);
        Properties consumerProperties = getConsumerProperties();

        IntStream.of(numberOfPartition)
                .forEach(index -> executorService.submit(getConsumerThread(consumerProperties)));
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

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-print-group-new");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, PurchaseKeyDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return properties;
    }

    private void stopConsuming() throws InterruptedException {
        doneConsuming = true;
        executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        executorService.shutdown();
    }
}
