package kafka_java_clients_api;

import java.util.Properties;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

public class MyConsumer implements Runnable {
    private static int consumerCount = 0;

    private int consumerId;
    private Consumer consumer;

    MyConsumer(String propertiesPath) throws IOException {
        FileReader file = new FileReader(propertiesPath);
        Properties props = new Properties();
        props.load(file);

        this.consumer = new KafkaConsumer<>(props);
        this.consumerId = consumerCount;

        consumerCount++;
    }

    public void subscribe(String[] topics) {
        this.consumer.subscribe(Arrays.asList(topics));
    }

    @Override
    public void run() {
        System.out.printf("Running kafka consumer %d...\n", this.consumerId);
        try {
            while (true) {
                System.out.printf("consumer %d polling...\n", this.consumerId);
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("----- %d received: -----\n", this.consumerId);
                    System.out.printf("key: %s,\tvalue: %s,\tpartition: %d,\toffset: %d\n", record.key(), record.value(), record.partition(), record.offset());
                }
                this.consumer.commitAsync();
            }
        } catch (WakeupException e) {
            System.out.printf("WakeupException caught in consumer %d:\n" + e.getMessage(), this.consumerId);
        } finally {
            this.consumer.close();
            System.out.printf("Closed consumer %d\n", this.consumerId);
        }
    }
}
