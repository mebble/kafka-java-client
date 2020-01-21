package kafka_java_clients_api;

import java.util.Arrays;
import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Main {
    public static void main(String[] args) throws java.io.IOException {
        String consumerPropertiesPath = System.getProperty("user.dir") + "/src/main/resources/consumer.properties";
        String producerPropertiesPath = System.getProperty("user.dir") + "/src/main/resources/producer.properties";

        Consumer consumer = ConsumerFactory.getConsumer(consumerPropertiesPath);
        consumer.subscribe(Arrays.asList("test-topic"));

        // run the kafka consumer
        System.out.println("Running the kafka consumer...");
        while (true) {
            System.out.println("polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("----- Record: -----");
                System.out.println("key: " + record.key());
                System.out.println("value: " + record.value());
                System.out.println("partition: " + record.partition());
                System.out.println("offset: " + record.offset());
                System.out.println("-------------------");
            }
            consumer.commitAsync();
        }
//        consumer.close();
    }
}
