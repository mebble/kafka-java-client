package kafka_java_clients_api;

import java.util.Properties;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.time.Duration;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {
    private static int producerCount = 0;

    private int producerId;
    private Producer producer;

    MyProducer(String propertiesPath) throws IOException {
        FileReader file = new FileReader(propertiesPath);
        Properties props = new Properties();
        props.load(file);

        this.producer = new KafkaProducer<>(props);
        this.producerId = producerCount;

        producerCount++;
    }
}