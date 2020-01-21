package kafka_java_clients_api;

import java.util.Properties;
import java.io.FileReader;
import java.io.IOException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerFactory {
    public static Consumer getConsumer(String propertiesPath) throws IOException {
        FileReader file = new FileReader(propertiesPath);
        Properties props = new Properties();
        props.load(file);

        return new KafkaConsumer<>(props);
    }
}
