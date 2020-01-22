package kafka_java_clients_api;

public class Main {
    public static void main(String[] args) throws java.io.IOException {
        String resourcesPath = System.getProperty("user.dir") + "/src/main/resources/";

        MyConsumer consumer1 = new MyConsumer(resourcesPath + "consumer1.properties");
        MyConsumer consumer2 = new MyConsumer(resourcesPath + "consumer2.properties");

        consumer1.subscribe(new String[] { "test-topic" });
        consumer2.subscribe(new String[] { "test-topic" });

        Thread consumer1Thread = new Thread(consumer1);
        Thread consumer2Thread = new Thread(consumer2);
        consumer1Thread.start();
        consumer2Thread.start();
    }
}
