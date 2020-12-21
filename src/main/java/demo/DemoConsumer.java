package demo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DemoConsumer extends ShutdownableThread {

    private static final Properties props = configureProps();

    private final int index;
    private final KafkaConsumer<Object, Object> consumer;

    public DemoConsumer(int index) {
        super("KafkaConsumerExample", false);
        this.index = index;
        this.consumer = new KafkaConsumer<>(props);
        System.out.println("Consumer#" + index + " created");
    }

    private static Properties configureProps() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "gp");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    @Override
    public void doWork() {
        System.out.println("Consumer#" + index + "is doing some work!!!");
        consumer.poll(Duration.ofMillis(30));
    }

    public void subscribe() {
        consumer.subscribe(Collections.singleton("foo" + index));
    }
}
