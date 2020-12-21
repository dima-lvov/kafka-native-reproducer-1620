package demo;

import java.util.stream.IntStream;

public class KafkaConsumerRunner {

    public static void main(String[] args) {
        IntStream.rangeClosed(1, 8)
                .mapToObj(DemoConsumer::new)
                .peek(DemoConsumer::subscribe)
                .forEach(Thread::start);
    }
}
