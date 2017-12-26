package com.douevencode.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Set;

@SpringBootApplication
public class KafkaSpringNoAnnotationsApplication {

    private final MyKafkaProducer producer;
    private final MyKafkaConsumer consumer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringNoAnnotationsApplication.class, args);
    }

    public KafkaSpringNoAnnotationsApplication() {
        this("http://localhost:50523", "consumerTopic", "producerConfig");
    }

    public KafkaSpringNoAnnotationsApplication(String brokerAddress, String consumerTopic, String producerTopic) {
        consumer = new MyKafkaConsumer(brokerAddress, consumerTopic);
        consumer.start();

        producer = new MyKafkaProducer(brokerAddress, producerTopic);
    }

    public void sendMessage(String message) {
        producer.send(message);
    }

    public Set<String> consumedMessages() {
        return consumer.consumedMessages;
    }
}
