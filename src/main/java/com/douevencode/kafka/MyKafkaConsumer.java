package com.douevencode.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class MyKafkaConsumer {

    private final String brokerAddress;
    private final String topic;

    Set<String> consumedMessages = new HashSet<>();

    MyKafkaConsumer(String brokerAddress, String topic) {
        this.brokerAddress = brokerAddress;
        this.topic = topic;
    }

    void start() {
        MessageListener<String, String> messageListener = record -> consumedMessages.add(record.value());

        ConcurrentMessageListenerContainer container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory(brokerAddress),
                        containerProperties(topic, messageListener));

        container.start();
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(String brokerAddress) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(brokerAddress),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private ContainerProperties containerProperties(String topic, MessageListener<String, String> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(messageListener);
        return containerProperties;
    }

    private Map<String, Object> consumerConfig(String brokerAddress) {
        return Map.of(
                    BOOTSTRAP_SERVERS_CONFIG, brokerAddress,
                    GROUP_ID_CONFIG, "groupId",
                    AUTO_OFFSET_RESET_CONFIG, "earliest"
            );
    }
}
