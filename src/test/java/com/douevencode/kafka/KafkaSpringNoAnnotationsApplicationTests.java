package com.douevencode.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaSpringNoAnnotationsApplicationTests {

    private static final String CONSUMER_TOPIC = "consumerTopic";
    private static final String PRODUCER_TOPIC = "producerTopic";

	@ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, false, CONSUMER_TOPIC, PRODUCER_TOPIC);

	private static KafkaSpringNoAnnotationsApplication app;

    @BeforeClass
    public static void before() {
        app = new KafkaSpringNoAnnotationsApplication(
                embeddedKafka.getBrokersAsString(),
                CONSUMER_TOPIC,
                PRODUCER_TOPIC);
    }

	@Test
	public void consumerReceivesMessage() throws Exception {
        String message = "Simple message";

        sendMessageToKafka(message, CONSUMER_TOPIC);

        await().atMost(1, SECONDS).until(() -> app.consumedMessages(), hasItem(message));
    }

    @Test
    public void producerProducesMessage() throws Exception {
        String producedMessage = "producedMessage";

        app.sendMessage(producedMessage);

        assertThat(getReceivedRecord(PRODUCER_TOPIC).value(), is(producedMessage));
    }

    private ConsumerRecord<String, String> getReceivedRecord(String topic) throws Exception {
        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps())
                        .createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
        return KafkaTestUtils.getSingleRecord(consumer, topic);
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "true", embeddedKafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private void sendMessageToKafka(String message, String topic) throws InterruptedException, java.util.concurrent.ExecutionException {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        Producer<Integer, String> producer = new DefaultKafkaProducerFactory<Integer, String>(props).createProducer();
        producer.send(new ProducerRecord<>(topic, message)).get();
    }
}