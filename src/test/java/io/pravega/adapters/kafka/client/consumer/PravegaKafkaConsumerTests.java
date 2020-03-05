package io.pravega.adapters.kafka.client.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PravegaKafkaConsumerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsImpty() {
        Properties consumerProps = new Properties();
        new PravegaKafkaConsumer<>(consumerProps);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
    }

    @Test
    public void subscribeChangesSubscription() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        assertEquals(0, consumer.subscription().size());

        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-1"));
        assertTrue(subscribedTopics.contains("topic-2"));
    }

    @Test
    public void subscribeCalledMultipleTimeMakesLatestTopicsEffective() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));
        consumer.subscribe(Arrays.asList("topic-3", "topic-4"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-3"));
        assertTrue(subscribedTopics.contains("topic-4"));
    }

    private Properties prepareDummyCompleteConsumerConfig() {
        Properties result = new Properties();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.pravega.client.stream.impl.JavaSerializer");
        return result;
    }
}
