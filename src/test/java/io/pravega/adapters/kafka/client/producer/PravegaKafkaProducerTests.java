package io.pravega.adapters.kafka.client.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

public class PravegaKafkaProducerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsImpty() {
        Properties props = new Properties();
        new PravegaKafkaProducer<>(props);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
    }

    private Properties prepareDummyMinimalConfig() {
        Properties result = new Properties();
        result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.pravega.client.stream.impl.JavaSerializer");
        return result;
    }
}
