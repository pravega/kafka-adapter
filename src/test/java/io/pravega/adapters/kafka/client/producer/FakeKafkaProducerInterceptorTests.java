package io.pravega.adapters.kafka.client.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class FakeKafkaProducerInterceptorTests {

    @Test
    public void producerSendMessageIsIntercepted() {
        Properties kafkaConfigProperties = new Properties();
        kafkaConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"dummyUrl");
        kafkaConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaConfigProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);

        kafkaConfigProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                Arrays.asList("io.pravega.adapters.kafka.client.producer.FakeProducerInterceptor"));

        FakeKafkaProducer<String, String> fakeKafkaProducer = new FakeKafkaProducer<>(kafkaConfigProperties);
        fakeKafkaProducer.send(new ProducerRecord<>("test-topic", 1, "test-key", "test-value"));


    }
}
