package io.pravega.adapters.kafka.client.producer;

import io.pravega.adapters.kafka.client.common.ChecksumUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@Slf4j
public class FakeKafkaProducerUsageExamples {

    @Test
    public void produceUsesFakeProducer() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"dummyUrl");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);


        Producer<String, String> fakeKafkaProducer = new FakeKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test-topic", 1, "test-key", "test-value");

        Future<RecordMetadata> recordMedata = fakeKafkaProducer.send(producerRecord);
        assertEquals(ChecksumUtils.computeCRC32Checksum(producerRecord.toString()), recordMedata.get().checksum());
    }

    @Test
    public void producedMessageIsInterceptedByFakeInterceptor() throws ExecutionException, InterruptedException {
        Properties kafkaConfigProperties = new Properties();
        kafkaConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"dummyUrl");
        kafkaConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaConfigProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);

        kafkaConfigProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
              Arrays.asList("io.pravega.adapters.kafka.client.producer.FakeProducerInterceptor"));

        Producer<String, String> fakeKafkaProducer = new FakeKafkaProducer<>(kafkaConfigProperties);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test-topic", 1, "test-key", "test-value");

        Future<RecordMetadata> recordMedata = fakeKafkaProducer.send(producerRecord);
        assertEquals(ChecksumUtils.computeCRC32Checksum(producerRecord.toString()), recordMedata.get().checksum());
    }
}
