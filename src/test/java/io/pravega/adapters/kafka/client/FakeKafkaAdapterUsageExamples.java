package io.pravega.adapters.kafka.client;

import io.pravega.adapters.kafka.client.common.ChecksumUtils;
import io.pravega.adapters.kafka.client.consumer.FakeKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.FakeKafkaProducer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Contains examples demonstrating the use of the fake Kafka producer and consumer implementations of the the
 * Kafka Adapter.
 */
public class FakeKafkaAdapterUsageExamples {

    @Test
    public void produceUsesFakeProducer() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyUrl");
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
        kafkaConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyUrl");
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

    @Test
    public void produceUsesFakeConsumer() {
        Properties consumerConfig = new Properties();

        consumerConfig.put("bootstrap.servers", System.getProperty("bootstrap.servers", "localhost:9092"));
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        Consumer myConsumer = new FakeKafkaConsumer(consumerConfig);
        myConsumer.subscribe(Arrays.asList("testtopic"));
        try {
            System.out.println("Polling");
            ConsumerRecords<String, String> records = myConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Consumed a record containing value: " + record.value());
            }
        } finally {
            myConsumer.close();
        }
    }
}
