package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.client.stream.impl.JavaSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdapterUsageAdvancedExamples {

    @Test
    public void testSendThenReceiveMultipleMessagesFromSingleTopic() throws ExecutionException, InterruptedException {
        String scopeName = "multiple-messages";
        String topic = "test-topic-" + Math.random();

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("pravega.scope", scopeName);

        Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig);

        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, 1, "test-key", "message-" + i);

            // Sending synchronously
            producer.send(producerRecord).get();
        }

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("pravega.scope", scopeName);

        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                String readPerson = record.value();
                System.out.println("Consumed a record containing value: " + readPerson);
            }
            assertEquals(20, records.count());
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testReceivesPartialSetOfWhenTimeoutExceedsTimeToFetchAll() {
        String scopeName = "multiple-messages";

        String topicName = "test-topic-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", controllerUri);
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("pravega.scope", scopeName);

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 50; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

        int actualCountOfItemsInStream = 0;
        // Finding out how many were really written
        try (PravegaReader reader = new PravegaReader(scopeName, topicName, controllerUri, new JavaSerializer<String>(),
                "some-reader-group" + Math.random(), "some-reader-id")) {
            while (reader.readNextEvent().getEvent() != null) {
                actualCountOfItemsInStream++;
            }
        }
        System.out.println("Actual no. of items in stream = " + actualCountOfItemsInStream);

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", controllerUri);
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("pravega.scope", scopeName);

        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                String readPerson = record.value();
                System.out.println("Consumed a record containing value: " + readPerson);
            }
            assertTrue(records.count() < actualCountOfItemsInStream);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testReceivesIncrementalMessagesOnPoll() {
        String scopeName = "multiple-messages";
        String topicName = "test-topic-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", controllerUri);
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("pravega.scope", scopeName);

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 50; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

        int actualCountOfItemsInStream = 0;
        // Finding out how many were really written
        try (PravegaReader reader = new PravegaReader(scopeName, topicName, controllerUri, new JavaSerializer<String>(),
                "some-reader-group" + Math.random(), "some-reader-id")) {
            while (reader.readNextEvent().getEvent() != null) {
                actualCountOfItemsInStream++;
            }
        }
        System.out.println("Actual no. of items in stream = " + actualCountOfItemsInStream);

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", controllerUri);
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("pravega.scope", scopeName);

        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                String readPerson = record.value();
                System.out.println("Consumed a record containing value: " + readPerson);
            }
            assertTrue(records.count() < actualCountOfItemsInStream);
        } finally {
            consumer.close();
        }
    }
}
