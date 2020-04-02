/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.dataaccess.PravegaReader;
import io.pravega.adapters.kafka.client.testutils.ConfigMaker;
import io.pravega.client.stream.impl.JavaSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class AdapterUsageAdvancedExamples {

    @Test
    public void testSendThenReceiveMultipleMessagesFromSingleTopic() throws ExecutionException, InterruptedException {
        String scopeName = "one-topic-multiple-msgs" + Math.random();
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
                String readMessage = record.value();
                log.info("Consumed a record containing value: " + readMessage);
            }
            assertEquals(20, records.count());
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testReceivesPartialSetOfMessagesUponTimeout() {
        String scopeName = "partial-messages" + Math.random();
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
            while (reader.readNextEvent(500).getEvent() != null) {
                actualCountOfItemsInStream++;
            }
        }
        log.debug("Actual no. of items in stream = {}", actualCountOfItemsInStream);

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
                String readMessage = record.value();
                log.info("Consumed a record containing value: {}" + readMessage);
            }
            assertTrue(records.count() < actualCountOfItemsInStream);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testReceivesIncrementalMessagesOnPoll() {
        String scopeName = "incremental-polls-" + Math.random();
        String topicName = "test-topic-";
        String controllerUri = "tcp://localhost:9090";

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", controllerUri);
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("pravega.scope", scopeName);

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 200; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

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

        // Inspect the output to check if the behavior is as expected. You should see something like below in the
        // console output. Poll 3 starts from where poll 2 left. And, poll 2 starts from where poll 1 left.
        //
        //   Poll 1 - record: message-0
        //   ...
        //   Poll 2 - record: message-189
        //   Poll 3 - record: message-190
        //   ...
        //
        try {
            ConsumerRecords<String, String> recordSet1 = consumer.poll(Duration.ofMillis(200));
            ConsumerRecords<String, String> recordSet2 = consumer.poll(Duration.ofMillis(50));
            ConsumerRecords<String, String> recordSet3 = consumer.poll(Duration.ofMillis(200));

            if (recordSet1 == null || recordSet1.isEmpty()) {
                log.info("No data found in record set 1");
            } else {
                for (ConsumerRecord<String, String> record : recordSet1) {
                    log.info("Poll 1 - record: {}", record.value());
                }
            }

            if (recordSet2 == null || recordSet2.isEmpty()) {
                log.info("No data found in record set 2");
            } else {
                for (ConsumerRecord<String, String> record : recordSet2) {
                    log.info("Poll 2 - record: {}", record.value());
                }
            }

            if (recordSet3 == null || recordSet3.isEmpty()) {
                log.info("No data found in record set 3");
            } else {
                for (ConsumerRecord<String, String> record : recordSet3) {
                    log.info("Poll 3 - record: {}", record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testReceivesIncrementalMessagesOnPollFromDifferentConsumersHavingSameName() {
        String scope = "incremental-polls-" + Math.random();
        String topic = "test-topic-";
        String controllerUri = "tcp://localhost:9090";
        String consumerGroupId = "test-cg";
        String clientId = "test-cid";

        Properties producerConfig = ConfigMaker.makeProducerProperties(scope, controllerUri, null, null);

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 200; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

        // Consume events
        Properties consumerConfig = ConfigMaker.makeConsumerProperties(scope, controllerUri, consumerGroupId, clientId);

        // Inspect the output to check if the behavior is as expected. You should see something like below in the
        // console output. Poll 3 starts from where poll 2 left. And, poll 2 starts from where poll 1 left.
        //
        //   Poll 1 - record: message-0
        //   ...
        //   Poll 2 - record: message-189
        //   Poll 3 - record: message-190
        //   ...
        //
        ConsumerRecords<String, String> recordSet1 = getConsumerRecords(topic, consumerConfig, Duration.ofMillis(400));
        ConsumerRecords<String, String> recordSet2 = getConsumerRecords(topic, consumerConfig, Duration.ofMillis(400));
        ConsumerRecords<String, String> recordSet3 = getConsumerRecords(topic, consumerConfig, Duration.ofMillis(400));

        if (recordSet1 == null || recordSet1.isEmpty()) {
            log.info("No data found in record set 1");
        } else {
            for (ConsumerRecord<String, String> record : recordSet1) {
                log.info("Poll 1 - record: {}", record.value());
            }
        }

        if (recordSet2 == null || recordSet2.isEmpty()) {
            log.info("No data found in record set 2");
        } else {
            for (ConsumerRecord<String, String> record : recordSet2) {
                log.info("Poll 2 - record: {}", record.value());
            }
        }

        if (recordSet3 == null || recordSet3.isEmpty()) {
            log.info("No data found in record set 3");
        } else {
            for (ConsumerRecord<String, String> record : recordSet3) {
                log.info("Poll 3 - record: {}", record.value());
            }
        }
    }

    // Not working yet
    @Test
    public void testReceivesMessagesFromMultipleTopics() {
        String scope = "multiple-topics" + Math.random();
        String topic1 = "test-topic-1";
        String topic2 = "test-topic-2";
        String controllerUri = "tcp://localhost:9090";
        String consumerGroupId = "test-cg";
        String clientId = "test-cid";

        Properties producerConfig = ConfigMaker.makeProducerProperties(scope, controllerUri, null, null);

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 2; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic1, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

        try (Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 2; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic2, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.flush();
        }

        Properties consumerConfig = ConfigMaker.makeConsumerProperties(scope, controllerUri, consumerGroupId, clientId);

        try (Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig)) {
            consumer.subscribe(Arrays.asList(topic1, topic2));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                String readMessage = record.value();
                log.info("Consumed a record containing value: {}", readMessage);
            }
            assertEquals(4, records.count());
        }
    }

    @Test
    public void tailReadsExample() throws ExecutionException, InterruptedException {
        String topic = "test-topic" + Math.random();
        String controllerUri = "tcp://localhost:9090";
        String consumerGroupId = "cgId";
        String clientId = "clientId";

        Properties producerConfig =
                ConfigMaker.makeProducerProperties(null, controllerUri, null, null);
        Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig);
        for (int i = 0; i < 5; i++) {
             ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "message-" + i);
             producer.send(producerRecord).get();
        }

        Properties consumerConfig = ConfigMaker.makeConsumerProperties(null, controllerUri, consumerGroupId, clientId);
        try (Consumer<String, String> consumerOfGroup1 = new PravegaKafkaConsumer(consumerConfig)) {
            consumerOfGroup1.subscribe(Arrays.asList(topic));

            consumerOfGroup1.seekToEnd(Arrays.asList(new TopicPartition(topic, 0)));
            ConsumerRecords<String, String> recordSet1 = consumerOfGroup1.poll(Duration.ofMillis(5000));

            // assertEquals(0, recordSet1.count());

            for (ConsumerRecord<String, String> record : recordSet1) {
                String readMessage = record.value();
                log.info("Consumed a record containing value recordSet1: {}", readMessage);
            }

            producer.send(new ProducerRecord<>(topic, "message-new-" + 6)).get();
            producer.send(new ProducerRecord<>(topic, "message-new-" + 7)).get();

            ConsumerRecords<String, String> recordSet2 = consumerOfGroup1.poll(Duration.ofMillis(5000));
            assertEquals(2, recordSet2.count());

            for (ConsumerRecord<String, String> record : recordSet2) {
                String readMessage = record.value();
                log.info("Consumed a record containing value: {}", readMessage);
            }
        }
    }

    private ConsumerRecords<String, String> getConsumerRecords(String topicName,
                                                               Properties consumerConfig, Duration duration) {
        try (Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig)) {
            consumer.subscribe(Arrays.asList(topicName));
            return consumer.poll(duration);
        }
    }
}
