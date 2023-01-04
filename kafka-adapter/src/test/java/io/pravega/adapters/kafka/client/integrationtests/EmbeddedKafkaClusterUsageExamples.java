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

import io.pravega.adapters.kafka.client.testutils.FakeKafkaProducer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import kafka.utils.MockTime;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Demonstrates a basic usage of EmbeddedKafkaCluster.
 */
@Slf4j
public class EmbeddedKafkaClusterUsageExamples {

    @ClassRule
    public static final EmbeddedKafkaCluster KAFKA_CLUSTER = new EmbeddedKafkaCluster(1);

    private final MockTime mockTime = KAFKA_CLUSTER.time;

    @BeforeClass
    public static void createTopics() throws InterruptedException {
        KAFKA_CLUSTER.createTopic("test.topic.1");
        log.info("Done creating topic 1");

        KAFKA_CLUSTER.createTopic("test.topic.2");
        log.info("Done creating topic 2");
    }

    @Test
    public void reallyProduceData() throws Exception {
        String topic = "topic.1";

        // Prepare producer configuration
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "TestProducerApp");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);

        // Produce records
        IntegrationTestUtils.produceValuesSynchronously(topic, Arrays.asList("Message-1", "Message-2", "Message-3"),
                producerConfig, mockTime);

        // Prepare serialization configuration
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "TestConsumerApp");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int numRecords = 3;
        int waitTime = 40000;

        // Consume records
        List<KeyValue<String, String>> consumedRecords =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, numRecords, waitTime);
        assertEquals(numRecords, consumedRecords.size());
    }

    @Test
    public void reallyProduceData2() throws ExecutionException, InterruptedException {
        String topic = "topic.2";

        // Prepare producer configuration
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "TestProducerApp");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);

        // Produce records
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
        log.info("Done creating producer");

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", "test-value");

        Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
        RecordMetadata recordMetadata = recordMetadataFuture.get();

        log.info("Record metadata: {}", recordMetadata);
    }

    @Test
    public void useFakeProducer() throws Exception {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "TestAppWithFakeProducer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);

        FakeKafkaProducer<String, String> kafkaProducer = new FakeKafkaProducer<>(producerConfig);
        log.info("Done creating Kafka producer");

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test.topic.1", 1, "test-key", "test-value");

        Future<RecordMetadata> recordMedataFuture = kafkaProducer.send(producerRecord);
        assertEquals(producerRecord.partition().intValue(),
                recordMedataFuture.get().partition());
    }

}
