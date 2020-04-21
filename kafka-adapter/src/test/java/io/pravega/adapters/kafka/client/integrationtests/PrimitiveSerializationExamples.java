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
import io.pravega.adapters.kafka.client.dataaccess.PravegaReader;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.testutils.ConfigExtractor;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class PrimitiveSerializationExamples {

    private static final String SCOPE_NAME = "CustomSerializationUsageExamples";

    @Test
    public void sendMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();

        producerConfig.put("bootstrap.servers", ConfigExtractor.extractBootstrapServers());
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerConfig.put("pravega.scope", SCOPE_NAME);

        Producer<String, Integer> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Integer> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", 20);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<Integer> reader = new PravegaReader<>(SCOPE_NAME, topic, "tcp://localhost:9090",
                new JavaSerializer<Integer>(), UUID.randomUUID().toString(), "readerId")) {
            Integer readInteger = reader.readNext(200);
            log.info("Read Person: {}", readInteger);
            assertEquals(20, readInteger.intValue());
        }
    }

    @Test
    public void testSendThenReceiveAnIntMessageInDefaultScope() throws ExecutionException, InterruptedException {
        String topic = "test-topic-" + Math.random();
        int message = 5;
        String bootstrapServers = ConfigExtractor.extractBootstrapServers();

        // Produce events
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        Producer<String, Integer> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Integer> producerRecord =
                new ProducerRecord<>(topic, message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());
        log.info("Done producing message: {}", message);
        pravegaKafkaProducer.close();

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, Integer> records = consumer.poll(1000);
            Integer consumedMessage = null;
            for (ConsumerRecord<String, Integer> record : records) {
                consumedMessage = record.value();
                log.info("Consumed a record containing value: {}", consumedMessage);
            }
            assertEquals(1, records.count());
            assertEquals(message, consumedMessage.intValue());
        } finally {
            consumer.close();
        }
    }
}
