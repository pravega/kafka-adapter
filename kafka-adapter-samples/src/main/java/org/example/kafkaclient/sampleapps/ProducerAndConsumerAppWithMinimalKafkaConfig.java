/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.example.kafkaclient.sampleapps;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import static org.example.kafkaclient.shared.Utils.loadConfigFromClasspath;

@Slf4j
public class ProducerAndConsumerAppWithMinimalKafkaConfig {
    private static final Properties APP_CONFIG = loadConfigFromClasspath("app.properties");

    public static void main(String... args) {
        String topic = APP_CONFIG.getProperty("topic.name");
        String message = "My important message 1";

        produce(topic, message);
        consume(topic, message);

        log.info("Done. Exiting...");
        System.exit(0);
    }

    private static void produce(String topic, String message) {
        // Prepare producer configuration
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", APP_CONFIG.getProperty("bootstrap.servers"));
        producerConfig.put("key.serializer", APP_CONFIG.getProperty("key.serializer"));
        producerConfig.put("value.serializer", APP_CONFIG.getProperty("value.serializer"));

        // Initialize a Kafka producer
        Producer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig);

        // Setup a record that we want to send
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);

        // Asynchronously send a producer record via the producer
        Future<RecordMetadata> recordMedataFuture = kafkaProducer.send(producerRecord);
        try {
            RecordMetadata kafkaRecordMetadata = recordMedataFuture.get();
            log.info("Done sending the producer record, received record metadata");

            assert kafkaRecordMetadata != null;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered an exception", e);
            System.exit(-1);
        } finally {
            kafkaProducer.close();
        }
    }

    private static void consume(String topic, String expectedMessage) {

        // Prepare the consumer configuration
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", APP_CONFIG.getProperty("bootstrap.servers"));
        consumerConfig.put("group.id", APP_CONFIG.getProperty("group.id"));
        consumerConfig.put("client.id", APP_CONFIG.getProperty("client.id"));
        consumerConfig.put("auto.offset.reset", APP_CONFIG.getProperty("auto.offset.reset"));
        consumerConfig.put("key.deserializer", APP_CONFIG.getProperty("key.deserializer"));
        consumerConfig.put("value.deserializer", APP_CONFIG.getProperty("value.deserializer"));

        // Initialize a Kafka consumer
        Consumer<String, String> kafkaConsumer = new KafkaConsumer(consumerConfig);

        // Have the consumer subscribe to the topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        try {
            // Read the records from the topic
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            log.debug("Done receiving the records");

            // Assert
            assert records.count() == 1;
            for (ConsumerRecord<String, String> record : records) {
                log.info("Consumed a record containing value: [{}]", record.value());
                assert record.value().equals(expectedMessage);
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
