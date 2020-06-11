/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.kafka.sampleapps;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

import io.pravega.kafka.shared.Utils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@RequiredArgsConstructor
@Slf4j
public class BasicKafkaApplicationShowcase implements BasicApplicationShowcase {

    @Getter
    private final Properties applicationConfig;

    public static void main(String... args) {
        BasicKafkaApplicationShowcase showcase = new BasicKafkaApplicationShowcase(
                Utils.loadConfigFromClasspath("app.properties"));

        String topic = showcase.getApplicationConfig().getProperty("topic.name");
        String message = "Test message - Hello world!";

        showcase.produce(message);
        showcase.consume(message);

        log.info("Done. Exiting...");
        System.exit(0);
    }

    @SneakyThrows
    public void produce(String message) {
        // Prepare producer configuration
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", applicationConfig.getProperty("bootstrap.servers"));
        producerConfig.put("key.serializer", applicationConfig.getProperty("key.serializer"));
        producerConfig.put("value.serializer", applicationConfig.getProperty("value.serializer"));
        String topic = applicationConfig.getProperty("topic.name");

        // Initialize a Kafka producer
        Producer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig);

        // Setup a record that we want to send
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);

        // Asynchronously send a producer record via the producer
        Future<RecordMetadata> responseFuture = kafkaProducer.send(producerRecord);

        try {
            // Wait for the write to complete
            RecordMetadata kafkaRecordMetadata = responseFuture.get();

            //region Any additional business logic here...
            log.info("Sent a producer record. Received reply: {}", kafkaRecordMetadata);
            //endregion
        } finally {
            kafkaProducer.close();
        }
    }

    public void consume(String expectedMessage) {
        // Prepare the consumer configuration
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", applicationConfig.getProperty("bootstrap.servers"));
        consumerConfig.put("group.id", applicationConfig.getProperty("group.id"));
        consumerConfig.put("client.id", applicationConfig.getProperty("client.id"));
        consumerConfig.put("auto.offset.reset", applicationConfig.getProperty("auto.offset.reset"));
        consumerConfig.put("key.deserializer", applicationConfig.getProperty("key.deserializer"));
        consumerConfig.put("value.deserializer", applicationConfig.getProperty("value.deserializer"));
        String topic = applicationConfig.getProperty("topic.name");

        // Initialize a Kafka consumer
        Consumer<String, String> kafkaConsumer = new KafkaConsumer(consumerConfig);

        // Have the consumer subscribe to the topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        try {
            // Read the records from the topic
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                //region Business logic here...
                assert record.value().equals(expectedMessage);
                log.info("Read message: {}", record.value());
                //endregion
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
