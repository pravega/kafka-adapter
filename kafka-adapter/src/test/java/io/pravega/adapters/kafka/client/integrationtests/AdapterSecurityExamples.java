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

import io.grpc.StatusRuntimeException;
import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.testutils.ConfigExtractor;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class AdapterSecurityExamples {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void testBasicAuthUsingJavaSystemProperties() throws ExecutionException, InterruptedException {
        /* Typical Security params for invoking the default Password Auth Handler */

        // We don't want to load a custom credentials object dynamically from a jar provided in classpath.
        // This property defaults to `false` anyway.
        System.setProperty("pravega.client.auth.loadDynamic", "false");

        // Use `Basic` authentication method
        System.setProperty("pravega.client.auth.method", "Basic");

        // Credentials token in format that Basic Authentication can use: base64encoded(username:password)
        System.setProperty("pravega.client.auth.token", "YWRtaW46MTExMV9hYWFh");

        try {
            produceAndConsumeMessage();
        } finally {
            System.clearProperty("pravega.client.auth.loadDynamic");
            System.clearProperty("pravega.client.auth.method");
            System.clearProperty("pravega.client.auth.token");
        }
    }

    @Test
    public void testBasicAuthUsingEnvironmentVariables() throws ExecutionException, InterruptedException {
        /* Typical Security params for invoking the default Password Auth Handler */

        // We don't want to load a custom credentials object dynamically from a jar provided in classpath.
        // This property defaults to `false` anyway.
        environmentVariables.set("pravega_client_auth_loadDynamic", "false");

        // Use `Basic` authentication method
        environmentVariables.set("pravega_client_auth_method", "Basic");

        // Credentials token in format that Basic Authentication can use: base64encoded(username:password)
        environmentVariables.set("pravega_client_auth_token", "YWRtaW46MTExMV9hYWFh");

        produceAndConsumeMessage();

        environmentVariables.clear("pravega_client_auth_loadDynamic", "pravega_client_auth_method",
                "pravega_client_auth_token");
    }

    @Test (expected = StatusRuntimeException.class)
    public void testThatSendingMessageFailsWithoutCredentials() throws ExecutionException, InterruptedException {
        String topic = "tpmkc-" + Math.random();
        String message = "test-message-1";
        String bootstrapServers = "tcp://localhost:9090";
        produceAMessage(topic, message, bootstrapServers);
    }

    private void produceAndConsumeMessage() throws InterruptedException, ExecutionException {
        String topic = "tpmkc-" + Math.random();
        String message = "test-message-1";
        String bootstrapServers = ConfigExtractor.extractBootstrapServers();
        produceAMessage(topic, message, bootstrapServers);
        consumeAMessage(topic, message, bootstrapServers);
    }

    private void consumeAMessage(String topic, String message, String bootstrapServers) {
        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig)) {
            consumer.subscribe(Arrays.asList(topic));
            ConsumerRecords<String, String> records = consumer.poll(1000);
            assertEquals(1, records.count());
            for (ConsumerRecord<String, String> record : records) {
                assertEquals(message, record.value());
            }
        }
    }

    private void produceAMessage(String topic, String message, String bootstrapServers) throws InterruptedException, ExecutionException {
        // Produce events
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig)) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, 1, "test-key", message);

            Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
            assertNotNull(recordMedata.get());
            log.info("Done producing message: {}", message);
        }
    }
}
