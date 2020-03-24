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
import io.pravega.adapters.kafka.client.testutils.Person;
import io.pravega.adapters.kafka.client.testutils.PersonSerializer;

import java.nio.ByteBuffer;
import java.time.Duration;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class CustomSerializationUsageExamples {

    private static final String SCOPE_NAME = "CustomSerializationUsageExamples";

    private static final Person PERSON_OBJ = new Person("John", "Doe", "jdoe");

    @Test
    public void sendAndReceiveMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();

        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        producerConfig.put("pravega.scope", SCOPE_NAME);

        Producer<String, Person> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Person> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", PERSON_OBJ);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        consumerConfig.put("pravega.scope", SCOPE_NAME);

        Consumer<String, Person> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, Person> record : records) {
                Person readPerson = record.value();
                log.info("Consumed a record containing value: " + readPerson);
                assertEquals(PERSON_OBJ.getUserName(), readPerson.getUserName());
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void sendMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();

        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        producerConfig.put("pravega.scope", SCOPE_NAME);

        Producer<String, Person> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Person> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", PERSON_OBJ);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<Person> reader = new PravegaReader<>(SCOPE_NAME, topic, "tcp://localhost:9090",
                new PersonSerializer(), UUID.randomUUID().toString(), "readerId")) {
            Person readPerson = reader.readNext(200);
            log.info("Read Person: {}", readPerson);
            assertEquals("jdoe", PERSON_OBJ.getUserName());
        }
    }

    @Ignore
    @Test
    public void receiveAnExistingMessage() throws ExecutionException, InterruptedException {
        // Change to an existing topic before running this test
        String topic = "test-topic-0.8010902196015256";

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // We are configuring a Pravega serializer here.
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.pravega.adapters.kafka.client.utils.PersonSerializer");

        // Pravega-specific config
        consumerConfig.put("pravega.scope",
                "CustomSerializationUsageExamples.sendAndReceiveMessage");

        Consumer<String, Person> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, Person> record : records) {
                Person readPerson = record.value();
                log.info("Consumed a record containing value: {}", readPerson);
                assertEquals(PERSON_OBJ.getUserName(), readPerson.getUserName());
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * Demonstrates workings of a custom serialization/deserialization.
     */
    private static void serializeThenDeserialize() {
        Person person =  new Person("Ravi", "Sharda", "rsharda");
        PersonSerializer serializer = new PersonSerializer();
        log.info("Initial: {}", person);
        ByteBuffer serialized = serializer.serialize(person);
        Person deserialized = serializer.deserialize(serialized);
        log.info("De-serialized: {}", deserialized);
        log.info("Equals?" + person.equals(deserialized));
    }
}



