/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.samplesapps.flinkconnector;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaBasicReadShowcase extends BasicReadShowcase {

    public KafkaBasicReadShowcase(boolean isCreateTestData, @NonNull String bootstrapServer, @NonNull String clientId,
                                  String topic) {
        super(isCreateTestData, bootstrapServer, clientId, topic, new SimpleStringSchema());
    }

    @Override
    protected final void createTestData() {
        log.debug("Sending test data to Pravega");

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServer());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 10; i++) {
                String message = "message-" + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.getStream(), message);
                producer.send(producerRecord).get();
                log.debug("Sent message to Kafka");
            }
            producer.flush();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Encountered exception", e);
        }
        log.info("Done writing 10 messages to Kafka topic {}", this.getStream());
    }

    public static void main(String[] args) {
        String bootstrapServers = System.getProperty("bootstrapservers", "localhost:9092");
        log.info("Evaluated bootstrap server=[{}]", bootstrapServers);

        KafkaBasicReadShowcase driver = new KafkaBasicReadShowcase(false, bootstrapServers,
                "test-client", "kafka-topic");
        driver.execute();
    }
}
