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

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

@Slf4j
@RequiredArgsConstructor
public abstract class BasicReadShowcase {
    final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    @Getter
    private final boolean isCreateTestData;

    @NonNull
    @Getter
    private final String bootstrapServer;

    @NonNull
    @Getter
    private final String clientId;

    @NonNull
    @Getter
    private final String stream;

    @NonNull
    @Getter
    private final DeserializationSchema deserializationSchema;

    // The template method.
    protected abstract void createTestData();

    @SneakyThrows
    final void execute() {
        log.info("Starting the app");
        if (isCreateTestData) {
            createTestData();
            log.info("Created test data");
        }

        Properties connectorConfig = new Properties();
        connectorConfig.setProperty("bootstrap.servers", this.getBootstrapServer());
        connectorConfig.setProperty("group.id", UUID.randomUUID().toString());
        connectorConfig.setProperty("client.id", this.getClientId());
        connectorConfig.setProperty("auto.offset.reset", "earliest");
        connectorConfig.setProperty("flink.poll-timeout", "2000");
        connectorConfig.setProperty("request.timeout.ms", "1000");
        connectorConfig.setProperty("flink.disable-metrics", "true");
        connectorConfig.setProperty("enable.auto.commit", "false");
        connectorConfig.setProperty("heartbeat.timeout", "550000");
        log.info("Created connector properties");

        // Instantiating the Kafka consumer streaming data source
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(Arrays.asList(this.getStream()),
                this.deserializationSchema, connectorConfig);
        log.info("Instantiated Flink Kafka consumer");

        // This will invoke seekToBeginning of the Consumer, which is currently not supported.
        // flinkKafkaConsumer.setStartFromEarliest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);
        stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                log.debug("Received message: {}", s);
                return true;
            }
        }).print();

        env.execute();
        log.info("Exiting App...");
    }
}
