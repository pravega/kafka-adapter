/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import shared.PravegaWriter;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class BasicReadExample {

    private final static String SCOPE = "test-scope";
    private final static String STREAM = "test-stream";

    private final static String CONTROLLER_URI = "tcp://localhost:9090";
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) throws Exception {
        log.info("Starting the app");
        setupData();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaConsumerClientProps = new Properties();
        kafkaConsumerClientProps.setProperty("bootstrap.servers", CONTROLLER_URI);
        kafkaConsumerClientProps.setProperty("group.id", UUID.randomUUID().toString());
        kafkaConsumerClientProps.setProperty("client.id", "BasicReadExampleApp");
        // kafkaConsumerClientProps.setProperty("auto.offset.reset", appProperties.getProperty("kafka.auto.offset.reset"));
        // kafkaConsumerClientProps.setProperty("max.poll.records", appProperties.getProperty("kafka.max.poll.records"));
        // kafkaConsumerClientProps.setProperty("flink.poll-timeout", appProperties.getProperty("flink.poll-timeout"));
        // kafkaConsumerClientProps.setProperty("request.timeout.ms", "1000");
        // kafkaConsumerClientProps.setProperty("check.crcs", "false");

        // Instantiating the Kafka consumer streaming data source
        FlinkKafkaConsumer<String> flinkConsumer = new FlinkKafkaConsumer<String>(Arrays.asList(STREAM),
                new SimpleStringSchema(), kafkaConsumerClientProps);
        log.debug("Instantiated flink consumer");

        DataStream<String> stream = env.addSource(flinkConsumer);
        stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                log.debug("Received message: {}", s);
                return true;
            } });

        env.execute();
        log.info("Exiting App...");
    }

    static {
        setupData();
    }

    private static void setupData() {
        try (PravegaWriter<String> writer = new PravegaWriter<String>(SCOPE, STREAM, CONTROLLER_URI,
                new JavaSerializer<String>(), 1)) {
            for (int i = 0; i < 10; i++) {
                writer.writeEvent("message " + i).join();
                log.debug("Wrote message {}", i);
            }
            writer.flush();
        }
    }
}
