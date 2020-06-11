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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.kafka.shared.Utils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class BasicPravegaApplicationShowcase implements BasicApplicationShowcase {

    @Getter
    private final Properties applicationConfig;

    private StreamManager streamManager;

    public static void main(String... args) {
        BasicPravegaApplicationShowcase showcase = new BasicPravegaApplicationShowcase(
                Utils.loadConfigFromClasspath("app.properties"));

        String topic = showcase.getApplicationConfig().getProperty("topic.name");
        String message = "My important message 1";

        showcase.createScopeAndStream();

        showcase.produce(message);
        showcase.consume(message);

        if (showcase.streamManager != null) {
            showcase.streamManager.close();
        }
    }

    public void createScopeAndStream() {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(applicationConfig.getProperty("bootstrap.servers")))
                .build();
        String scope = applicationConfig.getProperty("scope.name");
        String stream = applicationConfig.getProperty("stream.name");

        streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(2))
                    .build();
        streamManager.createStream(scope, stream, streamConfig);
    }

    public void produce(String event) {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(applicationConfig.getProperty("bootstrap.servers")))
                .build();

        String scope = applicationConfig.getProperty("scope.name");
        String stream = applicationConfig.getProperty("stream.name");

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<String> pravegaWriter = clientFactory.createEventWriter(
                stream, new JavaSerializer<String>(), EventWriterConfig.builder().build());
        try {
            // Asynchronously, send an event to the stream
            CompletableFuture<Void> writeFuture = pravegaWriter.writeEvent(event);

            // Wait for the write to complete
            writeFuture.join();
            //region Any additional business logic here...
            log.debug("Sent a producer record");
            //endregion
        } finally {
            pravegaWriter.close();
            clientFactory.close();
        }
    }

    public void consume(String message) {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(applicationConfig.getProperty("bootstrap.servers")))
                .build();
        String scope = applicationConfig.getProperty("scope.name");
        String stream = applicationConfig.getProperty("stream.name");
        String application = applicationConfig.getProperty("client.id");
        String readerGroup = applicationConfig.getProperty("group.id");

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, stream))
                .disableAutomaticCheckpoints().build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        EventStreamReader<String> pravegaReader = clientFactory.createReader(application, readerGroup,
                new JavaSerializer<>(), ReaderConfig.builder().build());
        try {
            String readEvent = pravegaReader.readNextEvent(120).getEvent();
            //region Any additional business logic here...
            log.debug("Read an event: {}", readEvent);
            //endregion
        } finally {
            pravegaReader.close();
            clientFactory.close();
            readerGroupManager.close();
        }
    }
}
