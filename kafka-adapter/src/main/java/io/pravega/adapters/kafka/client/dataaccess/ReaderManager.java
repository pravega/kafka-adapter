/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.dataaccess;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;

import java.net.URI;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class ReaderManager<T> {

    @Getter(AccessLevel.PACKAGE)
    @NonNull
    private final String scope;

    @NonNull
    private final String readerGroupName;

    @NonNull
    private final String readerId;

    @NonNull
    @Getter(AccessLevel.PACKAGE)
    private final List<String> streamNames;

    @NonNull
    private final URI controllerUri;

    @NonNull
    private final Serializer<T> serializer;

    @Getter(AccessLevel.PACKAGE)
    private EventStreamReader<T> reader;

    private ReaderGroupManager readerGroupManager;

    @Getter(AccessLevel.PACKAGE)
    private StreamManager streamManager;

    @Getter(AccessLevel.PACKAGE)
    private ReaderGroup readerGroup;

    boolean isInitialized() {
        return reader != null;
    }

    public void initialize() {
        if (isInitialized()) {
            return;
        }
        ReaderGroupConfig.ReaderGroupConfigBuilder rgBuilder =
                ReaderGroupConfig.builder().disableAutomaticCheckpoints();

        for (String stream : this.streamNames) {
            rgBuilder.stream(Stream.of(scope, stream));
        }

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri)
                .build();

        ReaderGroupConfig readerGroupConfig = rgBuilder.build();
        readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        readerGroup = readerGroupManager.getReaderGroup(this.readerGroupName);

        reader = EventStreamClientFactory.withScope(scope, clientConfig)
                .createReader(readerId, readerGroupName, serializer, ReaderConfig.builder().build());
        streamManager = StreamManager.create(clientConfig);
    }

    public void close() {
        try {
            if (reader != null ) {
                reader.close();
            }
            if (readerGroupManager != null) {
                readerGroupManager.close();
            }
            if (streamManager != null) {
                streamManager.close();
            }
        } catch (Exception e) {
            log.warn("Encountered exception in cleaning up", e);
        }
    }
}
