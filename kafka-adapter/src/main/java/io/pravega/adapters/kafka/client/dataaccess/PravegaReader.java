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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.pravega.client.stream.StreamCut;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaReader<T> implements Reader<T> {
    private final String scope;

    private final List<String> streams = new ArrayList<>();

    private final String controllerUri;

    private final Serializer serializer;

    private final ReaderManager readerManager;

    // Used for mocking
    @VisibleForTesting
    @Setter(AccessLevel.PACKAGE)
    private EventStreamReader<T> reader;

    public PravegaReader(@NonNull String scope, @NonNull List<String> streams, @NonNull String controllerUri,
                         @NonNull Serializer serializer, @NonNull String readerGroupName, @NonNull String readerId) {
        this.scope = scope;
        this.streams.addAll(streams);
        this.controllerUri = controllerUri;
        this.serializer = serializer;
        this.readerManager = new ReaderManager(readerGroupName, readerId);
    }

    public PravegaReader(@NonNull String scope, @NonNull String stream, @NonNull String controllerUri,
                         @NonNull Serializer serializer, @NonNull String readerGroupName, @NonNull String readerId) {
        this(scope, Arrays.asList(stream), controllerUri, serializer, readerGroupName, readerId);
    }

    private boolean isInitialized() {
        return this.reader != null;
    }

    public void init() {
        if (isInitialized()) {
            return;
        }
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();
        this.readerManager.initialize(this.streams, this.scope, clientConfig, this.serializer);
        this.reader = this.readerManager.getReader();
    }

    @Override
    public List<T> readAll(long timeoutInMillis) {
        if (!isInitialized()) {
            init();
        }
        List<T> result = new ArrayList<>();
        EventRead<T> event = null;
        do {
            event = this.reader.readNextEvent(timeoutInMillis);
            if (event.getEvent() != null) {
                result.add(event.getEvent());
            }
        } while (event.getEvent() != null);
        return result;
    }

    @Override
    public EventRead<T> readNextEvent(long timeoutInMillis) {
        if (!isInitialized()) {
            init();
        }
        return this.reader.readNextEvent(timeoutInMillis);
    }

    @Override
    public T tryReadNext(long timeinMillis) {
        if (!isInitialized()) {
            init();
        }
        EventRead<T> event = this.reader.readNextEvent(timeinMillis);
        if (event != null) {
            return event.getEvent();
        } else {
            return null;
        }
    }

    @Override
    public T readNext(long timeinMillis) {
        T result = tryReadNext(timeinMillis);
        if (result == null) {
            throw new IllegalStateException("No Event");
        }
        return result;
    }

    @Override
    public void seekToEnd() {
        if (!isInitialized()) {
            init();
        }
        log.debug("seekToEnd() invoked");
        ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder();
        this.streams.stream().forEach(stream -> {
            StreamInfo streamInfo = this.readerManager.getStreamManager().getStreamInfo(this.scope, stream);
            StreamCut tailStreamCut = streamInfo.getTailStreamCut();
            log.debug("tailStreamCut: {}", tailStreamCut);
            builder.stream(this.scope + "/" + stream, tailStreamCut);
            // builder.startFromStreamCuts()
        });
        this.readerManager.getReaderGroup().resetReaderGroup(builder.build());
    }

    @Override
    public List<String> getStreams() {
        return this.streams;
    }

    @Override
    public void close() {
        this.readerManager.close();
    }
}
