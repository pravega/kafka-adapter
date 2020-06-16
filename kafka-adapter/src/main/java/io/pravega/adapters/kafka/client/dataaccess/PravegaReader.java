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

    private final ReaderManager readerManager;

    // Used for mocking
    @VisibleForTesting
    @Setter(AccessLevel.PACKAGE)
    private EventStreamReader<T> reader;

    public PravegaReader(@NonNull String scope, @NonNull List<String> streams, @NonNull String controllerUri,
                         @NonNull Serializer serializer, @NonNull String readerGroupName, @NonNull String readerId) {
        this.readerManager = new ReaderManager(scope, readerGroupName, readerId, streams, URI.create(controllerUri),
                serializer);
    }

    public PravegaReader(@NonNull String scope, @NonNull String stream, @NonNull String controllerUri,
                         @NonNull Serializer serializer, @NonNull String readerGroupName, @NonNull String readerId) {
        this(scope, Arrays.asList(stream), controllerUri, serializer, readerGroupName, readerId);
    }

    private boolean isInitialized() {
        return this.reader != null;
    }

    public void initIfNotInitialized() {
        if (isInitialized()) {
            return;
        }
        this.readerManager.initialize();
        this.reader = this.readerManager.getReader();
    }

    @Override
    public List<T> readAll(long timeoutInMillis) {
        initIfNotInitialized();
        List<T> result = new ArrayList<>();
        EventRead<T> event = null;
        do {
            event = this.reader.readNextEvent(timeoutInMillis);
            if (event != null && event.getEvent() != null) {
                result.add(event.getEvent());
            }
        } while (!isLastEvent(event));
        return result;
    }

    private boolean isLastEvent(EventRead event) {
        if (event == null) {
            return true;
        } else {
            if (event.isCheckpoint()) {
                return false;
            } else {
                return event.getEvent() == null;
            }
        }
    }

    @Override
    public EventRead<T> readNextEvent(long timeoutInMillis) {
        initIfNotInitialized();
        EventRead<T> result = this.reader.readNextEvent(timeoutInMillis);
        if (result.isCheckpoint()) {
            result = this.reader.readNextEvent(timeoutInMillis);
        }
        return result;
    }

    @Override
    public void seekToEnd() {
        log.debug("seekToEnd() invoked");
        initIfNotInitialized();

        this.readerManager.getReaderGroup().getGroupName();

        ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder();

        this.readerManager.getStreamNames().forEach(stream -> {
            StreamInfo streamInfo = this.readerManager.getStreamManager().getStreamInfo(this.readerManager.getScope(),
                    (String) stream);
            StreamCut tailStreamCut = streamInfo.getTailStreamCut();
            log.debug("tailStreamCut: {}", tailStreamCut);
            builder.stream(this.readerManager.getScope() + "/" + stream, tailStreamCut);
        });
        this.readerManager.reset(builder.build());
    }

    @Override
    public void close() {
        this.readerManager.close();
    }
}
