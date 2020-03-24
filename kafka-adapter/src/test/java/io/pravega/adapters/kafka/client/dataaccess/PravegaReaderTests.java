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

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.RequiredArgsConstructor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PravegaReaderTests {

    @Test
    public void readsAreDelegatedToEventReader() {
        EventStreamReader<String> mockInternalReader = mock(EventStreamReader.class);
        when(mockInternalReader.readNextEvent(anyLong())).thenReturn(new FakeEventRead<String>("test-message"));

        PravegaReader<String> reader = dummyReader();
        reader.setReader(mockInternalReader);

        assertEquals("test-message", reader.readNextEvent(100).getEvent());
    }

    private PravegaReader<String> dummyReader() {
        return new PravegaReader<String>("test-scope", "test-stream", "test-uri", new JavaSerializer<String>(),
                "test-rg-name", "test-reader-id");
    }
}

@RequiredArgsConstructor
class FakeEventRead<T> implements EventRead<T> {

    private final T event;

    @Override
    public T getEvent() {
        return this.event;
    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public EventPointer getEventPointer() {
        return null;
    }

    @Override
    public boolean isCheckpoint() {
        return false;
    }

    @Override
    public java.lang.String getCheckpointName() {
        return null;
    }
}
