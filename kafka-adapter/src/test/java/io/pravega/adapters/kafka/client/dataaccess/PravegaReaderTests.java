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

import io.pravega.adapters.kafka.client.testutils.FakeEvent;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import java.util.List;

import static io.pravega.adapters.kafka.client.testutils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PravegaReaderTests {

    private Serializer dummySerializer = new ByteArraySerializer();

    @Test
    public void ctorThrowsExceptionWhenInputHasNull() {
        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>(null, "stream", "controlleruri", dummySerializer, "rgName", "readerId"),
                e -> e instanceof NullPointerException);

        String stream = null;
        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>("scope", stream, "controlleruri", dummySerializer, "rgName", "readerId"),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>("scope", "stream", null, dummySerializer, "rgName", "readerId"),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>("scope", "stream", "controlleruri", null, "rgName", "readerId"),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>("scope", "stream", "controlleruri", dummySerializer, null, "readerId"),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter NullPointerException.",
                () -> new PravegaReader<>("scope", "stream", "controlleruri", dummySerializer, "rgName", null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void readsAreDelegatedToEventReader() {
        EventStreamReader<String> mockInternalReader = mock(EventStreamReader.class);
        when(mockInternalReader.readNextEvent(anyLong())).thenReturn(new FakeEvent<String>("test-message"));

        PravegaReader<String> reader = dummyReader();
        reader.setReader(mockInternalReader);

        assertEquals("test-message", reader.readNextEvent(100).getEvent());
    }

    @Test
    public void readAllReturnsAllEventsUntilNullEvent() {
        EventStreamReader<String> mockInternalReader = mock(EventStreamReader.class);
        when(mockInternalReader.readNextEvent(anyLong())).thenReturn(
                new FakeEvent<String>("test-message"),
                new FakeEvent<String>("test-message"),
                null);

        PravegaReader<String> reader = dummyReader();
        reader.setReader(mockInternalReader);

        List<String> readEvents = reader.readAll(200);
        assertNotNull(readEvents);
        assertEquals(2, readEvents.size());
    }

    @Test
    public void readAllReturnsAllEventsUntilEventWithNullMsg() {
        EventStreamReader<String> mockInternalReader = mock(EventStreamReader.class);
        when(mockInternalReader.readNextEvent(anyLong())).thenReturn(
                new FakeEvent<String>("test-message"),
                new FakeEvent<String>("test-message"),
                new FakeEvent<>(null));

        PravegaReader<String> reader = dummyReader();
        reader.setReader(mockInternalReader);

        List<String> readEvents = reader.readAll(200);
        assertNotNull(readEvents);
        assertEquals(2, readEvents.size());
    }

    private PravegaReader<String> dummyReader() {
        return new PravegaReader<String>("test-scope", "test-stream", "test-uri", new JavaSerializer<String>(),
                "test-rg-name", "test-reader-id");
    }
}
