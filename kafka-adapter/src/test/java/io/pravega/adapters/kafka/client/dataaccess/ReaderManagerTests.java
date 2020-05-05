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

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import static io.pravega.adapters.kafka.client.testutils.TestUtils.assertThrows;
import static org.junit.Assert.assertFalse;

public class ReaderManagerTests {

    private Serializer dummySerializer = new ByteArraySerializer();

    @Test
    public void nullArgumentsToCtorThrowsException() {
        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>(null, null, null, null,
                        null, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void ctorThrowsExceptionForNullArguments() {
        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>(null, "rgName",
                        "readerId", Arrays.asList("stream"), URI.create("controllerUri"), dummySerializer),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>("scope", null,
                        "readerId", Arrays.asList("stream"), URI.create("controllerUri"), dummySerializer),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>("scope", "rgName",
                        null, Arrays.asList("stream"), URI.create("controllerUri"), dummySerializer),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>("scope", "rgName",
                        "readerId", null, URI.create("controllerUri"), dummySerializer),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>("scope", "rgName",
                        "readerId", Arrays.asList("stream"), null, dummySerializer),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new ReaderManager<>("scope", "rgName",
                        "readerId", Arrays.asList("stream"), URI.create("controllerUri"), null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void uninitializedReturnsCorrectStatus() {
        ReaderManager<String> objectUnderTest = new ReaderManager<>("dummyScope",
                "dummyReaderGroupName", "dummyReaderId", new ArrayList<>(),
                URI.create("dummyUrl"), new JavaSerializer<>());
        assertFalse(objectUnderTest.isInitialized());
    }
}
