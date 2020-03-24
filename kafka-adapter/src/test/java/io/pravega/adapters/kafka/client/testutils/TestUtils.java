/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.testutils;

import io.pravega.common.function.RunnableWithException;
import org.junit.Assert;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

public class TestUtils {

    public static void assertThrows(String message, RunnableWithException runnable, Predicate<Throwable> tester) {
        try {
            runnable.run();
            Assert.fail(message + " No exception has been thrown.");
        } catch (CompletionException | ExecutionException ex) {
            if (!tester.test(ex.getCause())) {
                throw new AssertionError(message + " Exception thrown was of unexpected type: " + ex.getCause(), ex);
            }
        } catch (Throwable ex) {
            if (!tester.test(ex)) {
                throw new AssertionError(message + " Exception thrown was of unexpected type: " + ex, ex);
            }
        }
    }
}
