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

import io.pravega.client.stream.EventRead;

import java.util.List;

public interface Reader<T> extends AutoCloseable {
    List<T> readAll(long timeoutInMillis);

    EventRead<T> readNextEvent(long timeoutInMillis);

    T tryReadNext(long timeinMillis);

    T readNext(long timeinMillis);

    void seekToEnd();

    List<String> getStreams();
}
