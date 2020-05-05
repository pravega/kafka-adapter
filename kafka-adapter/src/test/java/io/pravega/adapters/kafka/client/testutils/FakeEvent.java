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

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Position;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FakeEvent implements EventRead<String> {

    private final String messageToReturn;

    @Override
    public String getEvent() {
        return messageToReturn;
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
    public String getCheckpointName() {
        return null;
    }
}
