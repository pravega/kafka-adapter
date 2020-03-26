/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.samplesapps.shared;

import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StringDeserializationSchema extends AbstractDeserializationSchema<String> {

    private JavaSerializer<String> serializer = new JavaSerializer<>();

    @Override
    public String deserialize(byte[] message) throws IOException {
        return serializer.deserialize(ByteBuffer.wrap(message));
    }
}
