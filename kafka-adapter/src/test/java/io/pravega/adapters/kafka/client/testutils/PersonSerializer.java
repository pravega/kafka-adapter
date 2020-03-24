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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class PersonSerializer implements Serializer<Person>, Serializable {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ByteBuffer serialize(Person value) {
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Person deserialize(ByteBuffer serializedValue) {
        try {
            return objectMapper.readValue(serializedValue.array(), Person.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
