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
package io.pravega.kafka.sampleapps.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class PersonSerializer implements Serializer<Person> {

    @Override
    public byte[] serialize(String topic, Person data) {
        byte[] result = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            result = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
