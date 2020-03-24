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
package org.example.kafkaclient.sampleapps.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class PersonDeserializer implements Deserializer<Person> {

    @Override
    public Person deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        Person result = null;
        try {
            result = objectMapper.readValue(data, Person.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
