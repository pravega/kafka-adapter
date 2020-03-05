package io.pravega.adapters.kafka.client.utils;

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
