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
