package org.example.kafkaclient.sampleapps.serialization;

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
