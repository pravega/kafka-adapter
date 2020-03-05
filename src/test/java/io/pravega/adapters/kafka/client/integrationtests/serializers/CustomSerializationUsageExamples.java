package io.pravega.adapters.kafka.client.integrationtests.serializers;

import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.utils.Person;
import io.pravega.adapters.kafka.client.utils.PersonSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CustomSerializationUsageExamples {

    @Test
    public void sendMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();
        Person person = new Person("Ravi", "Sharda", "rsharda");

        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        producerConfig.put("pravega.scope", "CustomSerializationUsageExamples");

        Producer<String, Person> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Person> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", person);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<Person> reader = new PravegaReader<>("CustomSerializationUsageExamples", topic,
                new PravegaKafkaConfig(producerConfig).serverEndpoints(), new PersonSerializer())) {
            Person readPerson = reader.readNext();
            System.out.println("Read Person: " + readPerson);
            assertEquals("rsharda", person.getUserName());
        }
    }

    private static void serializeThenDeserialize() {
        Person person =  new Person("Ravi", "Sharda", "rsharda");
        PersonSerializer serializer = new PersonSerializer();
        System.out.println("Initial: " + person);
        ByteBuffer serialized = serializer.serialize(person);
        Person deserialized = serializer.deserialize(serialized);
        System.out.println("De-serialized: " + deserialized);
        System.out.println("Equals?" + person.equals(deserialized));
    }
}



