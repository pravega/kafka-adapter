package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.utils.Person;
import io.pravega.adapters.kafka.client.utils.PersonSerializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CustomSerializationUsageExamples {

    private static final String SCOPE_NAME = "CustomSerializationUsageExamples";

    private static final Person PERSON_OBJ = new Person("John", "Doe", "jdoe");

    @Test
    public void sendAndReceiveMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();

        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        producerConfig.put("pravega.scope", SCOPE_NAME);

        Producer<String, Person> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Person> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", PERSON_OBJ);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        consumerConfig.put("pravega.scope", SCOPE_NAME);

        Consumer<String, Person> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, Person> record : records) {
                Person readPerson = record.value();
                System.out.println("Consumed a record containing value: " + readPerson);
                assertEquals(PERSON_OBJ.getUserName(), readPerson.getUserName());
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void sendMessage() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();
        String topic = "test-topic-" + Math.random();

        producerConfig.put("bootstrap.servers", "tcp://localhost:9090");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        producerConfig.put("pravega.scope", SCOPE_NAME);

        Producer<String, Person> producer = new PravegaKafkaProducer<>(producerConfig);
        ProducerRecord<String, Person> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", PERSON_OBJ);

        Future<RecordMetadata> recordMedata = producer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<Person> reader = new PravegaReader<>(SCOPE_NAME, topic,
                new PravegaKafkaConfig(producerConfig).serverEndpoints(), new PersonSerializer())) {
            Person readPerson = reader.readNext();
            System.out.println("Read Person: " + readPerson);
            assertEquals("rsharda", PERSON_OBJ.getUserName());
        }
    }

    @Test
    public void receiveAnExistingMessage() throws ExecutionException, InterruptedException {
        // Change to an existing topic before running this test
        String topic = "test-topic-0.8010902196015256";

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // We are configuring a Pravega serializer here.
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.pravega.adapters.kafka.client.utils.PersonSerializer");

        // Pravega-specific config
        consumerConfig.put("pravega.scope",
                "CustomSerializationUsageExamples.sendAndReceiveMessage");

        Consumer<String, Person> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, Person> record : records) {
                Person readPerson = record.value();
                System.out.println("Consumed a record containing value: " + readPerson);
                assertEquals(PERSON_OBJ.getUserName(), readPerson.getUserName());
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * Demonstrates workings of a custom serialization/deserialization.
     */
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



