package io.pravega.adapters.kafka.client;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Contains examples demonstrating the use of Kafka producer and consumer implementations of the the Kafka Adapter.
 */
public class PravegaKafkaAdapterUsageExamples {

    @Test
    public void testProduceWithMinimalKafkaConfig() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();

        String topic = "tpmkc-" + Math.random();
        String message = "test-message-1";

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<String> reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                PravegaKafkaConfig.extractEndpoints(producerConfig, null), new JavaSerializer<String>())) {
            assertEquals(message, reader.readNext());
        }
    }

    @Test
    public void testProduceWithKafkaAndPravegaConfig() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();

        String scope = "test-scope";
        String topic = "tpkpc-" + Math.random();
        String controllerUri = "tcp://localhost:9090";
        String message = "test-message-1";

        producerConfig.put(PravegaKafkaConfig.CONTROLLER_URI, controllerUri);
        producerConfig.put(PravegaKafkaConfig.SCOPE, scope);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                PravegaKafkaConfig.extractEndpoints(producerConfig, null),
                new JavaSerializer<String>())) {
            assertEquals(message, reader.readNext());
        }
    }

    @Test
    public void testProduceThenConsumeWithMinimalKafkaPravegaConfig() throws ExecutionException, InterruptedException {
        String topic = "tpmkc-" + Math.random();
        String message = "test-message-1";
        String bootstrapServers = "tcp://localhost:9090";

        // Produce events
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());
        System.out.println("Done producing message: " + message);
        pravegaKafkaProducer.close();

        // Consume events
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Consumed a record containing value: " + record.value());
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testConsumeOnly() {
        String topic = "tpmkc-0.11596792843929993";
        String message = "test-message-1";
        String bootstrapServers = "tcp://localhost:9090";

        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // First let's make sure that the event is actually there in the stream.
        try (PravegaReader reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                PravegaKafkaConfig.extractEndpoints(consumerConfig, null), new JavaSerializer<String>())) {
            assertEquals(message, reader.readNext());
            System.out.format("Found expected message in in scope/stream %s/%s\n", PravegaKafkaConfig.DEFAULT_SCOPE,
                    topic);
        }

        // Now, let's use the Kafka Consumer API to fetch the same message.
        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            //while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records == null) {
                    System.out.println("Found no records in the current poll");
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumed a record containing value: " + record.value());
                }
            //}
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testProduceThenConsumeAsynchronously() {
        String topic = "tpc-" + Math.random();
        String message = "test-message-1";
        String bootstrapServers = "tcp://localhost:9090";

        // Produce events
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        pravegaKafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Encountered an exception: " + exception.getMessage());
            }
            System.out.println("Produced record metadata: " + metadata);
            assertNotNull(metadata);
        });

        pravegaKafkaProducer.close();

    }
}
