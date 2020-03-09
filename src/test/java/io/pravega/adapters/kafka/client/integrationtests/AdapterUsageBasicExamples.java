package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.stream.impl.JavaSerializer;

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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Contains examples demonstrating the use of Kafka producer and serialization implementations of the the Kafka Adapter.
 */
public class AdapterUsageBasicExamples {

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
                new PravegaKafkaConfig(producerConfig).serverEndpoints(), new JavaSerializer<String>())) {
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
                new PravegaKafkaConfig(producerConfig).serverEndpoints(),
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
        // consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // First let's make sure that the event is actually there in the stream.
        try (PravegaReader reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                new PravegaKafkaConfig(consumerConfig).serverEndpoints(), new JavaSerializer<String>())) {
            assertEquals(message, reader.readNext());
            System.out.format("Found expected message in in scope/stream %s/%s%n", PravegaKafkaConfig.DEFAULT_SCOPE,
                    topic);
        }

        // Now, let's use the Kafka Consumer API to fetch the same message.
        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records == null) {
                System.out.println("Found no records in the current poll");
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumed a record containing value: " + record.value());
                }
            }
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
                System.out.println("Encountered an exception: " + exception.getMessage());
            }
            System.out.println("Produced record metadata: " + metadata);
            assertNotNull(metadata);
        });
        pravegaKafkaProducer.close();
    }

    @Test
    public void testReceivesEmptyMessagesOnPollWhenNoEventsPresent() {
        /// Keeping scope and topic names random to eliminate the chances of it already being present.
        String scopeName = "multiple-messages-" + Math.random();
        String topicName = "test-topic-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        // First let's ensure that the scope and topic are created
        try (PravegaWriter writer = new PravegaWriter(scopeName, topicName, controllerUri,
                new JavaSerializer<String>())) {
            writer.init();
        }

        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", controllerUri);
        consumerConfig.put("group.id", UUID.randomUUID().toString());
        consumerConfig.put("client.id", "your_client_id");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("pravega.scope", scopeName);

        try (Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig)) {
            consumer.subscribe(Arrays.asList(topicName));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
        }
    }
}
