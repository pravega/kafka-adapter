package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.consumer.PravegaKafkaConsumer;
import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.stream.impl.JavaSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class AdapterUsageBasicExamples {

    @Test
    public void testProduceWithMinimalKafkaConfig() throws ExecutionException, InterruptedException {
        Properties props = new Properties();

        String topic = "tpmkc-" + Math.random();
        String message = "test-message-1";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(props);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader<String> reader = new PravegaReader(PravegaConfig.DEFAULT_SCOPE, topic,
                "tcp://localhost:9090", new JavaSerializer<String>(),
                UUID.randomUUID().toString(), "readerId")) {
            assertEquals(message, reader.readNext(200));
        }
    }

    @Test
    public void testProduceWithKafkaAndPravegaConfig() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();

        String scope = "test-scope";
        String topic = "tpkpc-" + Math.random();
        String controllerUri = "tcp://localhost:9090";
        String message = "test-message-1";

        producerConfig.put(PravegaConfig.CONTROLLER_URI, controllerUri);
        producerConfig.put(PravegaConfig.SCOPE, scope);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader reader = new PravegaReader(PravegaConfig.DEFAULT_SCOPE, topic, controllerUri,
                new JavaSerializer<String>(), UUID.randomUUID().toString(), "readerId")) {
            assertEquals(message, reader.readNext(200));
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
        log.info("Done producing message: {}", message);
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
                log.info("Consumed a record containing value: {}", record.value());
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
        try (PravegaReader reader = new PravegaReader(PravegaConfig.DEFAULT_SCOPE, topic,
                bootstrapServers, new JavaSerializer<String>(), UUID.randomUUID().toString(), "readerId")) {
            assertEquals(message, reader.readNext(200));
            log.info("Found expected message in in scope/stream {}/{}", PravegaConfig.DEFAULT_SCOPE,
                    topic);
        }

        // Now, let's use the Kafka Consumer API to fetch the same message.
        Consumer<String, String> consumer = new PravegaKafkaConsumer(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));

        try {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records == null) {
                log.info("Found no records in the current poll");
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Consumed a record containing value: {}", record.value());
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
                log.info("Encountered an exception: {}", exception.getMessage());
            }
            log.info("Produced record metadata: " + metadata);
            assertNotNull(metadata);
        });
        pravegaKafkaProducer.close();
    }

    @Test
    public void testReceivesEmptyMessagesOnPollWhenNoEventsPresent() {
        // Keeping scope and topic names random to eliminate the chances of it already being present.
        String scopeName = "multiple-messages-" + Math.random();
        String topicName = "test-topic-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        // First let's ensure that the scope and topic are created
        try (PravegaWriter writer = new PravegaWriter(scopeName, topicName, controllerUri,
                new JavaSerializer<String>(), 1)) {
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

    @Test
    public void testSendsAndReceivesMessagesFromStreamsContainingMultipleSegments() {
        // Keeping scope and topic names random to eliminate the chances of it already being present.
        String scopeName = "multistream-" + Math.random();
        String topicName = "test-topic-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        // Produce events
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, controllerUri);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(PravegaConfig.SCOPE, scopeName);
        producerConfig.put(PravegaConfig.NUM_SEGMENTS, "3");

        try (Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig)) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, 1, "test-key", "message: " + i);
                pravegaKafkaProducer.send(producerRecord);
            }
            pravegaKafkaProducer.flush();
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            assertEquals(5, records.count());
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                log.info("Read event message: {}", record.value());
            }
        }

    }
}
