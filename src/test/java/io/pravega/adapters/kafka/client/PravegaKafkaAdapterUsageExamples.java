package io.pravega.adapters.kafka.client;

import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaProducerConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

        try (PravegaReader reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                PravegaKafkaConfig.extractEndpoints(producerConfig, null))) {
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

        producerConfig.put(PravegaProducerConfig.CONTROLLER_URI, controllerUri);
        producerConfig.put(PravegaProducerConfig.SCOPE, scope);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader reader = new PravegaReader(PravegaKafkaConfig.DEFAULT_SCOPE, topic,
                PravegaKafkaConfig.extractEndpoints(producerConfig, null))) {
            assertEquals(message, reader.readNext());
        }
    }
}
