package io.pravega.adapters.kafka.client.producer;

import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaProducerConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PravegaKafkaProducerUsageExamples {

    @Test
    public void usingCustomProducerWithSameProperties() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();

        String topic = "test-stream";
        String message = "test-message-1";

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"tcp://localhost:9090");
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
    public void produceUsesCustomProducer() throws ExecutionException, InterruptedException {
        Properties producerConfig = new Properties();

        String scope = "test-scope";
        String topic = "test-stream-" + Math.random();
        String controllerUri = "tcp://localhost:9090";
        String message = "test-message-1";

        producerConfig.put(PravegaProducerConfig.CONTROLLER_URI, controllerUri);
        producerConfig.put(PravegaProducerConfig.SCOPE, scope);
        // producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"tcp://localhost:9090");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> pravegaKafkaProducer = new PravegaKafkaProducer<>(producerConfig);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, 1, "test-key", message);

        Future<RecordMetadata> recordMedata = pravegaKafkaProducer.send(producerRecord);
        assertNotNull(recordMedata.get());

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri)) {
            assertEquals(message, reader.readNext());
        }
    }

    @Test
    public void writerAndReaderExample() {
        String scope = "test-scope";
        String topic = "test-stream-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        try (PravegaWriter writer = new PravegaWriter(scope, topic, controllerUri)) {
            writer.writeEvent("Message - 1")
                    .thenRun(() -> writer.writeEvent("Message - 2"))
                    .thenRun(() -> writer.writeEvent("Message - 3"))
                    .join();
        }

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri)) {
            assertEquals("Message - 1", reader.readNext());
            assertEquals("Message - 2", reader.readNext());
            assertEquals("Message - 3", reader.readNext());
        }
    }
}
