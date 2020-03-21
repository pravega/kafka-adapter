package org.example.kafkaclient.sampleapps;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class ProducerAppWithAsynchronousSend {

    public static void main(String... args) {
        String bootstrapServers = "localhost:32768"; // Kafka server
        // String bootstrapServers = "tcp://localhost:9090";
        String topic = "ProducerAppWithMinimalKafkaConfig";
        String message = "test message";

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", System.getProperty("bootstrap.servers", bootstrapServers));
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        ProducerRecord producerRecord = new ProducerRecord(topic, message);

        // Asynchronously sending a producer record via the producer
        Future<RecordMetadata> rmFuture = producer.send(producerRecord);

        try {
            RecordMetadata rm = rmFuture.get();
            log.info("Done sending the producer record, received record metadata");
            assert rm != null;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered an exception", e);
            System.exit(-1);
        } finally {
            producer.close();
        }

        log.info("Exiting...");
        System.exit(0);
    }
}
