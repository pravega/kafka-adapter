package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.producer.PravegaKafkaProducer;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.utils.ConfigMaker;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TransactionExamples {

    @Test
    public void transactionalProducer() {
        String scope = "tx-scope-" + Math.random();
        String topic = "test-topic";
        String controllerUri = "tcp://localhost:9090";
        String consumerGroupId = "test-cg";
        String clientId = "test-cid";

        Properties producerConfig = ConfigMaker.makeTestProperties(scope, controllerUri, null, null,
                "my-transactional-id", null, true);
        log.info("producerConfig: {}", producerConfig);

        Producer<String, String> producer = new PravegaKafkaProducer<>(producerConfig);
        producer.initTransactions();
        try {
            producer.beginTransaction();

            for (int i = 0; i < 2; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, 1, "test-key", "message-" + i);

                // Sending asynchronously
                producer.send(producerRecord);
            }
            producer.commitTransaction();
            log.info("Done writing events");
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // No recovery is possible here, so just exit.
        } catch (KafkaException e) {
            // For all other exceptions we might want to retry. Here, we aren't retrying, but we can if we want to.
            producer.abortTransaction();
        } finally {
            producer.close();
        }

        // Assert
        try (PravegaReader<String> reader = new PravegaReader(scope, topic, controllerUri,
                new JavaSerializer<String>(), consumerGroupId, clientId)) {
            List<String> records = reader.readAll();
            log.info("Read events: [{}]", records);
            assertEquals(2, records.size());
        }
    }
}
