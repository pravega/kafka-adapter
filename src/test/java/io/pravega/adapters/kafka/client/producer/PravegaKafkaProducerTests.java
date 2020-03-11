package io.pravega.adapters.kafka.client.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PravegaKafkaProducerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsImpty() {
        Properties props = new Properties();
        new PravegaKafkaProducer<>(props);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sendOffsetsToTransactionThrowsException() {
        new PravegaKafkaProducer<>(prepareDummyMinimalConfig()).sendOffsetsToTransaction(null, null);
    }

    @Test
    public void operationsThrowExceptionWhenAlreadyClosed() {
        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
        producer.close();

        assertThrows("Didn't encounter illegal state exception when sending a message.",
                () -> producer.send(new ProducerRecord<>("topic", "message")),
                e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter illegal state exception when initiating a transaction.",
                () -> producer.initTransactions(),
                e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter illegal state exception when beginning a transaction.",
                () -> producer.beginTransaction(),
                e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter illegal state exception when committing a transaction.",
                () -> producer.commitTransaction(),
                e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter illegal state exception when aborting a transaction.",
                () -> producer.abortTransaction(),
                e -> e instanceof IllegalStateException);
    }

    @Test
    public void operationsReturnEmptyResults() {
        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
        assertNotNull(producer.partitionsFor("whetever"));
        assertTrue(producer.partitionsFor("whetever").isEmpty());

        assertNotNull(producer.metrics());
        assertTrue(producer.metrics().isEmpty());
    }

    @Test
    public void sendThrowsExceptionIfRecordIsInvalid() {
        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig());

        assertThrows("Didn't encounter expected exception.",
                () -> producer.send(new ProducerRecord<>(null, "message")),
                e -> e instanceof IllegalArgumentException);

        assertThrows("Didn't encounter expected exception.",
                () -> producer.send(new ProducerRecord<>("topic", null)),
                e -> e instanceof IllegalArgumentException);
    }

    private Properties prepareDummyMinimalConfig() {
        Properties result = new Properties();
        result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.pravega.client.stream.impl.JavaSerializer");
        return result;
    }
}
