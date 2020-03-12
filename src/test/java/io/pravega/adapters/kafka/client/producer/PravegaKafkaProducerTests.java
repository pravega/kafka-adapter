package io.pravega.adapters.kafka.client.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import io.pravega.adapters.kafka.client.shared.Writer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    public void sendInvokesWritersWriteEvent() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        when(mockWriter.writeEvent(message)).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);

        // No need to subscribe, we have already set writersByStream in the producer through constructor injection.

        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, message));
        verify(mockWriter).writeEvent(message);
    }

    @Test
    public void sendReturnsNonNullRecordMetadata() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        when(mockWriter.writeEvent(message)).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);

        // No need to subscribe, we have already set writersByStream in the producer through constructor injection.

        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, message));
        RecordMetadata recordMetadata = result.get();
        assertNotNull(recordMetadata);
        assertEquals(topic, recordMetadata.topic());
    }

    @Test
    public void sendReturnsNullWhenWriterErrorsOut() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> fakeWriter = new Writer() {
            @Override
            public void close() throws Exception {
            }

            @Override
            public CompletableFuture<Void> writeEvent(Object event) {
                CompletableFuture<Void> result = new CompletableFuture<>();
                try {
                    throw new RuntimeException("whatever");
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
                return result;
            }

            @Override
            public void flush() {
            }

            @Override
            public void init() {
            }
        };

        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put(topic, fakeWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);

        // No need to subscribe, we have already set writersByStream in the producer through constructor injection.

        Future<RecordMetadata> rmFuture = producer.send(new ProducerRecord<>(topic, message));
        assertNotNull(rmFuture);
        assertNull(rmFuture.get());
    }

    @Test
    public void flushAndCloseInvokesWritersFlush() throws Exception {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);
        producer.flush();
        producer.close();

        verify(mockWriter, times(1)).flush();
        verify(mockWriter, times(1)).close();
    }

    @Test
    public void sendWithCallbackInvokesCallback() {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        when(mockWriter.writeEvent(message)).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);

        // No need to subscribe, we have already set writersByStream in the producer through constructor injection.

        final AtomicInteger callbackCallCounter = new AtomicInteger(0);
        producer.send(new ProducerRecord<>(topic, message), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                callbackCallCounter.incrementAndGet();
            }
        });
        assertEquals(1, callbackCallCounter.get());
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
