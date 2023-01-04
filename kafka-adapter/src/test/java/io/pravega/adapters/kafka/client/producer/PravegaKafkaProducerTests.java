/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.producer;

import io.pravega.adapters.kafka.client.dataaccess.Writer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.testutils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PravegaKafkaProducerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsEmpty() {
        Properties props = new Properties();
        new PravegaKafkaProducer<>(props);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sendOffsetsToTransactionThrowsException() {
        new PravegaKafkaProducer<>(prepareDummyMinimalConfig()).sendOffsetsToTransaction(null, "");
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
    public void transactionalWrite() {
        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig());
        producer.initTransactions();
        try {
            producer.beginTransaction();

            // produce data at this point

            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these, so we'll close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
        producer.close();

        // No exceptions equates a successful test run here.
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

    @Test
    public void transactionSendReturnsNonNullRecordMetadata() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        when(mockWriter.writeEvent(any())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);

        producer.initTransactions();
        producer.beginTransaction();

        // No need to subscribe, we have already set writersByStream in the producer through constructor injection.
        Future<RecordMetadata> lastRecordMetadataFuture = null;
        for (int i = 0; i < 5; i++) {
            lastRecordMetadataFuture = producer.send(
                    new ProducerRecord<String, String>(topic, message + "-" + i));
        }
        producer.commitTransaction();

        producer.flush();
        producer.close();

        assertNotNull(lastRecordMetadataFuture.get());
        assertEquals(topic, lastRecordMetadataFuture.get().topic());
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
    public void flushInvokesWritersFlush() throws Exception {
        Writer<String> mockWriter = mock(Writer.class);
        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);
        producer.flush();

        verify(mockWriter, times(1)).flush();
    }

    @Test
    public void closeInvokesWritersClose() throws Exception {
        String topic = "test-topic";
        String message = "message to send";

        Writer<String> mockWriter = mock(Writer.class);
        Map<String, Writer<String>> writersByStream = new HashMap<>();
        writersByStream.put("test-topic", mockWriter);

        Producer<String, String> producer = new PravegaKafkaProducer<>(prepareDummyMinimalConfig(), writersByStream);
        producer.close();
        verify(mockWriter, times(1)).close();

        reset(mockWriter);
        producer.close(Duration.ofMillis(100));
        verify(mockWriter, times(1)).close();

        reset(mockWriter);
        producer.close(Duration.ofMillis(200));
        verify(mockWriter, times(1)).close();
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
